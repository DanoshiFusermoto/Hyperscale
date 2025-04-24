package org.radix.hyperscale.ledger;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.map.primitive.MutableObjectIntMap;
import org.eclipse.collections.impl.factory.primitive.ObjectIntMaps;
import org.radix.hyperscale.Constants;
import org.radix.hyperscale.Context;
import org.radix.hyperscale.Service;
import org.radix.hyperscale.Universe;
import org.radix.hyperscale.common.Primitive;
import org.radix.hyperscale.concurrency.MonitoredReadWriteLock;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.events.EventListener;
import org.radix.hyperscale.events.SyncLostEvent;
import org.radix.hyperscale.events.SynchronousEventListener;
import org.radix.hyperscale.exceptions.ServiceException;
import org.radix.hyperscale.exceptions.StartupException;
import org.radix.hyperscale.exceptions.TerminationException;
import org.radix.hyperscale.exceptions.ValidationException;
import org.radix.hyperscale.ledger.BlockHeader.InventoryType;
import org.radix.hyperscale.ledger.ProgressRound.State;
import org.radix.hyperscale.ledger.Substate.NativeField;
import org.radix.hyperscale.ledger.events.AtomAcceptedEvent;
import org.radix.hyperscale.ledger.events.AtomCommitEvent;
import org.radix.hyperscale.ledger.events.AtomUnpreparedTimeoutEvent;
import org.radix.hyperscale.ledger.events.BlockCommitEvent;
import org.radix.hyperscale.ledger.events.BlockCommittedEvent;
import org.radix.hyperscale.ledger.events.ProgressPhaseEvent;
import org.radix.hyperscale.ledger.events.SyncAcquiredEvent;
import org.radix.hyperscale.ledger.events.SyncAtomCommitEvent;
import org.radix.hyperscale.ledger.events.SyncBlockEvent;
import org.radix.hyperscale.ledger.exceptions.LockException;
import org.radix.hyperscale.ledger.messages.SyncAcquiredMessage;
import org.radix.hyperscale.ledger.primitives.Atom;
import org.radix.hyperscale.ledger.primitives.AtomCertificate;
import org.radix.hyperscale.ledger.primitives.Blob;
import org.radix.hyperscale.ledger.primitives.StateCertificate;
import org.radix.hyperscale.ledger.primitives.StateInput;
import org.radix.hyperscale.ledger.sme.Method;
import org.radix.hyperscale.ledger.timeouts.CommitTimeout;
import org.radix.hyperscale.ledger.timeouts.ExecutionTimeout;
import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;
import org.radix.hyperscale.network.events.ConnectedEvent;
import org.radix.hyperscale.utils.Numbers;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.eventbus.Subscribe;
import com.sleepycat.je.OperationStatus;

public final class Ledger implements Service, LedgerInterface
{
	private static final Logger ledgerLog = Logging.getLogger("ledger");
	
	static 
	{
		ledgerLog.setLevel(Logging.INFO);
		
		final Universe universe = Universe.get();
		if (universe == null)
			throw new IllegalStateException("Universe is not yet initialized!");
		
		definitions = new Definitions();
	}
	
	/**
	 * Helper to provide common definitions and parameters derived from the Universe
	 */
	public static class Definitions
	{
		// BLOCK PERIODS AND INTERVALS
		public static final int 	EPOCH_PERIOD_DURATION_MILLISECONDS = 60000;
		public static final int 	EPOCH_DURATION_MILLISECONDS = (int) TimeUnit.DAYS.toMillis(1);
		public static final int		BLOCK_INTERVAL_TARGET_MILLISECONDS = 500;
		public static final int 	BLOCKS_PER_PERIOD = EPOCH_PERIOD_DURATION_MILLISECONDS / BLOCK_INTERVAL_TARGET_MILLISECONDS;
		public static final int 	BLOCKS_PER_EPOCH = EPOCH_DURATION_MILLISECONDS / BLOCK_INTERVAL_TARGET_MILLISECONDS;
		public static final long 	getDurationToBlockCount(long duration, TimeUnit unit)
		{
			long milliseconds = unit.toMillis(duration);
			return milliseconds / BLOCK_INTERVAL_TARGET_MILLISECONDS;
		}
		
		public static final int 	PROPOSAL_PHASE_TIMEOUT_MS = BLOCK_INTERVAL_TARGET_MILLISECONDS*6;
		public static final int 	VOTE_PHASE_TIMEOUT_MS = BLOCK_INTERVAL_TARGET_MILLISECONDS*6;
		public static final int 	TRANSITION_PHASE_TIMEOUT_MS = BLOCK_INTERVAL_TARGET_MILLISECONDS*2;


		public long roundInterval()
		{
			return Universe.get().getRoundInterval();
		}
		
		public long epochDuration(final TimeUnit unit)
		{
			return unit.convert(Universe.get().getEpochDuration(), TimeUnit.SECONDS);
		}

		public long proposalsPerEpoch()
		{
			return epochDuration(TimeUnit.MILLISECONDS) / roundInterval();
		}
		
		public long proposalPhaseTimeout(final TimeUnit unit)
		{
			return unit.convert(Constants.PROPOSAL_PHASE_TIMEOUT_ROUNDS * Universe.get().getRoundInterval(), TimeUnit.MILLISECONDS);
		}

		public long votePhaseTimeout(final TimeUnit unit)
		{
			return unit.convert(Constants.VOTE_PHASE_TIMEOUT_ROUNDS * Universe.get().getRoundInterval(), TimeUnit.MILLISECONDS);
		}

		public long transitionPhaseTimeout(final TimeUnit unit)
		{
			return unit.convert(Constants.TRANSITION_PHASE_TIMEOUT_ROUNDS * Universe.get().getRoundInterval(), TimeUnit.MILLISECONDS);
		}
	}
	
	private static final Definitions definitions;
	
	public static final Definitions definitions()
	{
		return definitions;
	}
	
	///
	
	private final Context 	context;
	
	private final AtomHandler 		atomHandler;
	private final PackageHandler 	packageHandler;

	private final StatePool 		statePool;
	private final StateHandler 		stateHandler;
	private final StateAccumulator	stateAccumulator;

	private final BlockHandler 		blockHandler;
	private final SyncHandler 		syncHandler;

	private final LedgerStore 		ledgerStore;
	private final LedgerSearch		ledgerSearch;
	private final ValidatorHandler 	validatorHandler;
	
	private final ReentrantReadWriteLock lock;
	private final ReentrantReadWriteLock headLock;
	private final transient AtomicReference<Epoch> epoch;
	private final transient AtomicReference<BlockHeader> head;
	
	public Ledger(Context context)
	{
		this.context = Objects.requireNonNull(context);

		this.ledgerStore = new LedgerStore(this.context);

		this.validatorHandler = new ValidatorHandler(this.context);
		this.blockHandler = new BlockHandler(this.context);
		this.syncHandler = new SyncHandler(this.context);
		this.stateAccumulator = new StateAccumulator(this.context, "ledger", this.ledgerStore);
		this.statePool = new StatePool(this.context);
		this.stateHandler = new StateHandler(this.context);
		this.packageHandler = new PackageHandler(this.context);
		this.atomHandler = new AtomHandler(this.context);
		this.ledgerSearch = new LedgerSearch(this.context);

		this.head = new AtomicReference<BlockHeader>(this.context.getNode().getHead());
		this.epoch = new AtomicReference<Epoch>(Epoch.from(this.head.get()));
		
		this.lock = new MonitoredReadWriteLock(this.context.getName()+" Ledger Lock", true);
		this.headLock = new MonitoredReadWriteLock(this.context.getName()+" Ledger Head Lock", true);

	}
	
	@Override
	public void start() throws StartupException
	{
		try
		{
			// Stuff required for integrity check
			this.ledgerStore.start();
			this.validatorHandler.start();
			
			integrity();
			
			// IMPORTANT Order dependent!
			this.context.getEvents().register(this.syncChangeListener);
			this.context.getEvents().register(this.syncBlockListener);
			this.context.getEvents().register(this.asyncBlockListener);
			this.context.getEvents().register(this.peerListener);

			this.blockHandler.start();
			this.atomHandler.start();
			this.packageHandler.start();
			this.stateAccumulator.reset();
			this.statePool.start();
			this.stateHandler.start();
			this.syncHandler.start();

			this.context.getEvents().register(this.asyncAtomListener);
			this.context.getEvents().register(this.syncAtomListener);
			
			this.ledgerSearch.start();
		}
		catch (Exception ex)
		{
			throw new StartupException(ex);
		}			
	}

	@Override
	public void stop() throws TerminationException
	{
		this.context.getEvents().unregister(this.peerListener);
		this.context.getEvents().unregister(this.asyncAtomListener);
		this.context.getEvents().unregister(this.syncAtomListener);
		this.context.getEvents().unregister(this.asyncBlockListener);
		this.context.getEvents().unregister(this.syncBlockListener);
		this.context.getEvents().unregister(this.syncChangeListener);

		this.ledgerSearch.stop();
		
		this.syncHandler.stop();
		this.stateHandler.stop();
		this.statePool.stop();
		this.atomHandler.stop();
		this.packageHandler.stop();
		this.blockHandler.stop();
		this.validatorHandler.stop();
		this.ledgerStore.stop();
	}
	
	@Override
	public void clean() throws ServiceException
	{
		this.ledgerStore.clean();
		this.validatorHandler.clean();
	}
	
	private void integrity() throws Exception
	{
		this.lock.writeLock().lock();
		try
		{
			Hash genesis = this.ledgerStore.getSyncBlock(0);
			if (genesis != null)
			{
				if (Universe.get().getGenesis().getHeader().getHash().equals(genesis) == false)
					throw new RuntimeException("You didn't clean your database dumbass!");
			}
	
			final Hash headHash = this.ledgerStore.head();
			// Check if this is just a new ledger store and doesn't need integrity or recovery
			if (headHash.equals(Universe.get().getGenesis().getHeader().getHash()) == true && this.ledgerStore.has(headHash, BlockHeader.class) == false)
			{
				// Some simple manual actions here with regard to persistance & provisioning as don't want to 
				// complicate the flow with a special cases for genesis in the modules
				
				// Store the genesis header
				this.ledgerStore.store(Universe.get().getGenesis().getHeader());

				// Store the genesis atom
//				for (Atom atom : Universe.getDefault().getGenesis().getAccepted())
//					this.ledgerStore.stores(atom);
					
				// Commit the genesis operations
				List<CommitOperation> commitOperations = new ArrayList<CommitOperation>();
				for (Atom atom : Universe.get().getGenesis().getAccepted())
				{
					// Need to take a "clean" copy of the atom in the case that multiple contexts are being initialised
					// Otherwise left overs from execution in other contexts may throw exceptions for the current context
					PendingAtom pendingAtom = new PendingAtom(this.context, new Atom(atom));
					pendingAtom.prepare();
					pendingAtom.accepted(Universe.get().getGenesis().getHeader());
					
					// All substates should be void
					pendingAtom.provision();
					for (StateAddress stateAddress : pendingAtom.getStateAddresses(null))
					{
						if (pendingAtom.isProvisioned(stateAddress) == false)
							pendingAtom.provision(new StateInput(atom.getHash(), new Substate(stateAddress)));
					}
						
					pendingAtom.execute();
					
					if (pendingAtom.thrown() != null)
						throw pendingAtom.thrown();
					
					commitOperations.add(pendingAtom.getCommitOperation());
				}
				
				this.ledgerStore.commit(Universe.get().getGenesis(), commitOperations);
				return;
			}
			else
			{
				// TODO block header is known but is it the strongest head that represents state?
				BlockHeader header = this.ledgerStore.get(headHash, BlockHeader.class);
				if (header == null)
				{
					// TODO recover to the best head with committed state
					ledgerLog.error(Ledger.this.context.getName()+": Local block header "+headHash+" not found in store");
					throw new UnsupportedOperationException("Integrity recovery not implemented");
				}
				
				this.head.set(header);
				ledgerLog.info(Ledger.this.context.getName()+": Setting ledger head as "+header);

				if (header.equals(Universe.get().getGenesis().getHeader()) == false && header.getView() == null)
					ledgerLog.warn(Ledger.this.context.getName()+": Ledger head "+header+" does not have a view certificate!");
				
				this.epoch.set(Epoch.from(header));
				ledgerLog.info(Ledger.this.context.getName()+": Setting ledger epoch as "+this.epoch.get().getClock());

				// Check continuity of sync blocks
				while(header.getHeight() > Math.max(1, this.head.get().getHeight() - definitions().proposalsPerEpoch()))
				{
					Hash prevHeaderHash = this.ledgerStore.getSyncBlock(header.getHeight()-1);
					BlockHeader prevHeader = this.ledgerStore.get(header.getPrevious(), BlockHeader.class);
					if (prevHeader == null)
						throw new ValidationException("Sync block for "+header.getPrevious()+" not found");

					if (header.getPrevious().equals(prevHeaderHash) == false)
					{
						ledgerLog.error(context.getName()+": Sync blocks continuity failed on "+header);
					
						OperationStatus status = this.ledgerStore.setSyncBlock(prevHeader);
						if (OperationStatus.SUCCESS.equals(status) == false)
							throw new ValidationException("Failed to update missing sync block "+prevHeader+" due to "+status);
						
						// If a sync block was missing, then likely validator powers is wrong too 
						// TODO this check may be redundant when operating under POS
						Block prevBlock = this.ledgerStore.get(prevHeader.getHash(), Block.class);
						this.validatorHandler.updateLocal(prevBlock);
						this.validatorHandler.updateRemote(prevBlock);
					}
					
					if (1==1)
					{
						try
						{
							this.ledgerStore.get(header.getHash(), Block.class);
							System.out.println("Checking integrity of sync block "+header.getHeight()+"@"+header.getHash());
						}
						catch (Exception ex)
						{
							System.out.println("Failed to retrieve full sync block "+header.getHeight()+"@"+header.getHash());
							ex.printStackTrace();
						}
					}
				
					
					header = prevHeader;
				}
				
				// TODO clean up vote power if needed after recovery as could be in a compromised state
			}
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}
	
	public AtomHandler getAtomHandler()
	{
		return this.atomHandler;
	}

	public PackageHandler getPackageHandler()
	{
		return this.packageHandler;
	}

	public ValidatorHandler getValidatorHandler()
	{
		return this.validatorHandler;
	}

	public BlockHandler getBlockHandler()
	{
		return this.blockHandler;
	}

	public StateHandler getStateHandler()
	{
		return this.stateHandler;
	}

	public StatePool getStatePool()
	{
		return this.statePool;
	}

	@VisibleForTesting
	public StateAccumulator getStateAccumulator()
	{
		this.headLock.readLock().lock();
		try
		{
			return this.stateAccumulator;
		}
		finally
		{
			this.headLock.readLock().unlock();
		}
	}
	
	Entry<BlockHeader, StateAccumulator> current()
	{
		this.headLock.readLock().lock();
		try
		{
			return new AbstractMap.SimpleEntry<BlockHeader, StateAccumulator>(this.head.get(), this.stateAccumulator);
		}
		finally
		{
			this.headLock.readLock().unlock();
		}		
	}
	
	Entry<BlockHeader, StateAccumulator> current(final String label)
	{
		this.headLock.readLock().lock();
		try
		{
			return new AbstractMap.SimpleEntry<BlockHeader, StateAccumulator>(this.head.get(), new StateAccumulator(label, this.stateAccumulator));
		}
		finally
		{
			this.headLock.readLock().unlock();
		}		
	}

	LedgerStore getLedgerStore()
	{
		return this.ledgerStore;
	}
	
	public SyncHandler getSyncHandler() 
	{
		return this.syncHandler;
	}

	@JsonGetter("head")
	public BlockHeader getHead()
	{
		this.headLock.readLock().lock();
		try
		{
			return this.head.get();
		}
		finally 
		{
			this.headLock.readLock().unlock();
		}
	}

	@JsonGetter("timestamp")
	public long getTimestamp()
	{
		return this.blockHandler.getTimestampEstimate();
	}

	@JsonGetter("epoch")
	public Epoch getEpoch()
	{
		this.headLock.readLock().lock();
		try
		{
			return this.epoch.get();
		}
		finally 
		{
			this.headLock.readLock().unlock();
		}
	}

	private void commit(final PendingBlock pendingBlock) throws LockException, IOException
	{
		Objects.requireNonNull(pendingBlock, "Pending block is null");
		
		final List<CommitOperation> commitOperations = Lists.mutable.ofInitialCapacity(pendingBlock.getHeader().getInventorySize(InventoryType.COMMITTED)); 
		pendingBlock.forInventory(InventoryType.COMMITTED, pa -> commitOperations.add(pa.getCommitOperation()));
		
		this.lock.writeLock().lock();
		try
		{
			this.ledgerStore.commit(pendingBlock.getBlock(), commitOperations);

			this.headLock.writeLock().lock();
			try
			{
				this.stateAccumulator.unlock(pendingBlock.<PendingAtom>get(InventoryType.COMMITTED));
				this.stateAccumulator.unlock(pendingBlock.<PendingAtom>get(InventoryType.UNCOMMITTED));
				this.stateAccumulator.unlock(pendingBlock.<PendingAtom>get(InventoryType.UNEXECUTED));
				this.stateAccumulator.lock(pendingBlock.<PendingAtom>get(InventoryType.ACCEPTED));
				
				BlockHeader currentHead = this.head.get();
				if (pendingBlock.getHeight() - currentHead.getHeight() != 1)
					throw new IllegalStateException("Head to commit "+pendingBlock.getHeight()+"@"+pendingBlock.getHash()+" is not canonical with current head "+currentHead.getHeight()+"@"+currentHead.getHash());
				
				this.head.set(pendingBlock.getHeader());
				this.epoch.set(Epoch.from(pendingBlock.getHeader()));
				this.context.getNode().setHead(pendingBlock.getHeader());
			}
			finally
			{
				this.headLock.writeLock().unlock();
			}

			this.context.getEvents().post(new BlockCommittedEvent(pendingBlock));
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}

	public <T extends Primitive> T get(Hash hash, Class<T> primitive) throws IOException
	{
		return this.ledgerStore.get(hash, primitive);
	}
	
	public Block getBlock(long height) throws IOException
	{
		Hash committedBlockHash = this.ledgerStore.getSyncBlock(height);
		if (committedBlockHash == null)
			return null;

		return this.ledgerStore.get(committedBlockHash, Block.class);
	}

	public BlockHeader getBlockHeader(long height) throws IOException
	{
		Hash committedBlockHash = this.ledgerStore.getSyncBlock(height);
		if (committedBlockHash == null)
			return null;

		return this.ledgerStore.get(committedBlockHash, BlockHeader.class);
	}

	public boolean submit(final Atom atom)
	{
		Objects.requireNonNull(atom);
		
		return this.atomHandler.submit(atom, false);
	}

	public Collection<Atom> submit(final Collection<Atom> atoms)
	{
		Objects.requireNonNull(atoms, "Atoms is null");
		Numbers.isZero(atoms.size(), "Atoms is empty");
		
		return this.atomHandler.submit(atoms, false);
	}

	@Override
	public Future<AssociationSearchResponse> get(final AssociationSearchQuery query)
	{
		return this.ledgerSearch.get(query);
	}

	@Override
	public Future<SubstateSearchResponse> get(SubstateSearchQuery query)
	{
		return this.ledgerSearch.get(query);
	}
	
	@Override
	public Future<PrimitiveSearchResponse> get(PrimitiveSearchQuery query)
	{
		return this.ledgerSearch.get(query);
	}
	
	boolean isSynced()
	{
		return this.syncHandler.isSynced();
	}
	
	// SHARD GROUP FUNCTIONS //
	public int numShardGroups()
	{
		this.headLock.readLock().lock();
		try
		{
			return numShardGroups(this.epoch.get());
		}
		finally
		{
			this.headLock.readLock().unlock();
		}
	}

	public int numShardGroups(final Hash proposal)
	{
		return numShardGroups(Epoch.from(proposal));
	}

	public int numShardGroups(final long epoch)
	{
		return numShardGroups(Epoch.from(epoch));
	}

	public int numShardGroups(final Epoch epoch)
	{
		// TODO dynamic shard group count from height / epoch
		return Universe.get().shardGroupCount();
	}

	// ASYNC ATOM LISTENER //
	private EventListener asyncAtomListener = new EventListener()
	{
		@Subscribe
		public void on(AtomUnpreparedTimeoutEvent event) 
		{
			context.getMetaData().increment("ledger.processed.atoms.timedout.prepare");
		}

		@Subscribe
		public void on(final AtomCommitEvent event) 
		{
			processAtomCommitEvent(event.getProposalHeader(), event.getPendingAtom(), event.getDecision());
		}
		
		@Subscribe
		public void on(final SyncAtomCommitEvent event) 
		{
			processAtomCommitEvent(event.getProposalHeader(), event.getPendingAtom(), event.getDecision());
		}

		private void processAtomCommitEvent(final BlockHeader header, final PendingAtom pendingAtom, final CommitDecision decision)
		{
			if (ledgerLog.hasLevel(Logging.DEBUG) && decision.equals(CommitDecision.REJECT))
				ledgerLog.warn(Ledger.this.context.getName()+": Atom "+pendingAtom.getHash()+" rejected");
			
			final Epoch epoch = Epoch.from(header);
			final int numShardGroups = numShardGroups(epoch);
			final double writeShardGroups = ShardMapper.toShardGroups(pendingAtom, StateLockMode.WRITE, numShardGroups).size();
			final double shardGroupFactor = numShardGroups / writeShardGroups;
			
			final List<Object> instructions = pendingAtom.getInstructions();
			if (instructions.isEmpty() == false)
			{
				int blobCount = 0;
				int methodCount = 0;
				final MutableObjectIntMap<String> callsByMethod = ObjectIntMaps.mutable.ofInitialCapacity(instructions.size());
				for (int i = 0 ; i < instructions.size() ; i++)
				{
					Object instruction = instructions.get(i);
					if (instruction instanceof Method method)
					{
						callsByMethod.addToValue(method.getMethod(), 1);
						methodCount++;
					}
					else if (instruction instanceof Blob)
						blobCount++;
				}
	
				callsByMethod.forEachKeyValue((method, count) -> {
					Ledger.this.context.getTimeSeries("executions").increment("local:"+method, count, header.getTimestamp(), TimeUnit.MILLISECONDS);
					Ledger.this.context.getTimeSeries("executions").increment("total:"+method, count*shardGroupFactor, header.getTimestamp(), TimeUnit.MILLISECONDS);
				});
				
				Ledger.this.context.getTimeSeries("executions").increment("local:executions", methodCount, header.getTimestamp(), TimeUnit.MILLISECONDS);
				Ledger.this.context.getTimeSeries("executions").increment("total:executions", methodCount*shardGroupFactor, header.getTimestamp(), TimeUnit.MILLISECONDS);
				
				Ledger.this.context.getMetaData().increment("ledger.processed.executions.local", methodCount);
				Ledger.this.context.getMetaData().increment("ledger.processed.executions.total", methodCount*shardGroupFactor);

				if (blobCount > 0)
				{
					Ledger.this.context.getTimeSeries("executions").increment("local:"+Blob.class.getAnnotation(StateContext.class).value(), blobCount, header.getTimestamp(), TimeUnit.MILLISECONDS);
					Ledger.this.context.getTimeSeries("executions").increment("total:"+Blob.class.getAnnotation(StateContext.class).value(), blobCount*shardGroupFactor, header.getTimestamp(), TimeUnit.MILLISECONDS);
				}
			}
		}

		long lastProgressPhase = System.currentTimeMillis();

		@Subscribe
		public void on(final ProgressPhaseEvent event)
		{
			if (event.getProgressPhase().equals(State.TRANSITION) || event.getProgressPhase().equals(State.VOTING))
			{
				Ledger.this.context.getTimeSeries("proposals").put("phase", System.currentTimeMillis() - lastProgressPhase, System.currentTimeMillis(), TimeUnit.MILLISECONDS);
				lastProgressPhase = System.currentTimeMillis();
			}
			else if (event.getProgressPhase().equals(State.COMPLETED))
				Ledger.this.context.getTimeSeries("proposals").put("round", event.getProgressRound().getDuration(), System.currentTimeMillis(), TimeUnit.MILLISECONDS);
			
			Ledger.this.context.getMetaData().put("ledger.interval.phase", Ledger.this.context.getTimeSeries("proposals").mean("phase", 30, System.currentTimeMillis(), TimeUnit.MILLISECONDS));
			Ledger.this.context.getMetaData().put("ledger.interval.round", Ledger.this.context.getTimeSeries("proposals").mean("round", 10, System.currentTimeMillis(), TimeUnit.MILLISECONDS));
			Ledger.this.context.getMetaData().put("ledger.interval.commit", Ledger.this.context.getTimeSeries("proposals").mean("commit", 10, System.currentTimeMillis(), TimeUnit.MILLISECONDS));
		}
	};
	
	// SYNCHRONOUS ATOM LISTENER //
	private SynchronousEventListener syncAtomListener = new SynchronousEventListener()
	{
		@Subscribe
		public void on(final AtomAcceptedEvent event)
		{
			if (ledgerLog.hasLevel(Logging.DEBUG))
			{
				ledgerLog.debug(Ledger.this.context.getName()+": Atom "+event.getPendingAtom().getHash()+" accepted into block "+event.getPendingAtom().getBlockHeader().getHash());
				ledgerLog.debug(Ledger.this.context.getName()+":   "+event.getPendingAtom().numStateAddresses(null)+" substates "+event.getPendingAtom().getStateAddresses(null));
			}
		}
	};
	
	// SYNCHRONOUS BLOCK LISTENER //
	private SynchronousEventListener syncBlockListener = new SynchronousEventListener()
	{
		@Subscribe
		public void on(final BlockCommitEvent event) throws LockException, IOException 
		{
			commit(event.getPendingBlock());
		}
		
		@Subscribe
		public void on(final BlockCommittedEvent event) throws LockException, IOException 
		{
			if (Ledger.this.context.getNode().isSynced() == true && Ledger.this.context.getNode().isProgressing() == false)
			{
				Ledger.this.context.getNode().setProgressing(true);
				ledgerLog.info(Ledger.this.context.getName()+":    Post sync liveness is restored");
			}
		}
		
		@Subscribe
		public void on(final SyncBlockEvent event) throws IOException 
		{
			sync(event.getBlock());
		}

		private void commit(final PendingBlock pendingBlock) throws LockException, IOException
		{
			if (pendingBlock.getHeader().getPrevious().equals(Ledger.this.getHead().getHash()) == false)
			{
				ledgerLog.error(Ledger.this.context.getName()+": Committed block "+pendingBlock.getHeader()+" does not attach to current head "+Ledger.this.getHead());
				return;
			}
			
			if (ledgerLog.hasLevel(Logging.DEBUG))
				ledgerLog.debug(Ledger.this.context.getName()+": Committing block "+pendingBlock.getHeader());
			
			long commitStart = System.currentTimeMillis();
			Ledger.this.commit(pendingBlock);
			Ledger.this.context.getTimeSeries("proposals").put("commit", System.currentTimeMillis()-commitStart, commitStart, TimeUnit.MILLISECONDS);
			
			stats(pendingBlock.getBlock());
			
			// FIXME Have to do finality here for now
			long wallClockTimestamp = System.currentTimeMillis();
			long leddgerClockTimestamp = pendingBlock.getHeader().getTimestamp();
			long finalityClient = 0;
			long finalityConsensus = 0;
			if (pendingBlock.getHeader().getInventorySize(InventoryType.COMMITTED) > 0)
			{
				long finalityClientSum = pendingBlock.<PendingAtom>get(InventoryType.COMMITTED).stream().map(pa -> pa.getWitnessedAt()).collect(Collectors.summingLong(t -> wallClockTimestamp - t));
				long finalityConsensusSum = pendingBlock.<PendingAtom>get(InventoryType.COMMITTED).stream().map(pa -> pa.getAcceptedAt()).collect(Collectors.summingLong(t -> leddgerClockTimestamp - t));
				finalityClient = finalityClientSum / pendingBlock.getHeader().getInventorySize(InventoryType.COMMITTED);
				finalityConsensus = finalityConsensusSum / pendingBlock.getHeader().getInventorySize(InventoryType.COMMITTED);
			}
			Ledger.this.context.getTimeSeries("finality").put("client", finalityClient, wallClockTimestamp, TimeUnit.MILLISECONDS);
			Ledger.this.context.getTimeSeries("finality").put("consensus", finalityConsensus, leddgerClockTimestamp, TimeUnit.MILLISECONDS);
			
			double finalityClientMean = Ledger.this.context.getTimeSeries("finality").mean("client", 30, wallClockTimestamp, TimeUnit.MILLISECONDS);
			double finalityConsensusMean = Ledger.this.context.getTimeSeries("finality").mean("consensus", 30, leddgerClockTimestamp, TimeUnit.MILLISECONDS);
			Ledger.this.context.getMetaData().put("ledger.throughput.latency.accepted", finalityConsensusMean);
			Ledger.this.context.getMetaData().put("ledger.throughput.latency.witnessed", finalityClientMean);
		}
			
		private void sync(final Block block) throws IOException
		{
			if (block.getHeader().getPrevious().equals(Ledger.this.getHead().getHash()) == false)
			{
				ledgerLog.error(Ledger.this.context.getName()+": Synced block "+block.getHeader()+" does not attach to current head "+Ledger.this.getHead());
				return;
			}
			
			Ledger.this.headLock.writeLock().lock();
			try
			{
				Ledger.this.head.set(block.getHeader());
				Ledger.this.epoch.set(Epoch.from(block.getHeader()));
				Ledger.this.context.getNode().setHead(block.getHeader());
			}
			finally
			{
				Ledger.this.headLock.writeLock().unlock();
			}
			
			ledgerLog.info(Ledger.this.context.getName()+": Synced block with "+block.getHeader().getInventorySize(InventoryType.ACCEPTED)+" atoms "+
																				block.getHeader().getInventorySize(InventoryType.UNACCEPTED)+" unaccepted "+
																				block.getHeader().getInventorySize(InventoryType.COMMITTED)+" certificates "+
																				(block.getHeader().getInventorySize(InventoryType.UNEXECUTED)+block.getHeader().getInventory(InventoryType.UNCOMMITTED).size())+" timeouts "+
																				block.getHeader().getInventorySize(InventoryType.EXECUTABLE)+" executable signals "+
																				block.getHeader().getInventorySize(InventoryType.LATENT)+" latent signals "+
																				block.getHeader().getInventorySize(InventoryType.PACKAGES)+" packages "+
																				block.getHeader());
			stats(block);
		}

		private void stats(final Block block) throws IOException
		{
			final Epoch epoch = Epoch.from(block.getHeader());
			final int numShardGroups = Ledger.this.numShardGroups(epoch);

			if (ledgerLog.hasLevel(Logging.DEBUG))
				ledgerLog.info(Ledger.this.context.getName()+": "+Ledger.this.context.getLedger().getStateAccumulator().locked().size()+" locked in accumulator "+Ledger.this.context.getLedger().getStateAccumulator().checksum());
			
			Ledger.this.context.getMetaData().increment("ledger.session.blocks");
//			if (block.getHeader().getCertificate() != null)
//				Ledger.this.context.getMetaData().increment("ledger.session.blocks.supers");
			
			double avgShardsTouched = 1;
			double shardsTouchedFactor = 1;
			long shardsTouchedIncrement = 0;
			long certificatesAcceptedIncrement = 0;
			long certificatesRejectedIncrement = 0;
			Set<ShardGroupID> shardGroupsTouched = Sets.mutable.ofInitialCapacity(numShardGroups);
			
			// COMMITTED //
			for (AtomCertificate atomCertificate : block.getCertificates())
			{
				shardGroupsTouched.clear();
				for (StateCertificate stateCertificate : atomCertificate.getInventory(StateCertificate.class))
					shardGroupsTouched.add(ShardMapper.toShardGroup(stateCertificate.getAddress(), numShardGroups));
					
				shardsTouchedIncrement += shardGroupsTouched.size();
				
				if (atomCertificate.getDecision().equals(CommitDecision.ACCEPT))
					certificatesAcceptedIncrement++;
				else if (atomCertificate.getDecision().equals(CommitDecision.REJECT))
					certificatesRejectedIncrement++;
			}
			
			// SHARDS //
			if (shardsTouchedIncrement > 0)
			{
				avgShardsTouched = ((double)shardsTouchedIncrement) / (certificatesAcceptedIncrement + certificatesRejectedIncrement);
				shardsTouchedFactor = numShardGroups / avgShardsTouched;
			}
			Ledger.this.context.getTimeSeries("shardratio").put("touched", avgShardsTouched, block.getHeader().getTimestamp(), TimeUnit.MILLISECONDS);
			Ledger.this.context.getMetaData().put("ledger.throughput.shards.touched", Ledger.this.context.getTimeSeries("shardratio").average("touched", System.currentTimeMillis()-TimeUnit.SECONDS.toMillis(30), TimeUnit.MILLISECONDS));
			
			// OTHER STUFF //
			Ledger.this.context.getMetaData().increment("ledger.processed.atoms.total", (long) shardsTouchedFactor * (certificatesAcceptedIncrement + certificatesRejectedIncrement));
			Ledger.this.context.getMetaData().increment("ledger.commits.certificates.accept", certificatesAcceptedIncrement);
			Ledger.this.context.getMetaData().increment("ledger.commits.certificates.reject", certificatesRejectedIncrement);
			Ledger.this.context.getMetaData().increment("ledger.commits.certificates", certificatesAcceptedIncrement + certificatesRejectedIncrement);
				
			// LATENT //
			Ledger.this.context.getMetaData().increment("ledger.processed.atoms.latent.execution", block.getHeader().getInventorySize(InventoryType.LATENT));
			for (Hash latent : block.getLatent())
			{
				SubstateCommit substateCommit = Ledger.this.getLedgerStore().search(StateAddress.from(Atom.class, latent));
				if (substateCommit == null)
					ledgerLog.warn(Ledger.this.context.getName()+": FIX THIS USING LEDGERSTORE PENDING ATOM SUBSTATES - Substate for latent execution atom "+latent+" not found");
			}

			// UNEXECUTED //
			for (ExecutionTimeout timeout : block.getUnexecuted())
			{
				SubstateCommit substateCommit = Ledger.this.getLedgerStore().search(StateAddress.from(Atom.class, timeout.getAtom()));
				if (substateCommit == null)
				{
					ledgerLog.warn(Ledger.this.context.getName()+": Substate for unexecuted atom "+timeout.getAtom()+" not found");
					continue;
				}
				
				if (substateCommit.getSubstate().get(NativeField.TIMEOUT) != null)
					Ledger.this.context.getMetaData().increment("ledger.processed.atoms.timedout.execution");
			}

			// UNCOMMITTED //
			for (CommitTimeout timeout : block.getUncommitted())
			{
				SubstateCommit substateCommit = Ledger.this.getLedgerStore().search(StateAddress.from(Atom.class, timeout.getAtom()));
				if (substateCommit == null)
				{
					ledgerLog.warn(Ledger.this.context.getName()+": Substate for uncommitted atom "+timeout.getAtom()+" not found");
					continue;
				}
				
				if (substateCommit.getSubstate().get(NativeField.TIMEOUT) != null)
					Ledger.this.context.getMetaData().increment("ledger.processed.atoms.timedout.commit");
			}

			// ACCEPTED //
			Ledger.this.context.getMetaData().increment("ledger.processed.atoms.local", block.getHeader().getInventorySize(InventoryType.ACCEPTED));
			if (ledgerLog.hasLevel(Logging.DEBUG))
				for (Atom atom : block.getAccepted())
					ledgerLog.debug(Ledger.this.context.getName()+": Pre-committed atom "+atom.getHash()+" in "+block.getHeader());

			// UNACCEPTED //
			Ledger.this.context.getMetaData().increment("ledger.processed.atoms.timedout.accept", block.getUnaccepted().size());

			// Hack for single shard as "estimates" dont work too well
			long localProcessedAtoms = Ledger.this.context.getMetaData().get("ledger.processed.atoms.local", 0l);
			long totalProcessedAtoms = Ledger.this.context.getMetaData().get("ledger.processed.atoms.total", 0l);
			if (totalProcessedAtoms < localProcessedAtoms)
				Ledger.this.context.getMetaData().put("ledger.processed.atoms.total", localProcessedAtoms);
			
			Ledger.this.context.getTimeSeries("throughput").increment("local", block.getHeader().getInventorySize(InventoryType.COMMITTED), block.getHeader().getTimestamp(), TimeUnit.MILLISECONDS);
			Ledger.this.context.getTimeSeries("throughput").increment("total", block.getHeader().getInventorySize(InventoryType.COMMITTED)*shardsTouchedFactor, block.getHeader().getTimestamp(), TimeUnit.MILLISECONDS);

			Ledger.this.context.getMetaData().put("ledger.throughput.atoms.local", Ledger.this.context.getTimeSeries("throughput").average("local", System.currentTimeMillis()-30000, TimeUnit.MILLISECONDS));
			Ledger.this.context.getMetaData().put("ledger.throughput.atoms.total", Ledger.this.context.getTimeSeries("throughput").average("total", System.currentTimeMillis()-30000, TimeUnit.MILLISECONDS));

			Ledger.this.context.getMetaData().put("ledger.throughput.executions.local", Ledger.this.context.getTimeSeries("executions").average("local:executions", System.currentTimeMillis()-30000, TimeUnit.MILLISECONDS));
			Ledger.this.context.getMetaData().put("ledger.throughput.executions.total", Ledger.this.context.getTimeSeries("executions").average("total:executions", System.currentTimeMillis()-30000, TimeUnit.MILLISECONDS));
		}
	};
	
	// ASYNCHRONOUS BLOCK LISTENER //
	private EventListener asyncBlockListener = new EventListener()
	{
		@Subscribe
		public void on(BlockCommittedEvent blockCommittedEvent)
		{
			
			try
			{
				if (ledgerLog.hasLevel(Logging.INFO))
				{
					final PendingBlock.SUPR superType = blockCommittedEvent.getPendingBlock().isSuper();
					final String superText;
					switch(superType)
					{
						case INTR: superText = ""; break;
						case SOFT: superText = "soft super"; break;
						case HARD: superText = "hard super"; break;
						default: superText = "unknown";
					}
					
					ledgerLog.info(Ledger.this.context.getName()+": Committed "+superText+" block with "+blockCommittedEvent.getPendingBlock().getHeader().getInventorySize(InventoryType.ACCEPTED)+" atoms "+
																				 blockCommittedEvent.getPendingBlock().getHeader().getInventorySize(InventoryType.UNACCEPTED)+" unaccepted "+
																				 blockCommittedEvent.getPendingBlock().getHeader().getInventorySize(InventoryType.COMMITTED)+" certificates "+
																				 (blockCommittedEvent.getPendingBlock().getHeader().getInventorySize(InventoryType.UNEXECUTED)+blockCommittedEvent.getPendingBlock().getHeader().getInventory(InventoryType.UNCOMMITTED).size())+" timeouts "+
																				 blockCommittedEvent.getPendingBlock().getHeader().getInventorySize(InventoryType.EXECUTABLE)+" executable signals "+
																				 blockCommittedEvent.getPendingBlock().getHeader().getInventorySize(InventoryType.LATENT)+" latent signals "+
																				 blockCommittedEvent.getPendingBlock().getHeader().getInventorySize(InventoryType.PACKAGES)+" packages "+
																				 blockCommittedEvent.getPendingBlock().getHeader()+" "+blockCommittedEvent.getPendingBlock().getBlock().getSize()+" bytes");
				}
	
				Ledger.this.context.getMetaData().increment("ledger.blocks.bytes", blockCommittedEvent.getPendingBlock().getBlock().getSize());
			}
			catch (IOException ioex)
			{
				ledgerLog.error(Ledger.this.context.getName()+": Could not serialize committed block "+blockCommittedEvent.getPendingBlock().getHash()+" for statistics", ioex);
			}
		}
	};
	
	// SYNC CHANGE LISTENER //
	private SynchronousEventListener syncChangeListener = new SynchronousEventListener()
	{
		@Subscribe
		public void on(final SyncAcquiredEvent event) throws LockException 
		{
			ledgerLog.log(Ledger.this.context.getName()+": Sync status acquired, loading state accumulator");
			ledgerLog.log(Ledger.this.context.getName()+":    Sync accumulator checksum is "+event.getAccumulator().checksum());

			Ledger.this.lock.writeLock().lock();
			try
			{
				Ledger.this.headLock.writeLock().lock();
				try
				{
					Ledger.this.stateAccumulator.reset();
					Ledger.this.stateAccumulator.lock(event.getAccumulator());
					Ledger.this.head.set(event.getHead());
					Ledger.this.epoch.set(Epoch.from(event.getHead()));
				}
				finally
				{
					Ledger.this.headLock.writeLock().unlock();
				}
				
				if (event.getHead().getHash().equals(Universe.get().getGenesis().getHash()) == true)
					Ledger.this.context.getNode().setProgressing(true);

				if (ledgerLog.hasLevel(Logging.INFO))
				{
					ledgerLog.info(Ledger.this.context.getName()+":    Ledger accumulator checksum is "+Ledger.this.stateAccumulator.checksum());
					if (Ledger.this.context.getNode().isProgressing() == false)
						ledgerLog.info(Ledger.this.context.getName()+":    Liveness is pending ...");
				}
			}
			finally
			{
				Ledger.this.lock.writeLock().unlock();
			}
		}
		
		@Subscribe
		public void on(final SyncLostEvent event) 
		{
			Ledger.this.lock.writeLock().lock();
			try
			{
				ledgerLog.log(Ledger.this.context.getName()+": Sync status lost, flushing state accumulator");
		
				Ledger.this.headLock.writeLock().lock();
				try
				{
					Ledger.this.stateAccumulator.reset();
				}
				finally
				{
					Ledger.this.headLock.writeLock().unlock();
				}
			}
			finally
			{
				Ledger.this.lock.writeLock().unlock();
			}
		}
	};
	
	// PEER LISTENER //
	private EventListener peerListener = new EventListener()
	{
    	@Subscribe
		public void on(final ConnectedEvent event)
		{
    		try
    		{
    			final ShardGroupID localShardGroupID = ShardMapper.toShardGroup(Ledger.this.context.getNode().getIdentity(), Ledger.this.numShardGroups());
    			final ShardGroupID remoteShardGroupID = ShardMapper.toShardGroup(event.getConnection().getNode().getIdentity(), Ledger.this.numShardGroups());
    			if (localShardGroupID.equals(remoteShardGroupID) == false)
    				return;
    			
    			// Reasons NOT to ask for all the pool inventories
    			if (Ledger.this.context.getNode().isSynced() == false)
    				return;
    			
    			if (Ledger.this.getHead().getHash().equals(Universe.get().getGenesis().getHash()) == false && 
    				Ledger.this.getHead().getHeight() >= event.getConnection().getNode().getHead().getHeight() - Constants.OOS_TRIGGER_LIMIT_BLOCKS)
    				return;

    			// Lagging, ask for inventory
    			final List<Hash> knownAtoms = Ledger.this.getAtomHandler().getAll().stream().filter(pa -> pa.getAtom() != null).map(pa -> pa.getHash()).collect(Collectors.toList());
    			Ledger.this.context.getNetwork().getMessaging().send(new SyncAcquiredMessage(Ledger.this.getHead(), knownAtoms), event.getConnection());
   				ledgerLog.info(Ledger.this.context.getName()+": Requesting full inventory from "+event.getConnection());
    		}
    		catch (IOException ioex)
    		{
    			ledgerLog.error(Ledger.this.context.getName()+": Failed to request sync connected items from "+event.getConnection(), ioex);
    		}
		}
	};
}
 