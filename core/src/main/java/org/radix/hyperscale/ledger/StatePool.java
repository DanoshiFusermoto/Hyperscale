package org.radix.hyperscale.ledger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.eclipse.collections.api.factory.Maps;
import org.eclipse.collections.api.factory.Sets;
import org.radix.hyperscale.Constants;
import org.radix.hyperscale.Context;
import org.radix.hyperscale.Service;
import org.radix.hyperscale.collections.MappedBlockingQueue;
import org.radix.hyperscale.common.Primitive;
import org.radix.hyperscale.crypto.CryptoException;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.crypto.bls12381.BLS12381;
import org.radix.hyperscale.crypto.bls12381.BLSPublicKey;
import org.radix.hyperscale.crypto.bls12381.BLSSignature;
import org.radix.hyperscale.events.EventListener;
import org.radix.hyperscale.events.SyncLostEvent;
import org.radix.hyperscale.events.SynchronousEventListener;
import org.radix.hyperscale.exceptions.StartupException;
import org.radix.hyperscale.exceptions.TerminationException;
import org.radix.hyperscale.exceptions.ValidationException;
import org.radix.hyperscale.executors.LatchedProcessor;
import org.radix.hyperscale.ledger.AtomStatus.State;
import org.radix.hyperscale.ledger.BlockHeader.InventoryType;
import org.radix.hyperscale.ledger.events.AtomExceptionEvent;
import org.radix.hyperscale.ledger.events.AtomExecuteLatentEvent;
import org.radix.hyperscale.ledger.events.AtomExecutedEvent;
import org.radix.hyperscale.ledger.events.AtomExecutionTimeoutEvent;
import org.radix.hyperscale.ledger.events.BlockCommittedEvent;
import org.radix.hyperscale.ledger.events.ProgressPhaseEvent;
import org.radix.hyperscale.ledger.events.StateCertificateConstructedEvent;
import org.radix.hyperscale.ledger.events.SyncAcquiredEvent;
import org.radix.hyperscale.ledger.messages.SyncAcquiredMessage;
import org.radix.hyperscale.ledger.messages.SyncAcquiredMessageProcessor;
import org.radix.hyperscale.ledger.primitives.StateCertificate;
import org.radix.hyperscale.ledger.timeouts.AtomTimeout;
import org.radix.hyperscale.ledger.timeouts.ExecutionTimeout;
import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;
import org.radix.hyperscale.network.AbstractConnection;
import org.radix.hyperscale.network.GossipFetcher;
import org.radix.hyperscale.network.GossipFilter;
import org.radix.hyperscale.network.GossipInventory;
import org.radix.hyperscale.network.GossipReceiver;
import org.radix.hyperscale.time.Time;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.eventbus.Subscribe;
import com.sleepycat.je.OperationStatus;

public final class StatePool implements Service
{
	private static final Logger syncLog = Logging.getLogger("sync");
	private static final Logger statePoolLog = Logging.getLogger("statepool");
	private static final Logger gossipLog = Logging.getLogger("gossip");
	
	private final Context context;
	
	private LatchedProcessor voteProcessor = new LatchedProcessor(1, TimeUnit.SECONDS)
	{
		@Override
		public void process()
		{
			if (StatePool.this.context.getNode().isSynced() == false)
				return;
						
			// Cache this for this iteration
			final BlockHeader head = StatePool.this.context.getLedger().getHead();
			
			_collateStateVoteBlocks(head);
			
			_processStateVoteBlocks();

			_castVotesAndCollect();
		}

		@Override
		public void onError(Throwable thrown) 
		{
			statePoolLog.fatal(StatePool.this.context.getName()+": Error processing state vote queue", thrown);
		}

		@Override
		public void onTerminated()
		{
			statePoolLog.log(StatePool.this.context.getName()+": State vote processor is terminated");
		}
	};

	private final MappedBlockingQueue<Hash, PendingState> votesToCastQueue;

	private final Map<Hash, StateVoteBlock> voteBlocksToCollect;
	private final MappedBlockingQueue<Hash, StateVoteBlock> voteBlocksToProcessQueue;

	private final Multimap<Hash, StateVoteCollector> stateVoteCollectors;
	private final Map<Hash, StateVoteBlockCollector> stateVoteBlockCollectors;

	StatePool(Context context)
	{
		this.context = Objects.requireNonNull(context, "Context is null");
		
		this.votesToCastQueue = new MappedBlockingQueue<Hash, PendingState>(this.context.getConfiguration().get("ledger.state.queue", 1<<14));
		
		this.voteBlocksToCollect = new ConcurrentHashMap<Hash, StateVoteBlock>();
		this.voteBlocksToProcessQueue = new MappedBlockingQueue<Hash, StateVoteBlock>(this.context.getConfiguration().get("ledger.state.queue", 1<<14));
		
		this.stateVoteCollectors = Multimaps.synchronizedMultimap(HashMultimap.create());
		this.stateVoteBlockCollectors = new ConcurrentHashMap<Hash, StateVoteBlockCollector>();
	}
	
	@Override
	public void start() throws StartupException
	{
		// STATE REPRESENTATIONS GOSSIP //
		this.context.getNetwork().getGossipHandler().register(StateVoteBlock.class, new GossipFilter<StateVoteBlock>(this.context) 
		{
			@Override
			public Set<ShardGroupID> filter(StateVoteBlock stateVoteBlock)
			{
				return Collections.singleton(ShardMapper.toShardGroup(StatePool.this.context.getNode().getIdentity(), StatePool.this.context.getLedger().numShardGroups()));
			}
		});

		this.context.getNetwork().getGossipHandler().register(StateVoteBlock.class, new GossipInventory() 
		{
			@Override
			public Collection<Hash> required(final Class<? extends Primitive> type, final Collection<Hash> items, final AbstractConnection connection) throws IOException
			{
				if (StatePool.this.context.getNode().isSynced() == false)
					return Collections.emptyList();
				
				if (type.equals(StateVoteBlock.class) == false)
				{
					gossipLog.error(StatePool.this.context.getName()+": State vote block type expected but got "+type);
					return Collections.emptyList();
				}
				
				List<Hash> required = new ArrayList<Hash>(items.size());
				for (Hash item : items)
				{
					if (StatePool.this.voteBlocksToCollect.containsKey(item) == false) 
						required.add(item);
				}
				
				required.removeAll(StatePool.this.voteBlocksToProcessQueue.contains(required));
				required.removeAll(StatePool.this.context.getLedger().getLedgerStore().has(required, type));
				return required;
			}
		});

		this.context.getNetwork().getGossipHandler().register(StateVoteBlock.class, new GossipReceiver<StateVoteBlock>() 
		{
			@Override
			public void receive(final Collection<StateVoteBlock> stateVoteBlocks, final AbstractConnection connection) throws IOException, CryptoException
			{
				if (StatePool.this.context.getNode().isSynced() == false)
					return;
				
				final int numShardGroups = StatePool.this.context.getLedger().numShardGroups();
				final ShardGroupID localShardGroupID = ShardMapper.toShardGroup(StatePool.this.context.getNode().getIdentity(), numShardGroups); 
				for (final StateVoteBlock stateVoteBlock : stateVoteBlocks)
				{
					if (statePoolLog.hasLevel(Logging.DEBUG))
					{
						if (stateVoteBlock.isHeader() == false)
							statePoolLog.info(StatePool.this.context.getName()+": Received state vote block "+stateVoteBlock);
						else
							statePoolLog.info(StatePool.this.context.getName()+": Received state vote block header "+stateVoteBlock);
					}
					
					if (stateVoteBlock.getOwner().getIdentity().equals(StatePool.this.context.getNode().getIdentity()))
					{
						statePoolLog.error(StatePool.this.context.getName()+": Received own state vote block "+stateVoteBlock+" from "+connection);
						// TODO disconnect and ban;
						continue;
					}
					
					if (stateVoteBlock.isHeader() == false)
					{
						final List<StateAddress> stateAddresses = stateVoteBlock.getStateAddresses();
						for (int i = 0 ; i < stateAddresses.size() ; i++)
						{
							final StateAddress stateAddress = stateAddresses.get(i);
							final ShardGroupID stateVoteShardGroupID = ShardMapper.toShardGroup(stateAddress, numShardGroups);
							if (localShardGroupID.equals(stateVoteShardGroupID) == false)
							{
								statePoolLog.warn(StatePool.this.context.getName()+": State vote on "+stateAddress+" for "+stateVoteBlock.getOwner().toString(Constants.TRUNCATED_IDENTITY_LENGTH)+" is for shard group ID "+stateVoteShardGroupID+" but expected local shard group ID "+localShardGroupID);
								// TODO disconnect and ban;
								continue;
							}
						}
					}
					
					if (StatePool.this.context.getLedger().getLedgerStore().store(stateVoteBlock).equals(OperationStatus.SUCCESS))
					{
						if (StatePool.this.context.getNetwork().getGossipHandler().broadcast(stateVoteBlock.isHeader() ? stateVoteBlock : stateVoteBlock.getHeader(), localShardGroupID) == false)
							statePoolLog.warn(StatePool.this.context.getName()+": Failed to broadcast state vote block "+stateVoteBlock);

						StatePool.this.voteBlocksToCollect.putIfAbsent(stateVoteBlock.getHash(), stateVoteBlock);
					}
					else
						statePoolLog.warn(StatePool.this.context.getName()+": Failed to store state vote block "+stateVoteBlock);
				}
				
				StatePool.this.voteProcessor.signal();
			}
		});
					
		this.context.getNetwork().getGossipHandler().register(StateVoteBlock.class, new GossipFetcher<StateVoteBlock>() 
		{
			@Override
			public Collection<StateVoteBlock> fetch(final Collection<Hash> items, final AbstractConnection connection) throws IOException
			{
				List<StateVoteBlock> fetched = new ArrayList<StateVoteBlock>();
				for (Hash item : items)
				{
					StateVoteBlock stateVoteBlock = StatePool.this.voteBlocksToCollect.get(item);
					if (stateVoteBlock == null)
						stateVoteBlock =  StatePool.this.voteBlocksToProcessQueue.get(item);
					if (stateVoteBlock == null)
						stateVoteBlock = StatePool.this.context.getLedger().getLedgerStore().get(item, StateVoteBlock.class);
					if (stateVoteBlock == null)
					{
						statePoolLog.error(StatePool.this.context.getName()+": Requested state vote block not found "+item);
						continue;
					}
					
					fetched.add(stateVoteBlock.getHeader());
				}

				return fetched;
			}
		});
		
		// SYNC //
		this.context.getNetwork().getMessaging().register(SyncAcquiredMessage.class, this.getClass(), new SyncAcquiredMessageProcessor(this.context)
		{
			@Override
			public void process(final SyncAcquiredMessage syncAcquiredMessage, final AbstractConnection connection)
			{
				int numShardGroups = StatePool.this.context.getLedger().numShardGroups();
				ShardGroupID localShardGroupID = ShardMapper.toShardGroup(StatePool.this.context.getNode().getIdentity(), numShardGroups);
				ShardGroupID remoteShardGroupID = ShardMapper.toShardGroup(connection.getNode().getIdentity(), numShardGroups);

				if (remoteShardGroupID.equals(localShardGroupID) == false)
				{
					statePoolLog.error(StatePool.this.context.getName()+": Received SyncAcquiredMessage from "+connection+" in shard group ID "+remoteShardGroupID+" but local shard group ID is "+localShardGroupID);
					// Disconnect and ban?
					return;
				}
				
				try
				{
					if (statePoolLog.hasLevel(Logging.DEBUG))
						statePoolLog.debug(StatePool.this.context.getName()+": State pool inventory request from "+connection);
					
					final Set<Hash> deferredStateVoteBlockInventory = new LinkedHashSet<Hash>();
					StatePool.this.context.getLedger().getAtomHandler().getAll().forEach(pa -> {
						if (pa.getStatus().after(State.NONE) == false)
							return;

						pa.getStateVoteBlocks().forEach(svb -> 
						{
							if (svb.getOwner().getIdentity().equals(connection.getNode().getIdentity()))
								return;
							
							deferredStateVoteBlockInventory.add(svb.getHash());
						});
					});
					
					StatePool.this.stateVoteBlockCollectors.forEach((h, svbc) -> {
						svbc.getStateVoteBlocks().forEach(svb -> 
						{
							if (svb.getOwner().getIdentity().equals(connection.getNode().getIdentity()))
								return;
							
							deferredStateVoteBlockInventory.add(svb.getHash());
						});
					});
					
					int broadcastedStateVoteBlockCount = 0;
					final Set<Hash> stateVoteBlockInventory = new LinkedHashSet<Hash>();
					long syncInventoryHeight = Math.max(1, syncAcquiredMessage.getHead().getHeight() - Constants.SYNC_INVENTORY_HEAD_OFFSET);
					while (syncInventoryHeight <= StatePool.this.context.getLedger().getHead().getHeight())
					{
						StatePool.this.context.getLedger().getLedgerStore().getSyncInventory(syncInventoryHeight, StateVoteBlock.class).forEach(ir -> {
							if (ir.<StateVoteBlock>get().getOwner().getIdentity().equals(connection.getNode().getIdentity()))
								return;
							
							stateVoteBlockInventory.add(ir.getHash());
							deferredStateVoteBlockInventory.remove(ir.getHash());
						});
						
						broadcastedStateVoteBlockCount += broadcastSyncInventory(stateVoteBlockInventory, StateVoteBlock.class, Constants.MAX_REQUEST_INVENTORY_ITEMS, connection);

						syncInventoryHeight++;
					}

					// Send any remaining (also now send the deferred)
					broadcastedStateVoteBlockCount += broadcastSyncInventory(stateVoteBlockInventory, StateVoteBlock.class, 0, connection);
					broadcastedStateVoteBlockCount += broadcastSyncInventory(deferredStateVoteBlockInventory, StateVoteBlock.class, 0, connection);

					syncLog.log(StatePool.this.context.getName()+": Broadcasted "+broadcastedStateVoteBlockCount+" state vote blocks to "+connection);
				}
				catch (Exception ex)
				{
					statePoolLog.error(StatePool.this.context.getName()+":  ledger.messages.sync.acquired " + connection, ex);
				}
			}
		});

		this.context.getEvents().register(this.syncChangeListener);
		this.context.getEvents().register(this.syncBlockListener);
		this.context.getEvents().register(this.syncAtomListener);
		this.context.getEvents().register(this.asyncBlockListener);
		this.context.getEvents().register(this.asyncProgressListener);
		
		Thread voteProcessorThread = new Thread(this.voteProcessor);
		voteProcessorThread.setDaemon(true);
		voteProcessorThread.setName(this.context.getName()+" State pool Processor");
		voteProcessorThread.start();
	}

	@Override
	public void stop() throws TerminationException
	{
		this.voteProcessor.terminate(true);
		this.context.getEvents().unregister(this.syncAtomListener);
		this.context.getEvents().unregister(this.syncBlockListener);
		this.context.getEvents().unregister(this.syncChangeListener);
		this.context.getEvents().unregister(this.asyncProgressListener);
		this.context.getEvents().unregister(this.asyncBlockListener);
		
		this.context.getNetwork().getMessaging().deregisterAll(this.getClass());
	}

	// PROCESSING PIPELINE //
	private void _collateStateVoteBlocks(final BlockHeader head)
	{
		if (this.voteBlocksToCollect.isEmpty() == false)
		{
			final List<Hash> stateVoteBlocksToCollectRemovals = new ArrayList<Hash>(Math.min(StatePool.this.voteBlocksToCollect.size(), StatePool.this.context.getLedger().numShardGroups(head.getHeight())));
			StatePool.this.voteBlocksToCollect.forEach((h,svb) -> 
			{
				try
				{
					// Process up to one above the head, allows catch up if local instance is a lagging
					if (svb.getHeight() > head.getHeight()+1)
						return;

					// TODO still lots to do here regarding non-aligned SVBs.  Need to figure out when and how to fetch them from the 
					//		producing validator, queue and verify them individually
					// TODO implement a means to trigger various conditions here via tests
					final Epoch epoch = Epoch.from(svb.getBlock());
					final int numShardGroups = StatePool.this.context.getLedger().numShardGroups(epoch);
					final ShardGroupID localShardGroupID = ShardMapper.toShardGroup(StatePool.this.context.getNode().getIdentity(), numShardGroups);
					final long voteThreshold = StatePool.this.context.getLedger().getValidatorHandler().getVotePowerThreshold(epoch, localShardGroupID);
					final StateVoteBlockCollector stateVoteBlockCollector = this.stateVoteBlockCollectors.computeIfAbsent(svb.getID(), s -> new StateVoteBlockCollector(this.context, svb.getID(), svb.getBlock(), voteThreshold, Time.getSystemTime()));
					
					// TODO Currently ignoring SVBs once a super majority is known.  Perhaps the wrong approach, especially
					//		if some form of slashing is to be implemented to penalise those that have not voted for an extended
					//		period of time, will need to process ALL SVBs.  Likely need a new verification pipeline to ensure 
					//		authentication complexity remains low when processing SVBs received after the threshold is met.
					if (stateVoteBlockCollector.isCompleted())
					{
						if (svb.isHeader() == false && statePoolLog.hasLevel(Logging.INFO))
							statePoolLog.warn(this.context.getName()+": Removed superflous delayed state vote block with header "+svb.toString()+" by "+svb.getOwner().toString(Constants.TRUNCATED_IDENTITY_LENGTH));

						stateVoteBlocksToCollectRemovals.add(h);
						return;
					}

					if (stateVoteBlockCollector.hasVoted(svb.getOwner()) == false)
					{
						final long votePower = this.context.getLedger().getValidatorHandler().getVotePower(epoch, svb.getOwner().getIdentity());
						stateVoteBlockCollector.vote(svb, votePower);

						if (svb.getOwner().equals(this.context.getNode().getKeyPair().getPublicKey()))
						{
							if (this.context.getLedger().getLedgerStore().store(svb).equals(OperationStatus.SUCCESS) == false)
								statePoolLog.warn(StatePool.this.context.getName()+": Failed to store local state vote block "+svb.getHash());

							if (this.context.getNetwork().getGossipHandler().broadcast(svb.isHeader() ? svb : svb.getHeader(), localShardGroupID) == false)
								statePoolLog.warn(StatePool.this.context.getName()+": Failed to broadcast local state vote block "+svb.getHash());
						}

						this.context.getMetaData().increment("ledger.pool.state.votes", svb.size());
					}

					final Collection<StateVoteBlock> completedStateVoteBlocks = stateVoteBlockCollector.tryComplete();
					if (completedStateVoteBlocks != null)
					{
						if (statePoolLog.hasLevel(Logging.INFO))
							statePoolLog.info(this.context.getName()+": Completed state vote block collector "+stateVoteBlockCollector);
							
						completedStateVoteBlocks.forEach(csvb -> this.voteBlocksToProcessQueue.put(csvb.getHash(), csvb));
					}
						
					stateVoteBlocksToCollectRemovals.add(svb.getHash());
				}
				catch (Exception ex)
				{
					statePoolLog.error(this.context.getName()+": Maintenence of delayed state vote block failed for "+svb.toString()+" by "+svb.getOwner().toString(Constants.TRUNCATED_IDENTITY_LENGTH), ex);
					stateVoteBlocksToCollectRemovals.add(svb.getHash());
				}
			});
			this.voteBlocksToCollect.keySet().removeAll(stateVoteBlocksToCollectRemovals);
		}
	}
	
	private void _processStateVoteBlocks()
	{
		if (this.voteBlocksToProcessQueue.isEmpty() == false)
		{
			final StateVoteBlock stateVoteBlock = this.voteBlocksToProcessQueue.peek().getValue();
			if (statePoolLog.hasLevel(Logging.INFO))
				statePoolLog.info(this.context.getName()+": Processing state vote block "+stateVoteBlock.toString());
			
			// Can perform a combined verification?
			final List<StateVoteBlock> stateVoteBlocksToProcess = new ArrayList<StateVoteBlock>();
			this.voteBlocksToProcessQueue.forEach((h, svb) -> {
				if (svb.equals(stateVoteBlock)) return;
				
				if (svb.getID().equals(stateVoteBlock.getID()) == false) return;
				
				stateVoteBlocksToProcess.add(svb);
			});
			stateVoteBlocksToProcess.add(stateVoteBlock);
			
			if (statePoolLog.hasLevel(Logging.INFO))
				statePoolLog.info(this.context.getName()+": Discovered "+stateVoteBlocksToProcess.size()+" state vote blocks associated with "+stateVoteBlock.toString());
			
			final BLSPublicKey aggregatedPublicKey;
			final BLSSignature aggregatedSignature;
			if (BLS12381.SKIP_VERIFICATION == false)
			{
				aggregatedPublicKey = BLS12381.aggregatePublicKey(stateVoteBlocksToProcess.stream().map(svb -> svb.getOwner()).collect(Collectors.toList()));
				aggregatedSignature = BLS12381.aggregateSignatures(stateVoteBlocksToProcess.stream().map(svb -> svb.getSignature()).collect(Collectors.toList()));
			}
			else
			{
				aggregatedPublicKey = null; aggregatedSignature = null;
			}
			
			boolean abortAggregatedVerification = false;
			boolean individualVerificationRequired = false;
			try 
			{
				if (BLS12381.SKIP_VERIFICATION == false)
				{
					if (aggregatedPublicKey.verify(stateVoteBlock.getExecutionMerkle(), aggregatedSignature) == false)
						individualVerificationRequired = true;
				}
			} 
			catch (CryptoException cex) 
			{
				statePoolLog.error(this.context.getName()+": Error validating state vote block "+stateVoteBlock.toString(), cex);
				abortAggregatedVerification = true;
			}
			
			if (abortAggregatedVerification == false)
			{
				if (individualVerificationRequired)
				{
					statePoolLog.warn(this.context.getName()+": Verifying state vote block batch "+stateVoteBlock.toString()+" individually");

					// Do individually
					final Iterator<StateVoteBlock> stateVoteBlockIterator = stateVoteBlocksToProcess.iterator();
					while (stateVoteBlockIterator.hasNext())
					{
						final StateVoteBlock stateVoteBlockToProcess = stateVoteBlockIterator.next();
						try
						{
							if (stateVoteBlockToProcess.isHeader())
							{
								// TODO Fetch full state vote block
								statePoolLog.warn(StatePool.this.context.getName()+": Need non-header version of state vote block "+stateVoteBlockToProcess.toString());
								stateVoteBlockIterator.remove();
								continue;
							}
							
							context.getMetaData().increment("ledger.pool.state.vote.block.verifications");
	
							try
							{
								stateVoteBlockToProcess.validate();
							}
							catch (ValidationException vex)
							{
								statePoolLog.warn(this.context.getName()+": State vote block "+stateVoteBlockToProcess.toString()+" did not validate");
								stateVoteBlockIterator.remove();
								continue;
							}
							
							if (stateVoteBlockToProcess.verify() == false)
							{
								statePoolLog.warn(this.context.getName()+": State vote block "+stateVoteBlockToProcess.toString()+" did not verify");
								stateVoteBlockIterator.remove();
								continue;
							}
						}
						catch (Exception ex)
						{
							statePoolLog.error(this.context.getName()+": Error validating state vote block "+stateVoteBlockToProcess.toString(), ex);
						}						
					}
				}
				else
					context.getMetaData().increment("ledger.pool.state.vote.block.verifications");

				final StateVoteBlock referenceStateVoteBlock = stateVoteBlocksToProcess.stream().filter(svb -> svb.isHeader() == false).findFirst().orElse(null);
				if (statePoolLog.hasLevel(Logging.INFO))
					statePoolLog.info(StatePool.this.context.getName()+": Reference state vote block is "+referenceStateVoteBlock);
				
				if (referenceStateVoteBlock != null)
				{
					final Set<Hash> referencedPendingAtoms = Sets.mutable.ofInitialCapacity(128);
					for (final StateVoteBlock stateVoteBlockToProcess : stateVoteBlocksToProcess)
					{
						try
						{
							final Epoch epoch = Epoch.from(stateVoteBlockToProcess.getBlock());
							final long votePower = this.context.getLedger().getValidatorHandler().getVotePower(epoch, stateVoteBlockToProcess.getOwner().getIdentity());
							final List<StateVote> stateVotes = stateVoteBlockToProcess.isHeader() == false ? stateVoteBlockToProcess.getVotes() : stateVoteBlockToProcess.deriveVotes(referenceStateVoteBlock, votePower);
							for (int i = 0 ; i < stateVotes.size() ; i++)
							{
								final StateVote stateVote = stateVotes.get(i);
								try
								{
									final PendingAtom pendingAtom = this.context.getLedger().getAtomHandler().get(stateVote.getAtom());
									if (pendingAtom == null)
									{
										statePoolLog.warn(this.context.getName()+": Pending atom not found for state vote "+stateVote.toString());
										continue;
									}
									
									if (referencedPendingAtoms.add(pendingAtom.getHash()))
										pendingAtom.addStateVoteBlock(stateVoteBlockToProcess);
									
									StatePool.this.process(pendingAtom, stateVote);
	
									if (statePoolLog.hasLevel(Logging.DEBUG))
										statePoolLog.debug(this.context.getName()+": Processed state vote "+stateVote.toString());
								}
								catch (Exception ex)
								{
									statePoolLog.error(this.context.getName()+": Error processing state vote "+stateVote.toString(), ex);
								}
							}
						}
						catch (Exception ex)
						{
							statePoolLog.error(StatePool.this.context.getName()+": Error processing state vote block "+stateVoteBlockToProcess.toString(), ex);
						}
						
						this.context.getMetaData().increment("ledger.statevoteblock.processed");
						this.context.getMetaData().increment("ledger.statevoteblock.latency", stateVoteBlockToProcess.getAge(TimeUnit.MILLISECONDS));
						
						referencedPendingAtoms.clear();
						
						// TODO penalise for sending a state vote block that doesn't touch the local replica!
					}
					
					stateVoteBlocksToProcess.forEach(svb -> svb.clear());
				}
				else
					statePoolLog.fatal(this.context.getName()+": Reference state vote block not found for "+stateVoteBlock);

				for (StateVoteBlock svb : stateVoteBlocksToProcess)
				{
					if (this.voteBlocksToProcessQueue.remove(svb.getHash(), svb) == false && this.context.getNode().isSynced())
						statePoolLog.warn(this.context.getName()+": State vote block peek/remove failed for "+svb);
				}
			}

			if (this.voteBlocksToProcessQueue.isEmpty() == false)
				this.voteProcessor.signal();
		}
	}
	
	private void _castVotesAndCollect()
	{
		if (this.votesToCastQueue.isEmpty() == false)
		{
			boolean createdStateVoteBlocks = false;
			Entry<Hash, PendingState> pendingState;
			while((pendingState = this.votesToCastQueue.peek()) != null)
			{
				try
				{
					if (pendingState.getValue().getAtom().thrown() == null && pendingState.getValue().getAtom().getExecutionDigest() == null)
						throw new IllegalStateException("Can not vote on state "+pendingState.getValue().getAddress()+" in pending atom "+pendingState.getValue().getAtom().getHash()+" when no decision made");
					
					final StateVoteCollector stateVoteCollector = pendingState.getValue().getStateVoteCollector();
					if (stateVoteCollector == null)
					{
						statePoolLog.error(this.context.getName()+": State vote collector for "+pendingState.getValue().getAddress()+" in atom "+pendingState.getValue().getAtom()+" in block "+pendingState.getValue().getBlockHeader()+" not found");
						continue;
					}

					final Epoch epoch = Epoch.from(stateVoteCollector.getBlock());
					final long votePower = this.context.getLedger().getValidatorHandler().getVotePower(epoch, this.context.getNode().getIdentity());
					stateVoteCollector.vote(pendingState.getValue(), votePower);
					
					final StateVoteBlock stateVoteBlock = stateVoteCollector.tryComplete(this.context.getNode().getKeyPair());
					if (stateVoteBlock != null)
					{
						this.voteBlocksToCollect.put(stateVoteBlock.getHash(), stateVoteBlock);
						this.stateVoteCollectors.remove(stateVoteCollector.getBlock(), stateVoteCollector);
						createdStateVoteBlocks = true;
					}
				}
				catch (Exception ex)
				{
					statePoolLog.error(this.context.getName()+": Error casting vote for " + pendingState.getKey(), ex);
				}
				finally
				{
					if (this.votesToCastQueue.remove(pendingState.getKey(), pendingState.getValue()) == false && this.context.getNode().isSynced())
						statePoolLog.warn(this.context.getName()+": State pool vote cast peek/remove failed for "+pendingState.getKey());
				}
			}
			
			if (createdStateVoteBlocks)
				this.voteProcessor.signal();
		}
	}

	// METHODS //
	private void prepare(final PendingBlock pendingBlock) throws IOException
	{
		final Epoch epoch = Epoch.from(pendingBlock.getHeader());
		final int numShardGroups = this.context.getLedger().numShardGroups(epoch);
		final ShardGroupID localShardGroupID = ShardMapper.toShardGroup(this.context.getNode().getIdentity(), numShardGroups);
		final long votePower = this.context.getLedger().getValidatorHandler().getVotePower(epoch, this.context.getNode().getIdentity());
		final long voteThreshold = this.context.getLedger().getValidatorHandler().getVotePowerThreshold(epoch, localShardGroupID);

		// Collate pending states into buckets //
		final MutableBoolean spanningStateSet = new MutableBoolean(false);
		final List<PendingState> pendingStates = new ArrayList<PendingState>();
		final Multimap<Long, PendingState> pendingStateBuckets = LinkedHashMultimap.create();
		for (final Hash atom : pendingBlock.getHeader().getInventory(InventoryType.ACCEPTED))
		{
			final PendingAtom pendingAtom = pendingBlock.get(InventoryType.ACCEPTED, atom);
			if (pendingAtom == null)
				throw new IllegalStateException("Pending atom "+atom+" required for SVC creation not found in proposal "+pendingBlock.toString());
			
			if (pendingAtom.getStatus().before(AtomStatus.State.ACCEPTED))
				throw new IllegalStateException("Pending atom "+pendingAtom+" is not ACCEPTED for SVC creation in proposal "+pendingBlock.toString());

			pendingAtom.forStates(StateLockMode.WRITE, pendingState -> {
				final ShardGroupID provisionShardGroupID = ShardMapper.toShardGroup(pendingState.getAddress(), numShardGroups);
				if (provisionShardGroupID.equals(localShardGroupID) == false)
				{
					spanningStateSet.setTrue();
					return;
				}
				
				pendingStates.add(pendingState);
			});
			
			if (Constants.STATE_VOTE_UNGROUPED || spanningStateSet.isFalse())
				pendingStateBuckets.putAll(0l, pendingStates);
			else
				pendingStateBuckets.putAll(1l, pendingStates);
			
			pendingStates.clear();
			spanningStateSet.setFalse();
		}

		// Create state vote collectors //
		for (final long bucketID : pendingStateBuckets.keySet())
		{
			final Collection<PendingState> pendingStateBucket = pendingStateBuckets.get(bucketID);
			if (pendingStateBucket.isEmpty() == false)
			{
				if (statePoolLog.hasLevel(Logging.DEBUG))
					statePoolLog.debug(this.context.getName()+": Creating StateVoteCollector for proposal "+pendingBlock.toString()+" with state keys "+pendingStateBucket.stream().map(sk -> sk.getAddress()).collect(Collectors.toList()));
				else if (statePoolLog.hasLevel(Logging.INFO))
					statePoolLog.info(this.context.getName()+": Creating StateVoteCollector for proposal "+pendingBlock.toString()+" with "+pendingStateBucket.size()+" state keys");

				final StateVoteCollector stateVoteCollector = new StateVoteCollector(this.context, pendingBlock.getHash(), pendingStateBucket, votePower, voteThreshold);
				for (final PendingState pendingState : pendingStateBucket)
					pendingState.setStateVoteCollector(stateVoteCollector);
				this.stateVoteCollectors.put(pendingBlock.getHash(), stateVoteCollector);
				
				this.context.getMetaData().increment("ledger.pool.state.vote.collectors.total");
			}
		}
	}
	
	private void vote(final PendingAtom pendingAtom)
	{
		Objects.requireNonNull(pendingAtom, "Atom is null");
		
		if (pendingAtom.getStatus().current().equals(AtomStatus.State.FINALIZING) == false)
			throw new IllegalStateException("Pending atom "+pendingAtom.getHash()+" is not FINALIZING");
		
		if (statePoolLog.hasLevel(Logging.DEBUG))
			statePoolLog.debug(this.context.getName()+": Voting on pending atom "+pendingAtom.getHash());
		
		final Epoch epoch = Epoch.from(pendingAtom.getBlockHeader());
		final int numShardGroups = this.context.getLedger().numShardGroups(epoch);
		final ShardGroupID localShardGroupID = ShardMapper.toShardGroup(this.context.getNode().getIdentity(), numShardGroups);
		for (final PendingState pendingState : pendingAtom.getStates(StateLockMode.WRITE))
		{
			final ShardGroupID stateShardGroupID = ShardMapper.toShardGroup(pendingState.getAddress(), numShardGroups);
			if (stateShardGroupID.equals(localShardGroupID) == false)
				continue;
			
			final StateVoteCollector stateVoteCollector = pendingState.getStateVoteCollector();
			if (stateVoteCollector == null)
				statePoolLog.error(this.context.getName()+": SVC not found for voting on state "+pendingState.getAddress()+" in pending atom "+pendingAtom.getHash());
			else if (stateVoteCollector.hasVoted(pendingState) == false)
				this.votesToCastQueue.put(pendingState.getHash(), pendingState);
			else if (statePoolLog.hasLevel(Logging.DEBUG))
				statePoolLog.debug(this.context.getName()+": State vote collector already has vote for pending state "+pendingState);
		}

		this.voteProcessor.signal();
	}
	
	private void processExecuteLatent(final PendingAtom pendingAtom) throws IOException, CryptoException
	{
		if (pendingAtom.isExecuteSignalled() || pendingAtom.getStatus().after(AtomStatus.State.PROVISIONED))
			throw new IllegalStateException("Pending atom "+pendingAtom.getHash()+" is signalled executable");

		synchronized(this.stateVoteCollectors)
		{
			List<StateVoteCollector> latentStateVoteCollectors = new ArrayList<StateVoteCollector>();
			
			final Epoch epoch = Epoch.from(pendingAtom.getBlockHeader());
			final int numShardGroups = this.context.getLedger().numShardGroups(epoch);
			final ShardGroupID localShardGroupID = ShardMapper.toShardGroup(this.context.getNode().getIdentity(), numShardGroups);
			for (PendingState pendingState : pendingAtom.getStates(StateLockMode.WRITE))
			{
				ShardGroupID stateShardGroupID = ShardMapper.toShardGroup(pendingState.getAddress(), numShardGroups);
				if (stateShardGroupID.equals(localShardGroupID) == false)
					continue;
				
				if (pendingState.getStateOutput() != null)
					throw new IllegalStateException("Latent state "+pendingState+" has state output");

				StateVoteCollector stateVoteCollector = pendingState.getStateVoteCollector();
				if (stateVoteCollector == null)
					throw new IllegalStateException("SVC not found for latent state "+pendingState.getAddress()+" in pending atom "+pendingAtom.getHash());
				
				if (stateVoteCollector.isLatent())
					throw new IllegalStateException("SVC "+stateVoteCollector+" already represents latent state "+pendingState);
				
				if (stateVoteCollector.isCompleted())
					throw new IllegalStateException("SVC "+stateVoteCollector+" is already completed containing latent state "+pendingState);
				
				if (stateVoteCollector.remove(pendingState) == false)
					throw new IllegalStateException("Latent state "+pendingState+" not removed from SVC "+stateVoteCollector);
					
				long votePower = this.context.getLedger().getValidatorHandler().getVotePower(epoch, this.context.getNode().getIdentity());
				long voteThreshold = this.context.getLedger().getValidatorHandler().getVotePowerThreshold(epoch, localShardGroupID);
				StateVoteCollector latentStateVoteCollector = new StateVoteCollector(this.context, pendingState.getBlockHeader().getHash(), pendingState, votePower, voteThreshold);
				latentStateVoteCollectors.add(latentStateVoteCollector);
				
				// Original SVC might just have contained a single item
				if (stateVoteCollector.isEmpty() == false)
				{
					final StateVoteBlock stateVoteBlock = stateVoteCollector.tryComplete(this.context.getNode().getKeyPair());
					if (stateVoteBlock != null)
					{
						this.stateVoteCollectors.remove(stateVoteCollector.getBlock(), stateVoteCollector);
						this.voteBlocksToCollect.put(stateVoteBlock.getHash(), stateVoteBlock);
					}
				}
				else
					this.stateVoteCollectors.remove(stateVoteCollector.getBlock(), stateVoteCollector);
			}
			
			latentStateVoteCollectors.forEach(lsvc -> {
				if (StatePool.this.stateVoteCollectors.containsEntry(lsvc.getBlock(), lsvc))
					statePoolLog.error(StatePool.this.context.getName()+": Already have state vote collector for block "+lsvc.getBlock()+" for states "+lsvc.getStates().toString());
				else
				{
					StatePool.this.stateVoteCollectors.put(lsvc.getBlock(), lsvc);
					
					for (PendingState pendingState : lsvc.getStates())
						pendingState.setStateVoteCollector(lsvc);
				}
			});

			this.context.getMetaData().increment("ledger.pool.state.vote.collectors.total", latentStateVoteCollectors.size());
		}
	}
	
	private void processTimeout(final PendingAtom pendingAtom, final AtomTimeout timeout) throws IOException, CryptoException
	{
		if (pendingAtom.getStatus().was(AtomStatus.State.ACCEPTED) == false)
			throw new IllegalStateException("Pending atom "+pendingAtom.getHash()+" is not ACCEPTED");
			
		final Epoch epoch = Epoch.from(pendingAtom.getBlockHeader());
		final int numShardGroups = this.context.getLedger().numShardGroups(epoch);
		final ShardGroupID localShardGroupID = ShardMapper.toShardGroup(StatePool.this.context.getNode().getIdentity(), numShardGroups);

		synchronized(StatePool.this.stateVoteCollectors)
		{
			for (PendingState pendingState : pendingAtom.getStates(StateLockMode.WRITE))
			{
				ShardGroupID stateShardGroupID = ShardMapper.toShardGroup(pendingState.getAddress(), numShardGroups);
				if (stateShardGroupID.equals(localShardGroupID) == false)
					continue;
				
				// State certificate is present for pending state then SVC must already be completed
				if (pendingAtom.getOutput(pendingState.getAddress()) != null)
					continue;

				StateVoteCollector stateVoteCollector = pendingState.getStateVoteCollector();
				if (stateVoteCollector == null)
				{
					statePoolLog.error(this.context.getName()+": SVC not found for timeout "+timeout+" of atom "+pendingAtom.getHash()+" with state "+pendingState.getAddress()+" in block "+pendingState.getBlockHeader());
					continue;
				}

				if (stateVoteCollector.isCompletable() == false)
				{
					stateVoteCollector.remove(pendingState);
					
					if (stateVoteCollector.isEmpty() == false)
					{
						final StateVoteBlock stateVoteBlock = stateVoteCollector.tryComplete(this.context.getNode().getKeyPair());
						if (stateVoteBlock != null)
						{
							statePoolLog.warn(this.context.getName()+": Completed state vote collector via AtomTimeout "+stateVoteCollector.toString());
							this.stateVoteCollectors.remove(stateVoteCollector.getBlock(), stateVoteCollector);
							this.voteBlocksToCollect.put(stateVoteBlock.getHash(), stateVoteBlock);
						}
					}
					else
						this.stateVoteCollectors.remove(stateVoteCollector.getBlock(), stateVoteCollector);
				}
			}
		}
	}
	
	private void remove(final Collection<PendingAtom> pendingAtoms)
	{
		final List<Hash> removals = new ArrayList<Hash>(pendingAtoms.size());
		final Map<StateAddress, PendingState> states = Maps.mutable.ofInitialCapacity(pendingAtoms.size());
		for (PendingAtom pendingAtom : pendingAtoms)
		{
			if (pendingAtom.getStatus().was(AtomStatus.State.ACCEPTED) == false)
				continue;
			
			final Epoch epoch = Epoch.from(pendingAtom.getBlockHeader());
			final int numShardGroups = this.context.getLedger().numShardGroups(epoch);
			final ShardGroupID localShardGroupID = ShardMapper.toShardGroup(StatePool.this.context.getNode().getIdentity(), numShardGroups);

			pendingAtom.forStates(StateLockMode.WRITE, pendingState -> {
				ShardGroupID stateShardGroupID = ShardMapper.toShardGroup(pendingState.getAddress(), numShardGroups);
				if (stateShardGroupID.equals(localShardGroupID) == false)
					return;
				
				if (states.putIfAbsent(pendingState.getAddress(), pendingState) == null)
				{
					removals.add(pendingState.getHash());

					if (statePoolLog.hasLevel(Logging.DEBUG))
						statePoolLog.debug(StatePool.this.context.getName()+": Removing state "+pendingState+" for atom "+pendingAtom.getHash()+" in block "+pendingAtom.getBlockHeader().getHash());
				}
			});
		}
		
		StatePool.this.votesToCastQueue.removeAll(removals);
	}
	
	private void process(final PendingAtom pendingAtom, final StateVote stateVote) throws IOException, CryptoException, ValidationException
	{
		Objects.requireNonNull(pendingAtom, "Pending atom is null");
		Objects.requireNonNull(stateVote, "State vote is null");

		final Epoch epoch = Epoch.from(pendingAtom.getBlockHeader());
		final long votePower = StatePool.this.context.getLedger().getValidatorHandler().getVotePower(epoch, stateVote.getOwner().getIdentity());

		// IMPORTANT Even if vote power is zero, local instance needs to vote to build the execution merkles used
		// 			 to batch validate state vote blocks 
		final PendingState pendingState = pendingAtom.vote(stateVote, votePower);

		if (pendingAtom.isForceCommitTimeout())
			return;
		
		tryFinalize(pendingState);
	}
	
	private boolean tryFinalize(final PendingState pendingState) throws IOException, CryptoException
	{
		// Don't build certificates from cast votes received until executed locally
		if (pendingState.getAtom().getStatus().before(AtomStatus.State.FINALIZING))
			throw new IllegalStateException("Pending atom "+pendingState.getAtom()+" containing state "+pendingState+" is not EXECUTED");

		if (pendingState.getAtom().getCertificate() != null)
			return false;
		
		if (pendingState.isFinalized() == true)
			return false;
		
		final StateCertificate stateCertificate = pendingState.tryFinalize();
		if (stateCertificate != null)
		{
			if (statePoolLog.hasLevel(Logging.INFO))
				statePoolLog.info(this.context.getName()+": State certificate "+stateCertificate.getHash()+" constructed for "+stateCertificate.getObject()+":"+stateCertificate.getAtom());

			StatePool.this.context.getEvents().post(new StateCertificateConstructedEvent(stateCertificate, pendingState));
			return true;
		}
		
		return false;
	}
	
	// ASYNC PROGRESS LISTENER //
	private EventListener asyncProgressListener = new EventListener() 
	{
		@Subscribe
		public void on(final ProgressPhaseEvent event)
		{
			StatePool.this.voteProcessor.signal();
		}
	};

	// SYNCHRONOUS ATOM LISTENER //
	private SynchronousEventListener syncAtomListener = new SynchronousEventListener()
	{
		@Subscribe
		public void on(final AtomExecutedEvent event) 
		{
			vote(event.getPendingAtom());
		}

		@Subscribe
		public void on(final AtomExecutionTimeoutEvent event) throws IOException, CryptoException
		{
			processTimeout(event.getPendingAtom(), event.getPendingAtom().getTimeout(ExecutionTimeout.class));
		}

		@Subscribe
		public void on(final AtomExceptionEvent event) 
		{
			// TODO
		}

		@Subscribe
		public void on(final AtomExecuteLatentEvent event) throws IOException, CryptoException
		{
			try
			{
				processExecuteLatent(event.getPendingAtom());
			}
			catch (Exception ex)
			{
				statePoolLog.error(StatePool.this.context.getName()+": "+ex.getMessage());
				statePoolLog.error("        Execute latent signalled at: "+event.getPendingAtom().getExecuteLatentSignalledBlock());
				statePoolLog.error("               Execute signalled at: "+event.getPendingAtom().getExecuteSignalledBlock());
				
				throw ex;
			}
		}
	};

	// ASYNC BLOCK LISTENER //
	private EventListener asyncBlockListener = new EventListener()
	{
		@Subscribe
		public void on(final BlockCommittedEvent blockCommittedEvent) 
		{
			try
			{
				synchronized(StatePool.this.voteBlocksToCollect) 
				{
					Iterator<StateVoteBlock> stateVoteBlockIterator = StatePool.this.voteBlocksToCollect.values().iterator();
					while(stateVoteBlockIterator.hasNext())
					{
						StateVoteBlock stateVoteBlock = stateVoteBlockIterator.next();

						if (stateVoteBlock.isStale())
						{
							stateVoteBlockIterator.remove();
							
							if (stateVoteBlock.isHeader())
								statePoolLog.warn(StatePool.this.context.getName()+": Removed stale state vote block header "+stateVoteBlock+" by "+stateVoteBlock.getOwner().toString(Constants.TRUNCATED_IDENTITY_LENGTH));
							else
								statePoolLog.warn(StatePool.this.context.getName()+": Removed stale state vote block "+stateVoteBlock+" by "+stateVoteBlock.getOwner().toString(Constants.TRUNCATED_IDENTITY_LENGTH));
						}
					}
				}

				synchronized(StatePool.this.stateVoteBlockCollectors)
				{
					Iterator<StateVoteBlockCollector> stateVoteBlockCollectorsIterator = StatePool.this.stateVoteBlockCollectors.values().iterator();
					while(stateVoteBlockCollectorsIterator.hasNext())
					{
						final StateVoteBlockCollector stateVoteBlockCollector = stateVoteBlockCollectorsIterator.next();
						if (stateVoteBlockCollector.isStale())
						{
							stateVoteBlockCollectorsIterator.remove();
							
							if (stateVoteBlockCollector.isCompleted() == false)
								statePoolLog.warn(StatePool.this.context.getName()+": Removed incomplete stale state vote block collector "+stateVoteBlockCollector.toString());
						}
					}
					
					synchronized(StatePool.this.stateVoteCollectors)
					{
						final Iterator<StateVoteCollector> stateVoteCollectorIterator = StatePool.this.stateVoteCollectors.values().iterator();
						while(stateVoteCollectorIterator.hasNext())
						{
							final StateVoteCollector stateVoteCollector = stateVoteCollectorIterator.next();
							if (stateVoteCollector.isCompleted())
							{
								stateVoteCollectorIterator.remove();
								statePoolLog.warn(StatePool.this.context.getName()+": Removed completed state vote collector "+stateVoteCollector.toString());
								continue;
							}

							if (stateVoteCollector.isStale())
							{
								stateVoteCollectorIterator.remove();
								statePoolLog.warn(StatePool.this.context.getName()+": Removed incomplete stale state vote collector "+stateVoteCollector.toString());
								stateVoteCollector.getStates().forEach(ps -> statePoolLog.warn(StatePool.this.context.getName()+":        "+ps.toString()));
								continue;
							}
						}
						
						StatePool.this.context.getMetaData().put("ledger.pool.state.vote.collectors", StatePool.this.stateVoteCollectors.size());
					}
				}
			}
			catch (Exception ex)
			{
				statePoolLog.error(StatePool.this.context.getName()+": Error performing state vote collector housekeeping", ex);
			}
		}
	};

	// SYNC BLOCK LISTENER //
	private SynchronousEventListener syncBlockListener = new SynchronousEventListener()
	{
		@Subscribe
		public void on(final BlockCommittedEvent blockCommittedEvent) throws IOException 
		{
			remove(blockCommittedEvent.getPendingBlock().get(InventoryType.UNACCEPTED));
			remove(blockCommittedEvent.getPendingBlock().get(InventoryType.COMMITTED));
			remove(blockCommittedEvent.getPendingBlock().get(InventoryType.UNEXECUTED));
			remove(blockCommittedEvent.getPendingBlock().get(InventoryType.UNCOMMITTED));
			
			prepare(blockCommittedEvent.getPendingBlock());
		}
	};
	
	// SYNC CHANGE LISTENER //
	private SynchronousEventListener syncChangeListener = new SynchronousEventListener()
	{
		@Subscribe
		public void on(final SyncAcquiredEvent event) throws IOException, CryptoException // TODO handle these
		{
			statePoolLog.log(StatePool.this.context.getName()+": Sync status acquired, preparing state pool");
			StatePool.this.votesToCastQueue.clear();
			StatePool.this.voteBlocksToCollect.clear();
			StatePool.this.voteBlocksToProcessQueue.clear();
			StatePool.this.stateVoteCollectors.clear();
			StatePool.this.stateVoteBlockCollectors.clear();

			synchronized(StatePool.this.stateVoteCollectors)
			{
				for (final StateVoteCollector stateVoteCollector : event.getStateVoteCollectors())
				{
					final StateVoteBlock stateVoteBlock = stateVoteCollector.tryComplete(StatePool.this.context.getNode().getKeyPair());
					if (stateVoteBlock != null)
						StatePool.this.voteBlocksToCollect.put(stateVoteBlock.getHash(), stateVoteBlock);
					else
					{
						if (statePoolLog.hasLevel(Logging.INFO))
							statePoolLog.info(StatePool.this.context.getName()+": Pushed incomplete state vote collector "+stateVoteCollector.toString());

						StatePool.this.stateVoteCollectors.put(stateVoteCollector.getBlock(), stateVoteCollector);
					}
				}
			}
		}

		@Subscribe
		public void on(final SyncLostEvent event) 
		{
			statePoolLog.log(StatePool.this.context.getName()+": Sync status lost, flushing state pool");
			StatePool.this.votesToCastQueue.clear();
			StatePool.this.voteBlocksToCollect.clear();
			StatePool.this.voteBlocksToProcessQueue.clear();
			StatePool.this.stateVoteCollectors.clear();
			StatePool.this.stateVoteBlockCollectors.clear();
		}
	};
}
