package org.radix.hyperscale.ledger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.eclipse.collections.api.factory.Maps;
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.impl.factory.Sets;
import org.eclipse.collections.impl.factory.primitive.LongObjectMaps;
import org.radix.hyperscale.Configuration;
import org.radix.hyperscale.Constants;
import org.radix.hyperscale.Context;
import org.radix.hyperscale.Service;
import org.radix.hyperscale.common.Primitive;
import org.radix.hyperscale.concurrency.MonitoredReadWriteLock;
import org.radix.hyperscale.concurrency.MonitoredReentrantLock;
import org.radix.hyperscale.crypto.CryptoException;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.crypto.Identity;
import org.radix.hyperscale.events.EventListener;
import org.radix.hyperscale.events.SyncLostEvent;
import org.radix.hyperscale.events.SynchronousEventListener;
import org.radix.hyperscale.exceptions.StartupException;
import org.radix.hyperscale.exceptions.TerminationException;
import org.radix.hyperscale.exceptions.ValidationException;
import org.radix.hyperscale.executors.LatchedProcessor;
import org.radix.hyperscale.executors.PollingProcessor;
import org.radix.hyperscale.ledger.BlockHeader.InventoryType;
import org.radix.hyperscale.ledger.PendingBlock.SUPR;
import org.radix.hyperscale.ledger.PendingBranch.Type;
import org.radix.hyperscale.ledger.Substate.NativeField;
import org.radix.hyperscale.ledger.events.BlockAppliedEvent;
import org.radix.hyperscale.ledger.events.BlockCommitEvent;
import org.radix.hyperscale.ledger.events.BlockCommittedEvent;
import org.radix.hyperscale.ledger.events.BlockConstructedEvent;
import org.radix.hyperscale.ledger.events.ProgressPhaseEvent;
import org.radix.hyperscale.ledger.events.SyncAcquiredEvent;
import org.radix.hyperscale.ledger.exceptions.LockException;
import org.radix.hyperscale.ledger.messages.SyncAcquiredMessage;
import org.radix.hyperscale.ledger.messages.SyncAcquiredMessageProcessor;
import org.radix.hyperscale.ledger.primitives.Atom;
import org.radix.hyperscale.ledger.primitives.AtomCertificate;
import org.radix.hyperscale.ledger.sme.PolyglotPackage;
import org.radix.hyperscale.ledger.timeouts.AcceptTimeout;
import org.radix.hyperscale.ledger.timeouts.CommitTimeout;
import org.radix.hyperscale.ledger.timeouts.ExecutionLatentTimeout;
import org.radix.hyperscale.ledger.timeouts.ExecutionTimeout;
import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;
import org.radix.hyperscale.network.AbstractConnection;
import org.radix.hyperscale.network.GossipFetcher;
import org.radix.hyperscale.network.GossipFilter;
import org.radix.hyperscale.network.GossipInventory;
import org.radix.hyperscale.network.GossipReceiver;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.eventbus.Subscribe;
import com.google.common.primitives.Longs;
import com.sleepycat.je.OperationStatus;

public class BlockHandler implements Service
{
	private static final Logger syncLog = Logging.getLogger("sync");
	private static final Logger blocksLog = Logging.getLogger("blocks");
	
	static
	{
		blocksLog.setLevel(Logging.INFO);
	}
	
	private enum BlockVoteStatus
	{
		SUCCESS, FAILED, SKIPPED, POSTPONED, STALE;
	}

	enum BlockInsertStatus
	{
		SUCCESS, FAILED, SKIPPED, POSTPONED, STALE;
	}
	
	private LatchedProcessor blockProcessor = new LatchedProcessor(Ledger.definitions().roundInterval() / ProgressRound.State.values().length, TimeUnit.MILLISECONDS)
	{
		@Override
		public void process()
		{
			if (BlockHandler.this.context.getNode().isSynced() == false)
				return;

			BlockHandler.this.syncLock.readLock().lock();
			try
			{
				final BlockHeader head = BlockHandler.this.context.getLedger().getHead();
				final QuorumCertificate progressView = BlockHandler.this.progressView;
				final ProgressRound progressRound = getProgressRound(BlockHandler.this.progressClock.get(), false);
				
				_processHeaders(progressRound);
				_processVotes(progressRound, head);

				_updateBlocks(head);
				_updateBranches(head);
				
				_livenessTick(progressRound, progressView, head);
				
				// Set the delay based on the latent timestamp of the current phase
				if (progressRound.phaseLatentAt() > 0)
				{
					long ephemeralDelay = progressRound.phaseLatentAt() - System.currentTimeMillis();
					if (ephemeralDelay > 0)
						setEphemeralDelay(ephemeralDelay, TimeUnit.MILLISECONDS);
				}
			}
			finally
			{
				BlockHandler.this.syncLock.readLock().unlock();
			}
		}

		@Override
		public void onError(Throwable thrown) 
		{
			blocksLog.fatal(BlockHandler.this.context.getName()+": Error processing blocks", thrown);
		}
		
		@Override
		public void onTerminated()
		{
			blocksLog.log(BlockHandler.this.context.getName()+": Block processor is terminated");
		}
	};

	// 1024 queue size should be sufficient to hold enough progress events even at fast proposal times as a backlog before an OOS is triggered
	private BlockingQueue<ProgressPhaseEvent> progressPhaseQueue = new ArrayBlockingQueue<ProgressPhaseEvent>(1024);
	private PollingProcessor buildAndCommitProcessor = new PollingProcessor()
	{
		@Override
		public void process() throws InterruptedException
		{
			final ProgressPhaseEvent progressRoundEvent = BlockHandler.this.progressPhaseQueue.poll(Ledger.definitions().roundInterval() / ProgressRound.State.values().length, TimeUnit.MILLISECONDS);
			if (BlockHandler.this.context.getNode().isSynced() == false)
				return;
			
			BlockHandler.this.syncLock.readLock().lock();
			try
			{
				final ProgressRound progressRound;
				final ProgressRound.State progressRoundPhase;
				if (progressRoundEvent != null)
				{
					progressRound = progressRoundEvent.getProgressRound();
					progressRoundPhase = progressRoundEvent.getProgressPhase();
					
					// TODO local/secondary proposals on timeout
					if (progressRoundPhase.equals(ProgressRound.State.PROPOSING))
						prebuild(progressRound, ProgressRound.State.PROPOSING, BlockHandler.this.progressView);
					else if (progressRoundPhase.equals(ProgressRound.State.TRANSITION))
					{
						// Trigger secondaries proposal build
						if (progressRound.getProposers().isEmpty())
							prebuild(progressRound, ProgressRound.State.TRANSITION, BlockHandler.this.progressView);
					}
				}
				else
				{
					// No progress events, possibly waiting in a TRANSITION phase which will never time out.
					// They can halt until at least one constructed proposal is available in the current round
					// but proposal n+i may have a dependency on something in n so can not be constructed.  
					//
					// If n is not yet committed (it was latent) we still need to try to commit something
					// even if there are no progress events using the last completed progress round as a reference.
					progressRound = BlockHandler.this.progressRounds.get(BlockHandler.this.progressClock.get()-1);
					if (progressRound == null)
					{
						// No previous progress round, likely just come out of sync, or there is a serious problem
						// and local replica will simply go out of sync at some point in the future.
						blocksLog.warn("Previous progress round "+(BlockHandler.this.progressClock.get()-1)+" not found for mandatory commit attempt");
						return;
					}

					progressRoundPhase = progressRound.getState();
				}
				
				_decideCommit(progressRound, progressRoundPhase);
			}
			finally
			{
				BlockHandler.this.syncLock.readLock().unlock();
			}
		}

		@Override
		public void onError(Throwable thrown) 
		{
			blocksLog.fatal(BlockHandler.this.context.getName()+": Error processing progress rounds", thrown);
		}
		
		@Override
		public void onTerminated()
		{
			blocksLog.log(BlockHandler.this.context.getName()+": Block builder and committer is terminated");
		}
	};

	private final Context context;
	private final BlockBuilder blockBuilder;
	
	private final Map<Hash, PendingBlock>	pendingBlocks;
	private final Set<PendingBranch>		pendingBranches;

	private final Map<Hash, BlockVote> 		votesToVerify;
	private final Map<Hash, BlockVote> 		postponedVotes;
	private final Map<Hash, BlockHeader> 	headersToVerify;
	private final Map<Long, BlockVoteCollector> blockVoteCollectors;

	private volatile boolean buildLock;
	private final AtomicLong buildClock;
	private final AtomicLong progressClock;
	private volatile QuorumCertificate progressView;
	private final MutableLongObjectMap<ProgressRound> progressRounds;

	// TODO temporary fix to ensure that validation of new proposals, and the commitment of existing ones don't cause deadlocks between each other
	private final MonitoredReentrantLock guardLock;
	private final ReentrantReadWriteLock syncLock;

	BlockHandler(final Context context)
	{
		this.context = Objects.requireNonNull(context, "Context is null");
		this.blockBuilder = new BlockBuilder(context);
		
		this.pendingBlocks = Collections.synchronizedMap(new HashMap<Hash, PendingBlock>());
		this.pendingBranches = Sets.mutable.<PendingBranch>withInitialCapacity(32).asSynchronized();
		this.headersToVerify = Collections.synchronizedMap(new HashMap<Hash, BlockHeader>());
		this.votesToVerify = Collections.synchronizedMap(new HashMap<Hash, BlockVote>());
		this.postponedVotes = Collections.synchronizedMap(new HashMap<Hash, BlockVote>());
		this.blockVoteCollectors = Collections.synchronizedMap(new HashMap<Long, BlockVoteCollector>());

		this.buildLock = true;
		this.buildClock = new AtomicLong(-1);
		this.progressClock = new AtomicLong(0);
		this.progressRounds = LongObjectMaps.mutable.<ProgressRound>empty().asSynchronized();
		
		this.guardLock = new MonitoredReentrantLock(this.context.getName()+" Block Handler Guard Lock");
		this.syncLock = new MonitoredReadWriteLock(this.context.getName()+" Block Handler Sync Lock", true);
	}

	@Override
	public void start() throws StartupException
	{
		// BLOCK HEADER GOSSIP //
		this.context.getNetwork().getGossipHandler().register(BlockHeader.class, new GossipFilter<BlockHeader>(this.context) 
		{
			@Override
			public Set<ShardGroupID> filter(final BlockHeader blockHeader)
			{
				final Epoch epoch = Epoch.from(blockHeader);
				final int numShardGroups = BlockHandler.this.context.getLedger().numShardGroups(epoch);
				final ShardGroupID blockShardGroupID = ShardMapper.toShardGroup(blockHeader.getProposer(), numShardGroups);
				final ShardGroupID localShardGroupID = ShardMapper.toShardGroup(BlockHandler.this.context.getNode().getIdentity(), numShardGroups);
				if (blockShardGroupID.equals(localShardGroupID) == false)
				{
					blocksLog.warn(BlockHandler.this.context.getName()+": Block header is for shard group ID "+blockShardGroupID+" but expected local shard group ID "+localShardGroupID);
					// TODO disconnect and ban;
					return Sets.immutable.<ShardGroupID>empty().castToSet();
				}
				
				return Sets.immutable.of(localShardGroupID).castToSet();
			}
		});

		this.context.getNetwork().getGossipHandler().register(BlockHeader.class, new GossipInventory() 
		{
			@Override
			public Collection<Hash> required(final Class<? extends Primitive> type, final Collection<Hash> items, final AbstractConnection connection) throws Throwable
			{
				if (type.equals(BlockHeader.class) == false)
				{
					blocksLog.error(BlockHandler.this.context.getName()+": Block header type expected but got "+type);
					return Collections.emptyList();
				}
				
				if (BlockHandler.this.context.getNode().isSynced() == false)
					return Collections.emptyList();
				
				List<Hash> required = new ArrayList<Hash>(items.size());
				for (Hash item : items)
				{
					if (BlockHandler.this.pendingBlocks.containsKey(item) == false &&
						BlockHandler.this.headersToVerify.containsKey(item) == false)
						required.add(item);
				}
				required.removeAll(BlockHandler.this.context.getLedger().getLedgerStore().has(required, type));

				return required;
			}
		});

		this.context.getNetwork().getGossipHandler().register(BlockHeader.class, new GossipReceiver<BlockHeader>() 
		{
			@Override
			public void receive(final Collection<BlockHeader> headers, final AbstractConnection connection) throws IOException
			{
				if (BlockHandler.this.context.getNode().isSynced() == false)
					return;
				
				for (final BlockHeader header : headers)
				{
					if (blocksLog.hasLevel(Logging.INFO))
						blocksLog.info(BlockHandler.this.context.getName()+": Received block header "+header+" for "+header.getProposer());
	
					final Epoch epoch = Epoch.from(header);
					final int numShardGroups = BlockHandler.this.context.getLedger().numShardGroups(epoch);
					final ShardGroupID localShardGroupID = ShardMapper.toShardGroup(BlockHandler.this.context.getNode().getIdentity(), numShardGroups); 
					final ShardGroupID blockVoteShardGroupID = ShardMapper.toShardGroup(header.getProposer(), numShardGroups);
					if (localShardGroupID.equals(blockVoteShardGroupID) == false)
					{
						blocksLog.warn(BlockHandler.this.context.getName()+": Block header "+header.getHash()+" for "+header.getProposer()+" is for shard group ID "+blockVoteShardGroupID +" but expected local shard group ID "+localShardGroupID);
						// TODO disconnect and ban;
						continue;
					}
					
					if (BlockHandler.this.context.getLedger().getLedgerStore().store(header).equals(OperationStatus.SUCCESS))
					{
						BlockHandler.this.headersToVerify.put(header.getHash(), header);
						BlockHandler.this.blockProcessor.signal();
						
						if (BlockHandler.this.context.getNetwork().getGossipHandler().broadcast(header, localShardGroupID) == false)
							blocksLog.warn(BlockHandler.this.context.getName()+": Failed to broadcast block header "+header);
					}
					else
						blocksLog.warn(BlockHandler.this.context.getName()+": Failed to store block header "+header);
				}
			}
		});
		
		this.context.getNetwork().getGossipHandler().register(BlockHeader.class, new GossipFetcher<BlockHeader>() 
		{
			@Override
			public Collection<BlockHeader> fetch(final Collection<Hash> items, final AbstractConnection connection) throws IOException
			{
				final List<Hash> toFetch = new ArrayList<Hash>(items);
				final List<BlockHeader> fetched = new ArrayList<BlockHeader>(items.size());

				final Iterator<Hash> toFetchIterator = toFetch.iterator();
				while(toFetchIterator.hasNext())
				{
					final Hash item = toFetchIterator.next();
					
					BlockHeader blockHeader = BlockHandler.this.headersToVerify.get(item);
					if (blockHeader == null)
					{
						final PendingBlock pendingBlock = BlockHandler.this.pendingBlocks.get(item);
						if (pendingBlock == null || pendingBlock.getHeader() == null)
							continue;
						
						blockHeader = pendingBlock.getHeader();
					}

					fetched.add(blockHeader);
					toFetchIterator.remove();
				}
				
				BlockHandler.this.context.getLedger().getLedgerStore().get(toFetch, BlockHeader.class, (h, p) -> { fetched.add(p); toFetch.remove(h); });

				if (toFetch.isEmpty() == false)
					toFetch.forEach(h -> blocksLog.error(BlockHandler.this.context.getName()+": Requested block header "+h+" not found"));
				
				return fetched;
			}
		});
		
		// BLOCK VOTE GOSSIP //
		this.context.getNetwork().getGossipHandler().register(BlockVote.class, new GossipFilter<BlockVote>(this.context) 
		{
			@Override
			public Set<ShardGroupID> filter(final BlockVote blockVote)
			{
				final Epoch epoch = Epoch.from(blockVote.getBlock());
				final int numShardGroups = BlockHandler.this.context.getLedger().numShardGroups(epoch);
				final ShardGroupID blockShardGroupID = ShardMapper.toShardGroup(blockVote.getOwner().getIdentity(), numShardGroups);
				final ShardGroupID localShardGroupID = ShardMapper.toShardGroup(BlockHandler.this.context.getNode().getIdentity(), numShardGroups);
				if (blockShardGroupID.equals(localShardGroupID) == false)
				{
					blocksLog.warn(BlockHandler.this.context.getName()+": Block vote is for shard group ID "+blockShardGroupID+" but expected local shard group ID "+localShardGroupID);
					// TODO disconnect and ban;
					return Sets.immutable.<ShardGroupID>empty().castToSet();
				}
				
				return Sets.immutable.of(localShardGroupID).castToSet();
			}
		});

		this.context.getNetwork().getGossipHandler().register(BlockVote.class, new GossipInventory() 
		{
			@Override
			public Collection<Hash> required(final Class<? extends Primitive> type, final Collection<Hash> items, final AbstractConnection connection) throws IOException
			{
				if (type.equals(BlockVote.class) == false)
				{
					blocksLog.error(BlockHandler.this.context.getName()+": Block vote type expected but got "+type);
					return Collections.emptyList();
				}
				
				if (BlockHandler.this.context.getNode().isSynced() == false)
					return Collections.emptyList();
				
				List<Hash> required = new ArrayList<Hash>(items.size());
				for (Hash item : items)
				{
					if (BlockHandler.this.votesToVerify.containsKey(item))
						continue;

					if (BlockHandler.this.postponedVotes.containsKey(item))
						continue;
					
					required.add(item);
				}

				required.removeAll(BlockHandler.this.context.getLedger().getLedgerStore().has(required, type));

				return required;
			}
		});
		
		this.context.getNetwork().getGossipHandler().register(BlockVote.class, new GossipReceiver<BlockVote>() 
		{
			@Override
			public void receive(final Collection<BlockVote> blockVotes, final AbstractConnection connection) throws IOException
			{
				if (BlockHandler.this.context.getNode().isSynced() == false)
					return;

				for (final BlockVote blockVote : blockVotes)
				{
					if (blocksLog.hasLevel(Logging.INFO))
						blocksLog.info(BlockHandler.this.context.getName()+": Received block vote "+blockVote.getHash()+" for "+blockVote.getHeight()+"@"+blockVote.getBlock()+" by "+blockVote.getOwner());
		
					final Epoch epoch = Epoch.from(blockVote.getBlock());
					final int numShardGroups = BlockHandler.this.context.getLedger().numShardGroups(epoch);
					final ShardGroupID blockShardGroupID = ShardMapper.toShardGroup(blockVote.getOwner().getIdentity(), numShardGroups);
					final ShardGroupID localShardGroupID = ShardMapper.toShardGroup(BlockHandler.this.context.getNode().getIdentity(), numShardGroups);
					if (localShardGroupID.equals(blockShardGroupID) == false)
					{
						blocksLog.warn(BlockHandler.this.context.getName()+": Block vote "+blockVote.getHash()+" for "+blockVote.getOwner()+" is for shard group ID "+blockShardGroupID+" but expected local shard group ID "+localShardGroupID);
						// TODO disconnect and ban;
						continue;
					}
						
					if (BlockHandler.this.context.getLedger().getLedgerStore().store(blockVote).equals(OperationStatus.SUCCESS))
					{
						BlockHandler.this.votesToVerify.put(blockVote.getHash(), blockVote);
						
						if (BlockHandler.this.context.getNetwork().getGossipHandler().broadcast(blockVote, localShardGroupID) == false)
							blocksLog.warn(BlockHandler.this.context.getName()+": Failed to broadcast block vote "+blockVote);
					}
					else
						blocksLog.warn(BlockHandler.this.context.getName()+": Failed to store block vote "+blockVote);
				}
				
				BlockHandler.this.blockProcessor.signal();
			}
		});
		
		this.context.getNetwork().getGossipHandler().register(BlockVote.class, new GossipFetcher<BlockVote>() 
		{
			public Collection<BlockVote> fetch(final Collection<Hash> items, final AbstractConnection connection) throws IOException
			{
				List<Hash> toFetch = new ArrayList<Hash>(items);
				List<BlockVote> fetched = new ArrayList<BlockVote>(items.size());
				Iterator<Hash> toFetchIterator = toFetch.iterator();
				while(toFetchIterator.hasNext())
				{
					Hash item = toFetchIterator.next();
					BlockVote blockVote = BlockHandler.this.votesToVerify.get(item);
					if (blockVote == null)
						blockVote = BlockHandler.this.postponedVotes.get(item);
					if (blockVote == null)
						continue;

					fetched.add(blockVote);
					toFetchIterator.remove();
				}
					
				BlockHandler.this.context.getLedger().getLedgerStore().get(toFetch, BlockVote.class, (h, p) -> { fetched.add(p); toFetch.remove(h); });

				if (toFetch.isEmpty() == false)
					toFetch.forEach(h -> blocksLog.error(BlockHandler.this.context.getName()+": Requested block vote "+h+" not found"));
				
				return fetched;
			}
		});

		// SYNC //
		this.context.getNetwork().getMessaging().register(SyncAcquiredMessage.class, this.getClass(), new SyncAcquiredMessageProcessor(this.context)
		{
			@Override
			public void process(final SyncAcquiredMessage syncAcquiredMessage, final AbstractConnection connection)
			{
				try
				{
					if (blocksLog.hasLevel(Logging.DEBUG))
						blocksLog.debug(BlockHandler.this.context.getName()+": Block pool inventory request from "+connection);
					
					int broadcastedBlockVoteCount = 0;
					int broadcastedBlockHeaderCount = 0;
					final Set<Hash> blockVoteInventory = new LinkedHashSet<Hash>();
					final Set<Hash> pendingBlockInventory = new LinkedHashSet<Hash>();
					
					// Deferred to broadcast last
					final Set<Hash> deferredPendingBlockInventory = new LinkedHashSet<Hash>();
					BlockHandler.this.pendingBlocks.forEach((h, pb) -> deferredPendingBlockInventory.add(pb.getHash()));
					
					long syncInventoryHeight = Math.max(1, syncAcquiredMessage.getHead().getHeight() - Constants.SYNC_INVENTORY_HEAD_OFFSET);
					while (syncInventoryHeight <= BlockHandler.this.context.getLedger().getHead().getHeight())
					{
						Hash syncBlockHash = BlockHandler.this.context.getLedger().getLedgerStore().getSyncBlock(syncInventoryHeight);
						pendingBlockInventory.add(syncBlockHash);
						deferredPendingBlockInventory.remove(syncBlockHash);
						
						BlockHandler.this.context.getLedger().getLedgerStore().getSyncInventory(syncInventoryHeight, BlockHeader.class).forEach(bh -> pendingBlockInventory.add(bh.getHash()));
						BlockHandler.this.context.getLedger().getLedgerStore().getSyncInventory(syncInventoryHeight, BlockVote.class).forEach(bv -> blockVoteInventory.add(bv.getHash()));
						
						broadcastedBlockHeaderCount += broadcastSyncInventory(pendingBlockInventory, BlockHeader.class, Constants.MAX_REQUEST_INVENTORY_ITEMS, connection);
						broadcastedBlockVoteCount += broadcastSyncInventory(blockVoteInventory, BlockVote.class, Constants.MAX_REQUEST_INVENTORY_ITEMS, connection);
						
						syncInventoryHeight++;
					}
					
					// Send any remaining (also now send the deferred)
					broadcastedBlockHeaderCount += broadcastSyncInventory(pendingBlockInventory, BlockHeader.class, 0, connection);
					broadcastedBlockVoteCount += broadcastSyncInventory(blockVoteInventory, BlockVote.class, 0, connection);
					broadcastedBlockHeaderCount += broadcastSyncInventory(deferredPendingBlockInventory, BlockHeader.class, 0, connection);

					syncLog.log(BlockHandler.this.context.getName()+": Broadcasted "+broadcastedBlockHeaderCount+" blocks to "+connection);
					syncLog.log(BlockHandler.this.context.getName()+": Broadcasted "+broadcastedBlockVoteCount+" block votes to "+connection);
				}
				catch (Exception ex)
				{
					blocksLog.error(BlockHandler.this.context.getName()+": ledger.messages.block.get.pool " + connection, ex);
				}
			}
		});

		this.context.getEvents().register(this.syncChangeListener);
		this.context.getEvents().register(this.syncBlockListener);
		this.context.getEvents().register(this.syncProgressListener);
		this.context.getEvents().register(this.asyncBlockListener);

		Thread blockProcessorThread = new Thread(this.blockProcessor);
		blockProcessorThread.setDaemon(true);
		blockProcessorThread.setName(this.context.getName()+" Block Processor");
		blockProcessorThread.start();
		
		Thread buildAndCommitThread = new Thread(this.buildAndCommitProcessor);
		buildAndCommitThread.setDaemon(true);
		buildAndCommitThread.setName(this.context.getName()+" Commit Processor");
		buildAndCommitThread.start();
	}

	@Override
	public void stop() throws TerminationException
	{
		this.blockProcessor.terminate(true);
		
		this.context.getEvents().unregister(this.asyncBlockListener);
		this.context.getEvents().unregister(this.syncProgressListener);
		this.context.getEvents().unregister(this.syncBlockListener);
		this.context.getEvents().unregister(this.syncChangeListener);
		this.context.getNetwork().getMessaging().deregisterAll(this.getClass());
	}
	
	// PROCESSING PIPELINE //
	private long reportedLatent = -1;
	private Hash latentHash = null;
	private void _updateBlocks(final BlockHeader head)
	{
		// Take a copy as this function may be expensive and we don't
		// want to hold the mutex object for long periods and block other threads.
		
		final List<PendingBlock> pendingBlocks;
		synchronized(this.pendingBlocks)
		{
			pendingBlocks = new ArrayList<>(this.pendingBlocks.values());
			Collections.sort(pendingBlocks, (pb1, pb2) -> {
				if (pb1.getHeight() < pb2.getHeight())
					return -1;
				if (pb1.getHeight() > pb2.getHeight())
					return 1;
				
				return pb1.getHash().compareTo(pb2.getHash());
			});
		}

		// Try to build the block primitives within the pending blocks
		for (int pb = 0 ; pb < pendingBlocks.size() ; pb++)
		{
			final PendingBlock pendingBlock = pendingBlocks.get(pb);
			if (pendingBlock.getHeader() == null)
				continue;
			
			try
			{
				final ProgressRound proposalRound = getProgressRound(pendingBlock.getHeight(), false);
				if (proposalRound.canPropose(pendingBlock.getHeader().getProposer()) == true)
				{
					final long roundVotePower = this.context.getLedger().getValidatorHandler().getVotePower(proposalRound.epoch(), pendingBlock.getHeader().getProposer());
					if (proposalRound.propose(pendingBlock.getHeader(), roundVotePower) == false)
						blocksLog.warn(this.context.getName()+": Progress round "+pendingBlock.getHeight()+" already has a proposal from "+pendingBlock.getHeader().getProposer());
					else if (blocksLog.hasLevel(Logging.INFO))
						blocksLog.info(this.context.getName()+": Seen proposal "+pendingBlock.getHash()+" "+pendingBlock.getHeader().getView()+" for progress round "+proposalRound.clock()+" from "+pendingBlock.getHeader().getProposer());
				}
				
				if (pendingBlock.isConstructed() == false)
				{
					final PendingBlock previousBlock = this.pendingBlocks.get(pendingBlock.getHeader().getPrevious());
					if (pendingBlock.getHeader().getPrevious().equals(head.getHash()) == false && 
						previousBlock != null && previousBlock.isConstructed() == false)
						continue;

					if (blocksLog.hasLevel(Logging.DEBUG))
						blocksLog.debug(this.context.getName()+": Updating block "+pendingBlock);
				
					boolean isLatent = false;
					if (this.reportedLatent == -1 && System.currentTimeMillis() - pendingBlock.getHeader().getTimestamp() > TimeUnit.SECONDS.toMillis(5))
					{
						this.reportedLatent = pendingBlock.getHeight();
						isLatent = true;
					}

					// TODO A real hunk of boilerplate stuff to build the contents.  Needs improving at some point 
					List<Hash> absentInventory = pendingBlock.getAbsent(InventoryType.ACCEPTED);
					for (int i = 0 ; i < absentInventory.size() ; i++)
					{
						final Hash atomHash = absentInventory.get(i);
						final PendingAtom pendingAtom = this.context.getLedger().getAtomHandler().get(atomHash);
						if (pendingAtom == null || pendingAtom.getStatus().before(AtomStatus.State.PREPARED))
						{
							if (isLatent)
							{
								this.latentHash = atomHash;
								blocksLog.error(this.context.getName()+": Accepted atom "+atomHash+" for block "+pendingBlock.getHash()+" has critical delay");
							}
							else if (blocksLog.hasLevel(Logging.DEBUG))
								blocksLog.debug(this.context.getName()+": Accepted atom "+atomHash+" for block "+pendingBlock.getHash()+" is absent");
								
							break;
						}
							
						pendingBlock.put(pendingAtom, InventoryType.ACCEPTED);
						if (atomHash.equals(this.latentHash))
						{
							this.reportedLatent = -1;
							this.latentHash = null;
						}
					}
		
					absentInventory = pendingBlock.getAbsent(InventoryType.UNACCEPTED);
					for (int i = 0 ; i < absentInventory.size() ; i++)
					{
						final Hash unacceptedHash = absentInventory.get(i);
						final PendingAtom pendingAtom = this.context.getLedger().getAtomHandler().get(unacceptedHash);
						if (pendingAtom == null || pendingAtom.getTimeout() == null || pendingAtom.getTimeout() instanceof AcceptTimeout == false)
						{
							if (isLatent)
							{
								this.latentHash = unacceptedHash;
								blocksLog.error(this.context.getName()+": Unaccepted atom "+unacceptedHash+" for block "+pendingBlock.getHash()+" has critical delay");
							}
							else if (blocksLog.hasLevel(Logging.DEBUG))
								blocksLog.debug(this.context.getName()+": Unaccepted atom "+unacceptedHash+" for block "+pendingBlock.getHash()+" is absent");

							break;
						}

						pendingBlock.put(pendingAtom, InventoryType.UNACCEPTED);
						if (unacceptedHash.equals(this.latentHash))
						{
							this.reportedLatent = -1;
							this.latentHash = null;
						}
					}

					absentInventory = pendingBlock.getAbsent(InventoryType.EXECUTABLE);
					for (int i = 0 ; i < absentInventory.size() ; i++)
					{
						final Hash atomHash = absentInventory.get(i);
						final PendingAtom pendingAtom = this.context.getLedger().getAtomHandler().get(atomHash);
						if (pendingAtom == null || pendingAtom.getStatus().before(AtomStatus.State.PREPARED))
						{
							if (isLatent)
							{
								this.latentHash = atomHash;
								blocksLog.error(this.context.getName()+": Executable atom "+atomHash+" for block "+pendingBlock.getHash()+" has critical delay");
							}
							else if (blocksLog.hasLevel(Logging.DEBUG))
								blocksLog.debug(this.context.getName()+": Executable atom "+atomHash+" for block "+pendingBlock.getHash()+" is absent");

							break;
						}

						pendingBlock.put(pendingAtom, InventoryType.EXECUTABLE);
						if (atomHash.equals(this.latentHash))
						{
							this.reportedLatent = -1;
							this.latentHash = null;
						}
					}

					absentInventory = pendingBlock.getAbsent(InventoryType.LATENT);
					for (int i = 0 ; i < absentInventory.size() ; i++)
					{
						final Hash latentHash = absentInventory.get(i);
						final PendingAtom pendingAtom = this.context.getLedger().getAtomHandler().get(latentHash);
						if (pendingAtom == null || pendingAtom.getTimeout() == null || pendingAtom.getTimeout() instanceof ExecutionLatentTimeout == false)
						{
							if (isLatent)
							{
								this.latentHash = latentHash;
								blocksLog.error(this.context.getName()+": Latent atom "+latentHash+" for block "+pendingBlock.getHash()+" has critical delay");
							}
							else if (blocksLog.hasLevel(Logging.DEBUG))
								blocksLog.debug(this.context.getName()+": Latent atom "+latentHash+" for block "+pendingBlock.getHash()+" is absent");

							break;
						}

						pendingBlock.put(pendingAtom, InventoryType.LATENT);
						if (latentHash.equals(this.latentHash))
						{
							this.reportedLatent = -1;
							this.latentHash = null;
						}
					}

					absentInventory = pendingBlock.getAbsent(InventoryType.COMMITTED);
					for (int i = 0 ; i < absentInventory.size() ; i++)
					{
						final Hash certificateHash = absentInventory.get(i);
						final PendingAtom pendingAtom = this.context.getLedger().getAtomHandler().certificate(certificateHash);
						if (pendingAtom == null || pendingAtom.getCertificate() == null)
						{
							if (isLatent)
							{
								this.latentHash = certificateHash;
								blocksLog.error(this.context.getName()+": Atom certificate "+certificateHash+" for block "+pendingBlock.getHash()+" has critical delay");
							}
							else if (blocksLog.hasLevel(Logging.DEBUG))
								blocksLog.debug(this.context.getName()+": Atom certificate "+certificateHash+" for block "+pendingBlock.getHash()+" is absent");
							break;
						}

						pendingBlock.put(pendingAtom, InventoryType.COMMITTED);
						if (certificateHash.equals(this.latentHash))
						{
							this.reportedLatent = -1;
							this.latentHash = null;
						}
					}
					
					absentInventory = pendingBlock.getAbsent(InventoryType.UNEXECUTED);
					for (int i = 0 ; i < absentInventory.size() ; i++)
					{
						final Hash unexecutedHash = absentInventory.get(i);
						final PendingAtom pendingAtom = this.context.getLedger().getAtomHandler().unexecuted(unexecutedHash);
						if (pendingAtom == null || pendingAtom.getTimeout() == null || 
							pendingAtom.getTimeout() instanceof ExecutionTimeout == false || pendingAtom.getTimeout().getHash().equals(unexecutedHash) == false)
						{
							if (isLatent)
							{
								this.latentHash = unexecutedHash;
								blocksLog.error(this.context.getName()+": Atom execution timeout "+unexecutedHash+" for block "+pendingBlock.getHash()+" has critical delay");
							}
							else if (blocksLog.hasLevel(Logging.DEBUG))
								blocksLog.debug(this.context.getName()+": Atom execution timeout "+unexecutedHash+" for block "+pendingBlock.getHash()+" is absent");

							break;
						}

						pendingBlock.put(pendingAtom, InventoryType.UNEXECUTED);
						if (unexecutedHash.equals(this.latentHash))
						{
							this.reportedLatent = -1;
							this.latentHash = null;
						}
					}

					absentInventory = pendingBlock.getAbsent(InventoryType.UNCOMMITTED);
					for (int i = 0 ; i < absentInventory.size() ; i++)
					{
						final Hash uncommitedHash = absentInventory.get(i);
						final PendingAtom pendingAtom = this.context.getLedger().getAtomHandler().uncommitted(uncommitedHash);
						if (pendingAtom == null || pendingAtom.getTimeout() == null || 
							pendingAtom.getTimeout() instanceof CommitTimeout == false || pendingAtom.getTimeout().getHash().equals(uncommitedHash) == false)
						{
							if (isLatent)
							{
								this.latentHash = uncommitedHash;
								blocksLog.error(this.context.getName()+": Atom commit timeout "+uncommitedHash+" for block "+pendingBlock.getHash()+" has critical delay");
							}
							else if (blocksLog.hasLevel(Logging.DEBUG))
								blocksLog.debug(this.context.getName()+": Atom commit timeout "+uncommitedHash+" for block "+pendingBlock.getHash()+" is absent");

							break;
						}

						pendingBlock.put(pendingAtom, InventoryType.UNCOMMITTED);
						if (uncommitedHash.equals(this.latentHash))
						{
							this.reportedLatent = -1;
							this.latentHash = null;
						}
					}
					
					absentInventory = pendingBlock.getAbsent(InventoryType.PACKAGES);
					for (int i = 0 ; i < absentInventory.size() ; i++)
					{
						final Hash packageHash = absentInventory.get(i);
						final PolyglotPackage pakage = this.context.getLedger().getPackageHandler().uncommitted(packageHash);
						if (pakage == null)
						{
							if (isLatent)
							{
								this.latentHash = packageHash;
								blocksLog.error(this.context.getName()+": Package "+packageHash+" for block "+pendingBlock.getHash()+" has critical delay");
							}
							else if (blocksLog.hasLevel(Logging.DEBUG))
								blocksLog.debug(this.context.getName()+": Package "+packageHash+" for block "+pendingBlock.getHash()+" is absent");

							break;
						}

						pendingBlock.put(pakage, InventoryType.PACKAGES);
						if (packageHash.equals(this.latentHash))
						{
							this.reportedLatent = -1;
							this.latentHash = null;
						}
					}
					
					if (pendingBlock.isConstructable())
					{
						pendingBlock.constructBlock();
						
						if (blocksLog.hasLevel(Logging.INFO))
							blocksLog.info(this.context.getName()+": Constructed proposal "+pendingBlock.toString());
					}
				}
			}
			catch (Exception e)
			{
				blocksLog.error(this.context.getName()+": Update of proposal "+pendingBlock.toString()+" failed", e);
			}
		}
	}
	
	private void _updateBranches(final BlockHeader head)
	{
		// Process blocks that are unbranched, take a copy as may need to 
		// add new pending blocks we know about to reconstruct branches in
		// certain cases (short lived de-syncs)
		final List<PendingBlock> pendingBlocks;
		synchronized(this.pendingBlocks)
		{
			pendingBlocks = new ArrayList<>(this.pendingBlocks.values());
			Collections.sort(pendingBlocks, (pb1, pb2) -> {
				if (pb1.getHeight() < pb2.getHeight())
					return -1;
				if (pb1.getHeight() > pb2.getHeight())
					return 1;
				
				return pb1.getHash().compareTo(pb2.getHash());
			});
		}
		
		for (int pb = 0 ; pb < pendingBlocks.size() ; pb++)
		{
			final PendingBlock pendingBlock = pendingBlocks.get(pb);
			// If an exception has been thrown on this pending block do not process it
			// any longer, nor apply to any branches.  It will be pruned later via the 
			// commit branch and block pruning mechanism.
			if (pendingBlock.thrown() != null)
				continue;
			
			try
			{
				if (pendingBlock.isUnbranched())
					updateBranchesWith(pendingBlock, head);
			}
			catch (Exception ex)
			{
				blocksLog.error(this.context.getName()+": Block update "+pendingBlock+" failed", ex);
			}
		}

		// Update the branches
		// TODO redundant?  Or worth doing so post update housekeeping / verification?
		synchronized(this.pendingBranches)
		{
			final Iterator<PendingBranch> pendingBranchIterator = this.pendingBranches.iterator();
			while (pendingBranchIterator.hasNext())
			{
				final PendingBranch pendingBranch = pendingBranchIterator.next();
				try
				{
					if (pendingBranch.getRoot().getHash().equals(head.getHash()) == false && 
						pendingBranch.contains(head.getHash()) == false)
					{
						if (blocksLog.hasLevel(Logging.DEBUG))
							blocksLog.debug(this.context.getName()+": Branch doesn't attach to ledger "+pendingBranch.getRoot());
						
						continue;
					}
					
					// Update branch and apply any currently unapplied pending blocks
					pendingBranch.update();
				}
				catch (Exception ex)
				{
					blocksLog.error(this.context.getName()+": Branch update "+pendingBranch.toString()+" failed", ex);
				}
			}
		}
	}

	private void _processHeaders(final ProgressRound progressRound)
	{
		final List<BlockHeader> headersToVerify = new ArrayList<BlockHeader>(this.headersToVerify.size());
		BlockHandler.this.headersToVerify.forEach((h, bh) -> {
			if (bh.getHeight() <= progressRound.clock())
				headersToVerify.add(bh);
		});

		if (headersToVerify.isEmpty() == false)
		{
			headersToVerify.sort((b1, b2) -> Longs.compare(b1.getHeight(), b2.getHeight()));
			final Iterator<BlockHeader> headersToVerifyIterator = headersToVerify.iterator();
			while(headersToVerifyIterator.hasNext())
			{
				final BlockHeader headerToVerify = headersToVerifyIterator.next();
				try
				{
					BlockInsertStatus status = insert(headerToVerify);
					if (status.equals(BlockInsertStatus.POSTPONED))
						headersToVerifyIterator.remove();
				}
				catch (IOException ex)
				{
					blocksLog.error(this.context.getName()+": Insertion of block header "+headerToVerify+" failed", ex);
				}
				catch (ValidationException | CryptoException ex)
				{
					blocksLog.error(this.context.getName()+": Validation of block header "+headerToVerify+" failed", ex);
				}
			}

			// All but postponed headers should remain in here
			for (final BlockHeader headerToVerify : headersToVerify)
			{
				if (this.headersToVerify.remove(headerToVerify.getHash(), headerToVerify) == false && this.context.getNode().isSynced())
					blocksLog.warn(this.context.getName()+": Block header peek/remove failed for "+headerToVerify);
			}
		}
	}
	
	private void _processVotes(final ProgressRound round, final BlockHeader head)
	{
		_verifyPendingVotes(round, head);
		
		_collateVotesAndApply(round, head);
		
		_tryPostponedVotes(round, head);
	}
	
	private void _verifyPendingVotes(final ProgressRound round, final BlockHeader head)
	{
		if (this.votesToVerify.isEmpty() == false)
		{
			final List<BlockVote> votesToVerify = new ArrayList<BlockVote>(this.votesToVerify.size());
			this.votesToVerify.forEach((h, bv) -> {
				if (bv.getHeight() <= round.clock())
					votesToVerify.add(bv);
			});

			synchronized(this.votesToVerify)
			{
				final Iterator<BlockVote> votesToVerifyIterator = this.votesToVerify.values().iterator();
				while (votesToVerifyIterator.hasNext())
				{
					final BlockVote blockVote = votesToVerifyIterator.next();
					
					// Early
					if (blockVote.getHeight() > round.clock())
						continue;

					// Stale
					if (blockVote.getHeight() <= head.getHeight())
					{
						votesToVerifyIterator.remove();
						if (blocksLog.hasLevel(Logging.DEBUG))
							blocksLog.debug(this.context.getName()+": Removed stale block vote "+blockVote.getHash()+" for "+blockVote.getHeight()+"@"+blockVote.getBlock());
						
						continue;
					}

					try
					{
						// Get progress round for this block vote 
						final ProgressRound voteRound = getProgressRound(blockVote.getHeight(), false);

						// Find or create block vote collector
						final BlockVoteCollector blockVoteCollector = this.blockVoteCollectors.computeIfAbsent(blockVote.getHeight(), h -> new BlockVoteCollector(this.context, voteRound));
						if (blockVoteCollector.hasVoted(blockVote.getOwner().getIdentity()) == false)
						{
							final long roundVotePower = this.context.getLedger().getValidatorHandler().getVotePower(voteRound.epoch(), blockVote.getOwner().getIdentity());
							if (roundVotePower == 0)
								blocksLog.warn(this.context.getName()+": Vote power is zero in epoch "+voteRound.epoch()+" for "+blockVote.getOwner().toString(12));
								
							blockVote.setWeight(roundVotePower);
							blockVoteCollector.vote(blockVote);
						}
						else
						{
							// TODO penalties for extra voting
							blocksLog.warn(this.context.getName()+": Block vote "+blockVote.getHash()+" already seen in progress round "+voteRound.clock()+" for "+blockVote.getOwner().toString(12));
						}
					}
					catch (Exception ex)
					{
						blocksLog.error(this.context.getName()+": Pre-processing of block vote failed for "+blockVote.getHash()+" for block "+blockVote.getBlock()+" by "+blockVote.getOwner(), ex);
					}
					finally
					{
						votesToVerifyIterator.remove();
					}
				}
			}
		}
	}
	
	private void _collateVotesAndApply(final ProgressRound round, final BlockHeader head)
	{
		if (this.blockVoteCollectors.isEmpty() == false)
		{
			boolean signalVotePhaseCompleted = false;
			synchronized(this.blockVoteCollectors)
			{
				final Iterator<BlockVoteCollector> blockVoteCollectorIterator = this.blockVoteCollectors.values().iterator();
				while(blockVoteCollectorIterator.hasNext())
				{
					final BlockVoteCollector blockVoteCollector = blockVoteCollectorIterator.next();
					if (blockVoteCollector.canVerify() == false)
						continue;

					try
					{
						final long currentRoundVoteWeight = blockVoteCollector.getProgressRound().getVoteWeight();
						final List<BlockVote> validBlockVotes = blockVoteCollector.tryVerify();
						if (validBlockVotes.isEmpty() == false)
						{
							if (blocksLog.hasLevel(Logging.DEBUG))
							{
								blocksLog.debug(this.context.getName()+": Valid block votes to process for progress round "+blockVoteCollector.getProgressRound().clock());
								for (final BlockVote validBlockVote : validBlockVotes)
									blocksLog.debug(this.context.getName()+":       "+validBlockVote.getHeight()+":"+validBlockVote.getBlock()+" from "+validBlockVote.getOwner().getIdentity().toString(12));
							}
							
							for (final BlockVote blockVote : validBlockVotes)
							{
								try
								{
									final BlockVoteStatus status = process(blockVote, head);
									if (status.equals(BlockVoteStatus.POSTPONED))
										this.postponedVotes.put(blockVote.getHash(), blockVote);
								}
								catch (Exception ex)
								{
									blocksLog.error(this.context.getName()+": Error processing block vote "+blockVote, ex);
								}
							}
							
							// Signal vote weight threshold reached?
							if (currentRoundVoteWeight < round.getVoteThreshold())
							{
								if (round.isVoteCompleted())
									signalVotePhaseCompleted = true;
							}
						}
					}
					catch (Exception ex)
					{
						blocksLog.error(this.context.getName()+": Error processing block vote collector for progress round "+blockVoteCollector.getHeight(), ex);
					}
				}
			}
			
			if (signalVotePhaseCompleted)
				this.blockProcessor.signal();
		}
	}
	
	private void _tryPostponedVotes(final ProgressRound round, final BlockHeader head)
	{
		if (this.postponedVotes.isEmpty() == false)
		{
			// Delayed state votes to count
			if (blocksLog.hasLevel(Logging.DEBUG))
				blocksLog.info(this.context.getName()+": Evaluating "+this.postponedVotes.size()+" postponed block votes");

			boolean signalVotePhaseCompleted = false;
			synchronized(this.postponedVotes)
			{
				final long currentRoundVoteWeight = round.getVoteWeight();
				final Iterator<BlockVote> votesToCountDelayedIterator = this.postponedVotes.values().iterator();
				while(votesToCountDelayedIterator.hasNext())
				{
					final BlockVote delayedBlockVote = votesToCountDelayedIterator.next(); 
					try
					{
						if (blocksLog.hasLevel(Logging.DEBUG))
							blocksLog.debug(this.context.getName()+": Evaluating delayed block vote "+delayedBlockVote.getHash()+" for block "+delayedBlockVote.getBlock()+" by "+delayedBlockVote.getOwner());
						
						final BlockVoteStatus status = process(delayedBlockVote, head);
						if (status.equals(BlockVoteStatus.POSTPONED) == false)
						{
							if (blocksLog.hasLevel(Logging.INFO))
								blocksLog.info(this.context.getName()+": Processed delayed block vote "+delayedBlockVote.getHash()+" for "+delayedBlockVote.getHeight()+"@"+delayedBlockVote.getBlock()+" for "+delayedBlockVote.getOwner());
							
							votesToCountDelayedIterator.remove();
						}
					}
					catch (Exception ex)
					{
						votesToCountDelayedIterator.remove();
						blocksLog.error(this.context.getName()+": Processing of delayed block vote failed for "+delayedBlockVote.getHash()+" for block "+delayedBlockVote.getBlock()+" by "+delayedBlockVote.getOwner(), ex);
					}
				}
				
				// Signal vote weight threshold reached?
				if (currentRoundVoteWeight < round.getVoteThreshold())
				{
					if (round.isVoteCompleted())
						signalVotePhaseCompleted = true;
				}
			}
			
			if (signalVotePhaseCompleted)
				this.blockProcessor.signal();
		}
	}
	
	// LIVENESS PIPELINE //
	// TODO separate out 
	private void _livenessTick(final ProgressRound round, final QuorumCertificate progressView, final BlockHeader head)
	{
		if (round.getState().equals(ProgressRound.State.NONE))
		{
			round.stepState();
			this.context.getEvents().post(new ProgressPhaseEvent(round));
			return;
		}

		if (round.getState().equals(ProgressRound.State.PROPOSING))
		{
			if (round.isProposalsTimedout() || round.isProposalsCompleted())
			{
				if (round.isProposalsTimedout())
					blocksLog.warn(this.context.getName()+": Proposal phase timed out "+round);
				else if (blocksLog.hasLevel(Logging.INFO))
					blocksLog.info(this.context.getName()+": Proposal phase completed "+round);

				round.stepState();
				this.context.getEvents().post(new ProgressPhaseEvent(round));
			}
			
			return;
		}
		
		if (round.getState().equals(ProgressRound.State.TRANSITION))
		{
			final long transitionThreshold = round.getProposeThreshold();
			long constructedVotePower = 0;

			// Must have constructed proposals to proceed to VOTING phase
			// Check all primaries proposed or TRANSITION phase is latent
			if (round.isTransitionLatent() || round.isFullyProposed())
			{
				for(final Hash proposal : round.getProposals())
				{
					final PendingBlock pendingBlock = this.pendingBlocks.get(proposal);
					if (pendingBlock == null)
						continue;
					
					if (pendingBlock.isConstructed() == false)
						continue;
					
					try
					{
						constructedVotePower += this.context.getLedger().getValidatorHandler().getVotePower(round.epoch(), pendingBlock.getHeader().getProposer());
					}
					catch (IOException ioex)
					{
						blocksLog.error(this.context.getName()+": Failed to get vote power for proposer "+pendingBlock.getHeader().getProposer().toString(12)+" of proposal "+pendingBlock.getHeight()+":"+pendingBlock.getHash(), ioex);
					}
				}
			}
			
			if (constructedVotePower >= transitionThreshold || round.isTransitionLatent())
			{
				// Phase can proceed if the constructed proposals satisfy 2f+1 threshold, or if the transition phase 
				// is latent and there is at least ONE constructed proposal.
				// TODO might be improvements possible here by considering primary and secondary proposers separately
				if (constructedVotePower > 0)
				{
					if (round.isTransitionLatent())
						blocksLog.warn(this.context.getName()+": Transition phase latent "+round);
					else if (blocksLog.hasLevel(Logging.INFO))
						blocksLog.info(this.context.getName()+": Transition phase completed "+round);
	
					round.stepState();
					
					// Do the local vote here
					// If the local instance is behind (views dont match) it will not vote
					PendingBranch selectedBranch = selectBranchToVote(round, round.getView());
					try 
					{
						if (selectedBranch != null)
							vote(round, selectedBranch);
					} 
					catch (IOException | CryptoException | ValidationException ex) 
					{
						blocksLog.error(this.context.getName()+": Failed to cast vote on progress round "+round+" to branch "+selectedBranch, ex);
					}
					
					this.context.getEvents().post(new ProgressPhaseEvent(round));
				}
				// Transition phase is latent, nothing constructed, therefore nothing locally proposed as a 
				// primary or secondary proposer either!
				//
				// Local instance could be latent or livestuck, trigger a view update which hopefully in turn triggers a commit.
				else if (round.canTransitionUpdate())
				{
					blocksLog.warn(this.context.getName()+": Transition phase for progress round "+round.clock()+" is stalled, liveness lost, triggering liveness kick");
					updateCommittable(round.clock(), progressView, head);
					round.transitionUpdated();
					this.blockProcessor.signal();
				}
			}

			return;
		}
		
		if (round.getState().equals(ProgressRound.State.VOTING))
		{
			if (round.isVoteTimedout() || round.isVoteCompleted())
			{
				round.stepState();
				this.context.getEvents().post(new ProgressPhaseEvent(round));
				
				if (round.isVoteTimedout())
					blocksLog.warn(this.context.getName()+": Voting phase timed out "+round);								
				else if (blocksLog.hasLevel(Logging.INFO))
					blocksLog.info(this.context.getName()+": Voting phase completed "+round);
			}

			return;
		}
		
		if (round.getState().equals(ProgressRound.State.COMPLETED))
		{
			final List<Identity> absentProposers = round.getAbsentProposers();
			if (absentProposers.isEmpty() == false)
				blocksLog.warn(this.context.getName()+": Absent proposers ["+absentProposers.stream().map(i -> i.toString(12)).collect(Collectors.joining(", "))+"] in progress round "+round);								
			
			final long nextProgressRoundClock = this.progressClock.incrementAndGet();
			final ProgressRound nextProgressRound = getProgressRound(nextProgressRoundClock, true);
			this.context.getEvents().post(new ProgressPhaseEvent(nextProgressRound));

			if (blocksLog.hasLevel(Logging.INFO))
				blocksLog.info(this.context.getName()+": Progress round completed "+round);

			// Progress interval & delay
			// If local replica is slow, then skip the wait delay if possible to attempt to catch up.  
			// If it is fast, then it will naturally have to wait for a quorum or timeout, slowing it down.
			// Finally if load and latency is such that new views are not being formed, gradually increase the delay
			final long targetRoundDuration = Math.max(Ledger.definitions().roundInterval(), Configuration.getDefault().get("ledger.liveness.delay", 0));
			final long roundDelayDuration = (targetRoundDuration-round.getDuration());
			final long roundDelayAdjustment = round.drift();
			final long adjustedRoundDelayDuration = roundDelayDuration+roundDelayAdjustment;
			
			// Within interval bounds
			if (adjustedRoundDelayDuration >= 0)
			{
				try 
				{
					if (blocksLog.hasLevel(Logging.INFO))
						blocksLog.info(this.context.getName()+": Round delay for "+round.clock()+" of "+adjustedRoundDelayDuration+"ms with duration "+round.getDuration()+"ms / delay "+roundDelayDuration+"ms / adjustment "+roundDelayAdjustment+"ms");
					
					// TODO better way to implement this delay as simply sleeping costs potential 
					// processing time which could be used to update / verify proposals and votes
					if (adjustedRoundDelayDuration > 0)
						Thread.sleep(adjustedRoundDelayDuration);
				} 
				catch (InterruptedException e) 
				{
					Thread.currentThread().interrupt();
			        blocksLog.warn(this.context.getName()+": Progress delay interval interrupted", e);
			        return;
				}
			}
			// Too slow
			else
			{
				if (blocksLog.hasLevel(Logging.INFO))
				{
					if (roundDelayAdjustment < 0)
						blocksLog.info(this.context.getName()+": Skipping round delay for "+round.clock()+" because replica is behind "+adjustedRoundDelayDuration+"ms with duration "+round.getDuration()+"ms / delay "+roundDelayDuration+"ms / adjustment "+roundDelayAdjustment+"ms");
					else
						blocksLog.info(this.context.getName()+": Skipping round delay for "+round.clock()+" because of long round interval "+round.getDuration()+"ms");
				}
			}
			
			final QuorumCertificate updatedView = updateView(round, head);
			nextProgressRound.start(updatedView);
			this.blockProcessor.signal();
			
			this.context.getMetaData().increment("ledger.interval.progress", round.getDuration());
			
			if (blocksLog.hasLevel(Logging.INFO))
				blocksLog.info(this.context.getName()+": Progress round is now "+nextProgressRound.toString());
		}
	}
	
	private void _decideCommit(final ProgressRound round, final ProgressRound.State roundPhase)
	{
		// Don't commit on phases where a proposal might be generated
		if ((roundPhase.equals(ProgressRound.State.PROPOSING) && round.isProposalsLatent() == false) || 
			(roundPhase.equals(ProgressRound.State.TRANSITION) && round.isTransitionLatent() == false)) 
			return;
		
		try
		{
			final PendingBranch selectedBranch = selectBranchWithQuorum(round);
			if (selectedBranch != null)
			{
				final PendingBlock commitTo = selectedBranch.commitable();
				if (commitTo != null)
					commit(commitTo, selectedBranch);
			}
		}
		catch (Exception ex)
		{
			// TODO additional clean up here, trim bad branch, etc
			blocksLog.error(this.context.getName()+": Failed to commit proposal on "+round, ex);
		}
	}

	// METHODS //
	public int size()
	{
		return this.pendingBlocks.size();
	}
	
	private ProgressRound getProgressRound(final long clock, final boolean create)
	{
		ProgressRound progressRound;
		
		synchronized(this.progressRounds)
		{
			if (create)
			{
				progressRound = this.progressRounds.getIfAbsentPut(clock, () -> {
					// FIXME needs to acquire value from ledger / branches but need vote power epochs etc
					final Epoch epoch = this.context.getLedger().getEpoch();
					final ShardGroupID localShardGroupID = ShardMapper.toShardGroup(this.context.getNode().getIdentity(), this.context.getLedger().numShardGroups());
					long nextTotalVotePower;
					long nextProposersVotePower;
					Set<Identity> nextProposers;
					try 
					{
						// TODO calculate seed
						nextProposers = this.context.getLedger().getValidatorHandler().getProposers(clock, Hash.ZERO, localShardGroupID);
						nextProposersVotePower = this.context.getLedger().getValidatorHandler().getVotePower(epoch, nextProposers);
						nextTotalVotePower = this.context.getLedger().getValidatorHandler().getTotalVotePower(epoch, localShardGroupID);
					} 
					catch (IOException e) 
					{ 
						blocksLog.fatal(this.context.getName()+": Failed to get progress sync vote power at "+clock, e);
						nextTotalVotePower = Long.MAX_VALUE; // Forces a progress vote timeout
						nextProposersVotePower = Long.MAX_VALUE; // Forces a progress propose timeout
						nextProposers = Collections.emptySet(); // Forces a progress propose timeout
					}
					
					return new ProgressRound(this.context, clock, nextProposers, nextProposersVotePower, nextTotalVotePower);
				});
			}
			else
			{
				progressRound = this.progressRounds.get(clock);
				
				if (progressRound == null)
					throw new IllegalStateException("Progress round "+clock+" is not created");
			}
		}
		
		return progressRound;
	}
	
	private QuorumCertificate updateView(final ProgressRound round, final BlockHeader head)
	{
		boolean isAdopted = false;
		final QuorumCertificate currentView = round.getView();
		QuorumCertificate updatedView = currentView;
		
		// Try to construct a new view from received votes in this and previous rounds
		for (long clock=Math.max(currentView.getHeight(), head.getHeight()+1) ; clock <= round.clock() ; clock++)
		{
			final ProgressRound pastRound;
			
			// Important to handle this properly as a commit may have happened but a view update may not
			try
			{
				pastRound = getProgressRound(clock, false);
			}
			catch (IllegalStateException isex)
			{
				blocksLog.warn(this.context.getName()+": Progress round "+clock+" not found when updating view QC in progress round "+round.clock());
				continue;
			}
			
			if (pastRound.hasCertificate() == false && 
			    pastRound.getVoteWeight() >= pastRound.getVoteThreshold())
			{
				try
				{
					// Determine the initial committable for the certificate
					final Hash initialCommittable;
					if (updatedView.getCommittable().equals(Hash.ZERO) == false)
					{
						if (updatedView.getPrevious().equals(Hash.ZERO) == false && head.getHeight() >= Block.toHeight(updatedView.getPrevious()))
							initialCommittable = updatedView.getPrevious();
						else
							initialCommittable = head.getHash();
					}
					else
						initialCommittable = head.getHash();
					
					// Build the certificate
					final QuorumCertificate certificate = pastRound.buildCertificate(updatedView, initialCommittable);
					if (certificate != null)
					{
						// Verify the view is not divergent due to latency or faultiness and would potentially cause a fork, stalling liveness
						if (verifyView(round, updatedView, certificate))
						{
							// Need guard lock as looking into branches for quorums
							this.guardLock.lock();
							try
							{
								// Set the `current` proposal referenced in the QC to the appropriate super type
								final PendingBlock currentBlock = this.pendingBlocks.get(certificate.getCurrent());
								if (currentBlock.isSuper() == PendingBlock.SUPR.INTR)
								{
									final SUPR type;
									// Super type depends on if local replica is behind, to the point that votes were received 
									// before the round started or if votes were received latently from other replicas
									//
									// Hard supers are only created if the view is being updated for the latest round AND all
									// votes constituting 2f+1 were received DURING the round
									if (round.drift() < Math.negateExact(Ledger.definitions().roundInterval()))
										type = SUPR.SOFT;
									else if (pastRound != round)
										type = SUPR.SOFT;
									else
										type = SUPR.HARD;
										
									currentBlock.setSuper(type);
								
									if (blocksLog.hasLevel(Logging.INFO))
										blocksLog.info(this.context.getName()+": Pending block 'current' is now a "+currentBlock.isSuper().name().toLowerCase()+" super from view QC creation "+currentBlock.toString());
								}
								
								if (blocksLog.hasLevel(Logging.DEBUG))
									blocksLog.debug(this.context.getName()+": Created view QC in progress round "+pastRound.clock()+" "+certificate);
								
								// Created a higher view QC than the referenced progress round view
								// NOTE:  It may not be higher than the current local view!
								if (certificate.getHeight() > updatedView.getHeight())
								{
									// Discover a committable block up to the round height to update the view QC
									final PendingBranch committableBranch = selectBranchWithQuorum(pastRound);
									if (committableBranch != null)
									{
										PendingBlock candidateBlock = committableBranch.commitable();
										if (candidateBlock != null)
										{
											final long prevHeight = Block.toHeight(certificate.getPrevious());
											if (prevHeight > 0 && candidateBlock.getHeight() > prevHeight)
												candidateBlock = committableBranch.getBlockAtHeight(prevHeight);
											
											if (candidateBlock != null)
											{
												// Check the discovered committable aligns with the created view
												if (viewContains(candidateBlock.getHash(), certificate))
													certificate.updateCommittable(candidateBlock.getHash());
												else
													blocksLog.warn(this.context.getName()+": Divergent `committable` "+candidateBlock.getHeight()+":"+candidateBlock.getHash()+" discovered when creating view QC in progress round "+pastRound.clock());
											}
										}
									}

									updatedView = certificate;
									isAdopted = false;
								}
							}
							finally
							{
								this.guardLock.unlock();
							}
						}
						else
							blocksLog.warn(this.context.getName()+": Divergent view QC created in progress round "+pastRound.clock()+" "+certificate);
					}
				}
				catch (Exception ex)
				{
					blocksLog.error(this.context.getName()+": Failed to construct view QC for progress round "+pastRound, ex);
				}
			}
		
			// Check if proposals carry a view which can be adopted
			// Also set proposals accordingly if they are referenced by a view QC
			// View QCs in proposals will have been validated when witnessed along with the proposal contents
			for (final Hash proposal : pastRound.getProposals())
			{
				final PendingBlock pendingBlock = this.pendingBlocks.get(proposal);
				if (pendingBlock == null)
				{
					// TODO why?
					blocksLog.warn(this.context.getName()+": Pending block "+proposal+" not found when inspecting proposal view QCs for progress round "+pastRound.clock());
					continue;
				}
				
				final QuorumCertificate certificate = pendingBlock.getHeader().getView();
				
				// Verify the committable contained in the proposal view QC
				verifyCommittable(round.clock(), certificate, head);
				
				// Verify the proposal view QC is not divergent due to latency or faultiness 
				// and could potentially cause a fork or stall liveness
				if (verifyView(round, updatedView, certificate) == false)
					continue;
	
				// Set observed proposals with a quorum as SOFT or committable supers 
				if (certificate.getCurrent().equals(Hash.ZERO) == false)
				{
					final PendingBlock currentBlock = this.pendingBlocks.get(certificate.getCurrent());
					if (currentBlock != null)
					{
						if (currentBlock.isSuper() == PendingBlock.SUPR.INTR)
						{
							currentBlock.setSuper(SUPR.SOFT);
							
							if (blocksLog.hasLevel(Logging.INFO))
								blocksLog.info(this.context.getName()+": Pending block 'current' is now a "+currentBlock.isSuper().name().toLowerCase()+" super from view QC inspection "+currentBlock.toString());
						}
					}
					else
					{
						// Committed? 
						try
						{
							final Hash committed = this.context.getLedger().getLedgerStore().getSyncBlock(Block.toHeight(certificate.getCurrent()));
							if (certificate.getCurrent().equals(committed) == false)
								blocksLog.error(this.context.getName()+": Pending block 'current' "+certificate.getCurrent()+" not found when inspecting proposal view QCs for progress round "+pastRound.clock());
						}
						catch (IOException ioex)
						{
							blocksLog.error(this.context.getName()+": Failed to retreive `current` proposal hash "+Block.toHeight(certificate.getCurrent())+":"+certificate.getCurrent()+" in progress round "+pastRound.clock(), ioex);
						}
					}
				}
				
				if (certificate.getHeight() <= updatedView.getHeight())
					continue;
				
				isAdopted = true;
				updatedView = certificate;
			}
		}
		
		// Have an updated view QC, check it is a higher view QC than the current local view
		if (updatedView.getHeight() > this.progressView.getHeight())
		{
			this.progressView = updatedView;

			if (blocksLog.hasLevel(Logging.INFO))
			{
				if (isAdopted)
					blocksLog.info(this.context.getName()+": Adopting view QC from proposal "+updatedView.getHeight()+":"+updatedView.getHash()+" for progress round "+round.clock()+" "+updatedView);
				else
					blocksLog.info(this.context.getName()+": Updated view QC in progress round "+round.clock()+" "+updatedView);
			}
		}
		else
			blocksLog.info(this.context.getName()+": Retaining view QC in progress round "+round.clock()+" "+updatedView);
		
		return updatedView;
	}
	
	private boolean viewContains(final Hash proposal, final QuorumCertificate view)
	{
		if (view.getCurrent().equals(proposal))
			return true;
		
		if (view.getPrevious().equals(proposal))
			return true;

		if (view.getCommittable().equals(proposal))
			return true;
		
		final long proposalHeight = Block.toHeight(proposal);
		if (proposalHeight == 0)
			return true;

		// Create the current view branch
		final Set<Hash> currentBranch = buildCanonicalViewBranch(view);

		// Ensure proposal referenced is present in the current view branch
		if (currentBranch.contains(proposal) == false)
		{
			// Committed? 
			try
			{
				final Hash committed = this.context.getLedger().getLedgerStore().getSyncBlock(proposalHeight);
				if (proposal.equals(committed) == false)
					return false;
			}
			catch (IOException ioex)
			{
				blocksLog.error(this.context.getName()+": Failed to retreive committed proposal hash at height "+proposalHeight, ioex);
				return false;
			}
		}
		
	    return true;	
	}

	private boolean verifyView(final ProgressRound round, final QuorumCertificate view, final QuorumCertificate candidate)
	{
		if (view.getCurrent().equals(candidate.getCurrent()))
			return true;
		
		// Not genesis comparison
		if (candidate.getHeight() > 0)
		{
			// Ensure there is a branch with candidate view `current` which has been extended at least one round
			synchronized(this.pendingBranches)
			{
				final boolean intersectsToRound = this.pendingBranches.stream().anyMatch(b -> {
					if (b.contains(candidate.getCurrent()) == false && b.getRoot().getHash().equals(candidate.getCurrent()) == false)
						return false;
				 
					if (round.clock() > Block.toHeight(candidate.getCurrent()))
					{
						if (b.getBlockAtHeight(Block.toHeight(candidate.getCurrent())+1) == null)
							return false;
					}
				 
					return true;
				});
				
				if (intersectsToRound == false)
				{
					blocksLog.warn(this.context.getName()+": Candidate view QC does not intersect to progress round "+round.clock()+" "+candidate);
					return false;
				}
			}
		}
		
		// Ensure `previous` proposal referenced is present in the current view branch
		if (viewContains(candidate.getPrevious(), view) == false)
		{
			blocksLog.warn(this.context.getName()+": Current view branch does not contain candidate `previous` "+Block.toHeight(candidate.getPrevious())+":"+candidate.getPrevious()+" in progress round "+round.clock());
			return false;
		}
		
		// Ensure `committable` proposal referenced is present in the current view branch
		if (viewContains(candidate.getCommittable(), view) == false)
		{
			blocksLog.warn(this.context.getName()+": Current view branch does not contain candidate `committable` "+Block.toHeight(candidate.getCommittable())+":"+candidate.getCommittable()+" in progress round "+round.clock());
			return false;
		}

	    return true;	
	}
	
	private void updateCommittable(final long roundClock, final QuorumCertificate view, final BlockHeader head)
	{
		final List<BlockHeader> headersToInspect = new ArrayList<BlockHeader>(this.headersToVerify.size());
		BlockHandler.this.headersToVerify.forEach((h, bh) -> {
			if (bh.getHeight() > roundClock)
				headersToInspect.add(bh);
		});

		if (headersToInspect.isEmpty() == false)
		{
			headersToInspect.sort((b1, b2) -> Longs.compare(b1.getHeight(), b2.getHeight()));
			
			// Create the progress view branch
			final Set<Hash> currentBranch = buildCanonicalViewBranch(view);

			for(final BlockHeader headerToInspect : headersToInspect)
			{
				if (Block.toHeight(headerToInspect.getView().getCommittable()) <= head.getHeight())
					continue;

				final PendingBlock committableBlock = this.pendingBlocks.get(headerToInspect.getView().getCommittable());
				if (committableBlock == null)
					continue;
				
				// Already committable?
				if (committableBlock.isCommittable())
					continue;
				
				// Ensure `committable` proposal referenced is present in the current view branch
				if (currentBranch.contains(committableBlock.getHash()) == false)
					continue;

				// Set as a SOFT super if not already
				if (committableBlock.isSuper() == PendingBlock.SUPR.INTR)
					committableBlock.setSuper(SUPR.SOFT);
					
				committableBlock.setCommittable();
			
				if (blocksLog.hasLevel(Logging.INFO))
					blocksLog.info(this.context.getName()+": Pending block "+committableBlock.getHeight()+":"+committableBlock.getHash()+" is now a committable super from future view QC inspection "+headerToInspect.getHeight()+":"+headerToInspect.getHash()+" "+headerToInspect.getView());
				
				break;
			}
		}
	}
	
	private void verifyCommittable(final long roundClock, final QuorumCertificate view, final BlockHeader head)
	{
		// Check the status of the views head, tag or upgrade as hard super and committable if a 2-chain is witnessed
		// TODO 2-chain detection
		// TODO canonical branch
		if (head.getHeight() < Block.toHeight(view.getCommittable()))
		{
			final PendingBlock committableBlock = this.pendingBlocks.get(view.getCommittable());
			if (committableBlock != null)
			{
				if (committableBlock.isCommittable() == false)
				{
					if (committableBlock.isSuper() == PendingBlock.SUPR.INTR)
						committableBlock.setSuper(SUPR.SOFT);
					
					committableBlock.setCommittable();
			
					if (blocksLog.hasLevel(Logging.INFO))
						blocksLog.info(this.context.getName()+": Pending block `commitable` is now a committable super from view QC inspection "+committableBlock.toString()+" during progress round "+progressClock);
				}
			}
		}
	}

	private Set<Hash> buildCanonicalViewBranch(final QuorumCertificate view)
	{
		final List<Hash> viewBranch = new ArrayList<Hash>();
		viewBranch.add(view.getCurrent());
		
		PendingBlock proposal = this.pendingBlocks.get(view.getCurrent());
		while(proposal != null)
		{
			if (proposal.getHeight() == 1)
				viewBranch.add(Hash.ZERO);
			else
				viewBranch.add(proposal.getHeader().getPrevious());

			proposal = this.pendingBlocks.get(proposal.getHeader().getPrevious());
		}
		Collections.reverse(viewBranch);
		return new LinkedHashSet<Hash>(viewBranch);
	}
	
	private BlockVoteStatus process(final BlockVote blockVote, final BlockHeader head) throws IOException, CryptoException, ValidationException
	{
		Objects.requireNonNull(blockVote, "Block vote is null");

		// TODO Should be pre-verified, check 

		// Stale?
		if (blockVote.getHeight() <= head.getHeight())
		{
			if (blocksLog.hasLevel(Logging.DEBUG))
				blocksLog.warn(BlockHandler.this.context.getName()+": Removed stale delayed block vote "+blockVote);
			
			return BlockVoteStatus.STALE;
		}

		// Progress round is not available yet, postpone
		final ProgressRound voteRound = getProgressRound(blockVote.getHeight(), false);
		if (voteRound == null)
		{
			if (blocksLog.hasLevel(Logging.DEBUG))
				blocksLog.warn(this.context.getName()+": Block vote "+blockVote.getHash()+" is postponed for block "+blockVote.getHeight()+"@"+blockVote.getBlock()+" by "+blockVote.getOwner());
			
			return BlockVoteStatus.POSTPONED;
		}
		
		// Apply the vote to the progress round.  
		// The vote should be applied even if the pending block is not yet know to ensure responsiveness and liveness of progress rounds 
		if (voteRound.hasVoted(blockVote.getOwner().getIdentity()))
		{
			// TODO penalty
			blocksLog.warn(BlockHandler.this.context.getName()+": Progress vote "+blockVote.getHash()+" already seen in progress round "+voteRound.clock()+" for "+blockVote.getOwner());
			return BlockVoteStatus.SKIPPED;
		}
		
		final PendingBlock pendingBlock = this.pendingBlocks.get(blockVote.getBlock());
		
		// Postpone applying to the pending block if it is not yet known
		if (pendingBlock == null)
		{
			if (blocksLog.hasLevel(Logging.DEBUG))
				blocksLog.warn(this.context.getName()+": Block vote "+blockVote.getHash()+" is postponed for block "+blockVote.getHeight()+"@"+blockVote.getBlock()+" by "+blockVote.getOwner());
			
			return BlockVoteStatus.POSTPONED;
		}

		// Pending block is known but not yet in a branch, postpone
		if (pendingBlock.isUnbranched())
		{
			if (blocksLog.hasLevel(Logging.DEBUG))
				blocksLog.warn(this.context.getName()+": Block vote "+blockVote.getHash()+" is postponed for unbranched block "+blockVote.getHeight()+"@"+blockVote.getBlock()+" by "+blockVote.getOwner());
			
			return BlockVoteStatus.POSTPONED;
		}
		
		try
		{
			voteRound.vote(blockVote);
			if (blocksLog.hasLevel(Logging.INFO))
				blocksLog.info(BlockHandler.this.context.getName()+": "+blockVote.getOwner().toString(12)+" voted on block "+blockVote.getHeight()+":"+blockVote.getBlock()+" "+blockVote.getHash());
		}
		finally
		{
			this.context.getMetaData().increment("ledger.blockvote.processed");
			this.context.getMetaData().increment("ledger.blockvote.latency", blockVote.getAge(TimeUnit.MILLISECONDS));
		}

		return BlockVoteStatus.SUCCESS;
	}
	
	/** Local instance voting on progress round and proposal branch
	 * 
	 * @param round The progress round for this vote
	 * @param branch The proposal branch to vote on
	 * @return
	 * 
	 * @throws IOException
	 * @throws CryptoException
	 * @throws ValidationException
	 */
	private BlockVote vote(final ProgressRound round, final PendingBranch branch) throws IOException, CryptoException, ValidationException
	{
		if (branch.isEmpty())
			throw new IllegalStateException("Branch is empty "+branch);
		
		if (round.clock() != this.progressClock.get())
			throw new ValidationException("Attempted to vote on progress round "+round.clock()+" but vote clock is "+this.progressClock.get());

		if (round.hasVoted(this.context.getNode().getIdentity()))
		{
			blocksLog.warn(this.context.getName()+": Progress vote is already cast in progress round "+round.clock()+" by "+this.context.getNode().getIdentity());
			return null;
		}
		
		final PendingBlock pendingBlock = branch.getBlockAtHeight(round.clock());
		if (pendingBlock == null)
		{
			blocksLog.warn(this.context.getName()+": No proposal available at progress round "+round.clock()+" to vote on in branch "+branch);
			return null;
		}
		
		if (pendingBlock.isApplied() == false)
			return null;

		if (pendingBlock.isConstructed() == false)
			return null;
		
		long votePower = branch.getVotePower(round.clock(), this.context.getNode().getIdentity());
		if (votePower == 0)
			return null;

		final BlockVote blockVote = new BlockVote(pendingBlock.getHash(), round.startedAt(), this.context.getNode().getIdentity().getKey());
		blockVote.sign(this.context.getNode().getKeyPair());
		if (this.context.getLedger().getLedgerStore().store(blockVote).equals(OperationStatus.SUCCESS))
		{
			this.votesToVerify.put(blockVote.getHash(), blockVote);
			this.blockProcessor.signal();

			final ShardGroupID localShardGroupID = ShardMapper.toShardGroup(this.context.getNode().getIdentity(), this.context.getLedger().numShardGroups());
			if (BlockHandler.this.context.getNetwork().getGossipHandler().broadcast(blockVote, localShardGroupID) == false)
				blocksLog.warn(BlockHandler.this.context.getName()+": Failed to broadcast own block vote "+blockVote);

			if (blocksLog.hasLevel(Logging.INFO))
			{
				if (pendingBlock.getHeader().getProposer().equals(this.context.getNode().getIdentity()))
					blocksLog.info(BlockHandler.this.context.getName()+": Voted on own block "+pendingBlock+" "+blockVote.getHash());
				else
					blocksLog.info(BlockHandler.this.context.getName()+": Voted on block "+pendingBlock+" "+blockVote.getHash());
			}
			
			return blockVote;
		}

		// TODO handle better?
		blocksLog.error(BlockHandler.this.context.getName()+": Vote on block "+pendingBlock+" failed");
		return null;
	}
	
	private void prebuild(final ProgressRound round, final ProgressRound.State roundPhase, final QuorumCertificate view)
	{
		// TODO This should actually throw, but complex to handle
		if (this.buildClock.get() > round.clock())
			return;
		
		// If the local instances shard is behind, don't build
		// TODO with views
//		if (progressRound.driftClock() > 0)
//			return;
		
		boolean build = round.canPropose(this.context.getNode().getIdentity()) == true;

		if (this.context.getConfiguration().get("ledger.liveness.recovery", Boolean.FALSE) == Boolean.TRUE && 
			round.getState().equals(ProgressRound.State.TRANSITION))
			build = false;

		if (this.context.getConfiguration().has("ledger.faults.produce_unbuildable_branches_every"))
		{
			int interval = this.context.getConfiguration().get("ledger.faults.produce_unbuildable_branches_every", -1);
			if (interval > 0 && round.clock() % interval == 0)
				build = false;
		}
		
		if (build == true && this.buildLock == false)
		{
			try
			{
				final List<PendingBlock> generatedBlocks = build(round, view);
				if (generatedBlocks != null)
				{
					final ShardGroupID localShardGroupID = ShardMapper.toShardGroup(this.context.getNode().getIdentity(), this.context.getLedger().numShardGroups());
					for (final PendingBlock generatedBlock : generatedBlocks)
					{
						if (this.context.getLedger().getLedgerStore().store(generatedBlock.getHeader()).equals(OperationStatus.SUCCESS))
						{
							final BlockInsertStatus status = insert(generatedBlock);
							if (status != BlockInsertStatus.SUCCESS)
								throw new IllegalStateException("Failed to insert generated proposal "+generatedBlock+" into pennding blocks");
							
							if (this.context.getNetwork().getGossipHandler().broadcast(generatedBlock.getHeader(), localShardGroupID) == false)
								blocksLog.warn(this.context.getName()+": Failed to broadcast generated block header "+generatedBlock.getHeader());
						}
						else
						{
							blocksLog.error(BlockHandler.this.context.getName()+": Failed to store generated proposal on "+generatedBlock.getHeader());
							break;
						}
					}
	
					this.blockProcessor.signal();
				}
			}
			catch (Exception ex)
			{
				blocksLog.error(this.context.getName()+": Failed to build proposal on "+round, ex);
			}
		}
		else if (build && this.buildLock)
			blocksLog.warn(this.context.getName()+": Build lock is set: Skipping build on proposal "+round);
	}

	private List<PendingBlock> build(final ProgressRound round, final QuorumCertificate view) throws IOException, LockException, ValidationException, CryptoException
	{
		Objects.requireNonNull(round, "Progress round is null");
		
		if (round.getState().equals(ProgressRound.State.COMPLETED))
			blocksLog.warn(this.context.getName()+": Building on progress round "+round.clock()+" which is COMPLETED");
		
		if (this.buildClock.get() > round.clock())
			throw new IllegalStateException("Build clock is "+this.buildClock.get()+" but progress round is "+round);
		
		final Entry<BlockHeader, StateAccumulator> ledgerState = BlockHandler.this.context.getLedger().current();

		// Get the round to be build on (current-1)
		final ProgressRound buildRound = this.progressRounds.get(round.clock()-1);
		if (buildRound == null)
			throw new IllegalStateException("Build round "+(round.clock()-1)+" is not found");
		
		// Select a branch to extend
		BlockHeader buildableHeader;
		PendingBranch selectedBranch = selectBranchToExtend(buildRound, view);
		if (selectedBranch == null)
		{
			// Special case for Genesis
			if (ledgerState.getKey().getHeight() == 0 && this.pendingBranches.size() == 1)
			{
				selectedBranch = new PendingBranch(this.context, ledgerState.getKey(), ledgerState.getValue(), Collections.emptyList());
				buildableHeader = selectedBranch.getRoot();
			}
			else
			{
				blocksLog.warn(this.context.getName()+": No branch selected at "+buildRound.clock()+" with ledger head "+ledgerState.getKey());
	
				// TODO secondaries?
				
				return null;
			}
		}
		else
		{
			final PendingBlock buildableBlock = selectedBranch.getBlockAtHeight(buildRound.clock());
			if (buildableBlock.isConstructed())
				buildableHeader = buildableBlock.getHeader();
			else
				buildableHeader = null;
		}
		
		if (buildableHeader == null)
		{
			blocksLog.warn(this.context.getName()+": No buildable header available at "+buildRound.clock()+" on selected branch "+selectedBranch);
			return null;
		}

		if (ledgerState.getKey().equals(selectedBranch.getRoot()) == false)
			throw new IllegalStateException("Build branch is stale when preparing, ledger head has changed from expected "+selectedBranch.getRoot()+" to "+ledgerState.getKey());

		final PendingBranch buildableBranch = new PendingBranch(this.context, ledgerState.getKey(), ledgerState.getValue(), selectedBranch.getBlocksTo(buildableHeader.getHash()));
		if (buildableBranch.isBuildable() == false)
			throw new IllegalStateException("Build branch is not buildable "+buildableBranch);
		
		long buildClock = buildableHeader.getHeight();
		final List<PendingBlock> generatedBlocks = new ArrayList<>();
		while(buildClock < round.clock())
		{
			final PendingBlock generatedBlock = this.blockBuilder.build(buildableHeader, buildableBranch, ledgerState.getKey(), view);
			if (generatedBlock == null)
				throw new IllegalStateException("Failed to build all required blocks in branch "+buildableBranch);

			buildableBranch.add(generatedBlock);
			buildableBranch.update();
			if (buildableBranch.isBuildable(generatedBlock.getHeader()) == false)
				throw new IllegalStateException("Generated block is not applied after insert "+generatedBlock+" into branch "+buildableBranch);
				
			buildableHeader = generatedBlock.getHeader();
			generatedBlocks.add(new PendingBlock(generatedBlock));
			buildClock++;
		}
		
		this.blockProcessor.signal();
			
		if (blocksLog.hasLevel(Logging.INFO))
		{
			for (PendingBlock generatedBlock : generatedBlocks)
				blocksLog.info(BlockHandler.this.context.getName()+": Generated block "+generatedBlock.getHeader());
		}
		
		return generatedBlocks;
	}
	
	private LinkedList<PendingBlock> commit(final PendingBlock block, final PendingBranch branch)
	{
		Objects.requireNonNull(block, "Pending block is null");
		Objects.requireNonNull(branch, "Pending branch is null");
		
		final LinkedList<PendingBlock> proposalsToCommit = branch.getBlocksTo(block.getHash());
		if (proposalsToCommit.isEmpty())
			return proposalsToCommit;
		
		if (blocksLog.hasLevel(Logging.INFO))
			blocksLog.info(this.context.getName()+":  Attempting commit "+proposalsToCommit.getFirst().getHash()+(proposalsToCommit.size() == 1 ? "" : " -> "+proposalsToCommit.getLast().getHash()));
		
		final LinkedList<PendingBlock> proposalsCommitted = new LinkedList<PendingBlock>();

		// Attempt the commit
		// TODO Might need to catch exceptions on these from synchronous listeners
		for (PendingBlock proposalToCommit : proposalsToCommit)
		{
			if (this.guardLock.tryLock())
			{
				try
				{
					BlockCommitEvent blockCommitEvent = new BlockCommitEvent(proposalToCommit);
					BlockHandler.this.context.getEvents().post(blockCommitEvent);
					proposalsCommitted.add(proposalToCommit);
				}
				finally
				{
					this.guardLock.unlock();
				}
			}
			else
			{
				blocksLog.warn(this.context.getName()+": Failed to acquire commit guard lock on "+proposalToCommit);
				break;
			}
		}
		
		if (proposalsCommitted.isEmpty())
			return proposalsCommitted;
		
		branch.committed(proposalsCommitted.getLast());
			
		// Discover blocks that can't be processed anymore due to this commit
		Set<PendingBlock> pendingBlocksToRemove = Sets.mutable.withInitialCapacity(this.pendingBlocks.size());
		Set<PendingBlock> invalidPendingBlocks = Sets.mutable.withInitialCapacity(this.pendingBlocks.size());
		synchronized(this.pendingBlocks)
		{
			for (PendingBlock pendingBlock : this.pendingBlocks.values())
			{
				if (pendingBlock.getHeight() <= proposalsCommitted.getLast().getHeight())
					continue;

				for (PendingBlock proposalToCommit : proposalsCommitted)
				{
					if (pendingBlock.intersects(proposalToCommit))
					{
						invalidPendingBlocks.add(pendingBlock);
						break;
					}
				}
					
				if (invalidPendingBlocks.contains(pendingBlock))
				{
					if (blocksLog.hasLevel(Logging.DEBUG))
						blocksLog.debug(context.getName()+": Pending block "+pendingBlock+" will be discarded as no longer validatable");
				}
			}
			
			pendingBlocksToRemove.addAll(invalidPendingBlocks);
			for (PendingBlock pendingBlock : this.pendingBlocks.values())
			{
				if (pendingBlock.getHeight() > proposalsCommitted.getLast().getHeight())
					continue;
				
				pendingBlocksToRemove.add(pendingBlock);
			}
		}
		
		synchronized(this.pendingBranches)
		{
			// Trim pending branches against new ledger head
			Iterator<PendingBranch> pendingBranchIterator = BlockHandler.this.pendingBranches.iterator();
			while(pendingBranchIterator.hasNext())
			{
				PendingBranch pendingBranch = pendingBranchIterator.next();
				
				if (pendingBranch.isEmpty() == false && pendingBranch.getLow().getHeight() <= proposalsCommitted.getLast().getHeight())
					pendingBranch.trimTo(proposalsCommitted.getLast().getHeader());
			}
		
			// Trim pending branches against redundant blocks
			pendingBranchIterator = BlockHandler.this.pendingBranches.iterator();
			while(pendingBranchIterator.hasNext())
			{
				PendingBranch pendingBranch = pendingBranchIterator.next();
				for (PendingBlock invalidPendingBlock : invalidPendingBlocks)
				{
					if (pendingBranch.contains(invalidPendingBlock) == false)
						continue;
					
					Collection<PendingBlock> trimmed = pendingBranch.trimFrom(proposalsCommitted.getLast().getHeader());
					pendingBlocksToRemove.addAll(trimmed);

					if (pendingBranch.isEmpty())
						break;
				}
			}
			
			// Remove all empty branches (except commit branch, keep it as next reference branch)
			pendingBranchIterator = BlockHandler.this.pendingBranches.iterator();
			while(pendingBranchIterator.hasNext())
			{
				PendingBranch pendingBranch = pendingBranchIterator.next();

				if (pendingBranch.equals(branch))
					continue;
						
				if(pendingBranch.isEmpty() || pendingBranch.getRoot().equals(proposalsCommitted.getLast().getHeader()) == false)
					pendingBranchIterator.remove();
			}
		}
		
		if (blocksLog.hasLevel(Logging.DEBUG))
			blocksLog.debug(this.context.getName()+": Removing "+pendingBlocksToRemove.size()+" pending blocks containing "+invalidPendingBlocks.size()+" invalid blocks post commit of head "+proposalsCommitted.getLast().toString());
		
		for (final PendingBlock pendingBlock : pendingBlocksToRemove)
		{
			this.pendingBlocks.remove(pendingBlock.getHash(), pendingBlock);

			if (blocksLog.hasLevel(Logging.DEBUG))
				blocksLog.debug(this.context.getName()+": Removed "+pendingBlock+" post commit of head "+proposalsCommitted.getLast().toString());

			if (pendingBlock.isConstructed() == true)
			{
				this.context.getMetaData().increment("ledger.block.processed");
				this.context.getMetaData().increment("ledger.block.latency", pendingBlock.getBlock().getAge(TimeUnit.MILLISECONDS));
			}
		}
		
		for (final PendingBlock proposalCommitted : proposalsCommitted)
		{
			if (this.progressRounds.remove(proposalCommitted.getHeight()) == null)
				blocksLog.warn(this.context.getName()+": Progress round "+proposalCommitted.getHeight()+" not removed when committing proposal "+proposalCommitted);
			
			this.buildClock.updateAndGet(v -> v < proposalCommitted.getHeight() ? proposalCommitted.getHeight() : v);
		}
		
		return proposalsCommitted;
	}
	
	public Collection<BlockHeader> getPendingHeaders()
	{
		synchronized(this.headersToVerify)
		{
			final List<BlockHeader> pendingHeaders = new ArrayList<BlockHeader>(this.headersToVerify.values());
			pendingHeaders.sort((bh1, bh2) -> {
				if (bh1.getHeight() < bh2.getHeight())
						return -1;
					else if (bh1.getHeight() > bh2.getHeight())
						return 1;
					
				return bh1.getHash().compareTo(bh2.getHash());
			});
			return pendingHeaders;
		}
	}

	public Collection<PendingBranch> getPendingBranches()
	{
		synchronized(this.pendingBranches)
		{
			final List<PendingBranch> pendingBranches = new ArrayList<PendingBranch>(this.pendingBranches);
			pendingBranches.sort((pb1, pb2) -> pb1.getHigh().getHash().compareTo(pb2.getHigh().getHash()));
			return pendingBranches;
		}
	}
	
	public Collection<PendingBlock> getPendingBlocks()
	{
		synchronized(this.pendingBlocks)
		{
			final List<PendingBlock> pendingBlocks = new ArrayList<PendingBlock>(this.pendingBlocks.values());
			pendingBlocks.sort((pb1, pb2) -> {
				if (pb1.getHeight() < pb2.getHeight())
					return -1;
				else if (pb1.getHeight() > pb2.getHeight())
					return 1;
				
				return pb1.getHash().compareTo(pb2.getHash());
			});
			return pendingBlocks;
		}
	}
	
	BlockInsertStatus insert(final BlockHeader header) throws IOException, ValidationException, CryptoException
	{
		Objects.requireNonNull(header, "Block header is null");
		
		final Epoch epoch = Epoch.from(header);
		final int numShardGroups = this.context.getLedger().numShardGroups(epoch);
		final ShardGroupID blockShardGroupID = ShardMapper.toShardGroup(header.getProposer(), numShardGroups);
		final ShardGroupID localShardGroupID = ShardMapper.toShardGroup(this.context.getNode().getIdentity(), numShardGroups);
		if (blockShardGroupID.equals(localShardGroupID) == false)
		{
			blocksLog.warn(this.context.getName()+": Block header is for shard group ID "+blockShardGroupID+" but expected local shard group ID "+localShardGroupID);
			// TODO disconnect and ban;
			return BlockInsertStatus.FAILED;
		}
		
		if (header.getHeight() <= this.context.getLedger().getHead().getHeight())
		{
			if (blocksLog.hasLevel(Logging.DEBUG))
				blocksLog.debug(this.context.getName()+": Block header is stale "+header);
			
			return BlockInsertStatus.STALE;
		}
		
		if (this.pendingBlocks.get(header.getHash()) != null)
		{
			blocksLog.warn(this.context.getName()+": Block header "+header+" is already known");
			return BlockInsertStatus.SKIPPED;
		}
		
		if (header.verify(header.getProposer().getKey()) == false)
			throw new ValidationException(header, "Signature is invalid for block header");
		
		if (this.guardLock.tryLock())
		{
			try
			{
				validate(header);
			}
			finally
			{
				this.guardLock.unlock();
			}
		}
		else
			return BlockInsertStatus.POSTPONED;

		final PendingBlock pendingBlock = new PendingBlock(this.context, header);
		if (this.pendingBlocks.putIfAbsent(pendingBlock.getHash(), pendingBlock) != null)
			throw new IllegalStateException("Pending block "+pendingBlock.getHash()+" is already inserted");
		
		if (blocksLog.hasLevel(Logging.DEBUG))
			blocksLog.debug(this.context.getName()+": Inserted pending block "+header.toString());
		
		return BlockInsertStatus.SUCCESS;
	}

	/**
	 * Inserts blocks generated by the local validator.
	 * 
	 * Should be private but is visible for testing
	 * 
	 * @param pendingBlock
	 * @throws IOException 
	 * @throws CryptoException 
	 * @throws ValidationException 
	 */
	@VisibleForTesting
	BlockInsertStatus insert(final PendingBlock pendingBlock) throws CryptoException, ValidationException, IOException
	{
		Objects.requireNonNull(pendingBlock, "Pending block is null");
		if (pendingBlock.getHeader() == null)
			throw new IllegalStateException("Pending block "+pendingBlock.getHash()+" does not have a header");
		
		if (pendingBlock.isConstructed() == false)
			throw new IllegalStateException("Pending block "+pendingBlock.getHash()+" does not have a constructed block");
		
		if (pendingBlock.getHeader().getProposer().equals(this.context.getNode().getIdentity()) == false)
			throw new IllegalStateException("Pending block "+pendingBlock.getHash()+" is not generated by local validator "+this.context.getNode().getIdentity());
		
		if (pendingBlock.getHeight() <= this.context.getLedger().getHead().getHeight())
		{
			blocksLog.warn(this.context.getName()+": Built stale proposal "+pendingBlock.toString());
			blocksLog.warn(this.context.getName()+":     Head is now "+this.context.getLedger().getHead().toString());
			this.context.getMetaData().increment("ledger.mining.stale");
			return BlockInsertStatus.STALE;
		}
		
		if (1==0)
		{
			if (pendingBlock.getHeader().verify(pendingBlock.getHeader().getProposer().getKey()) == false)
				throw new ValidationException(pendingBlock.getHeader(), "Signature is invalid for block header");
		
			validate(pendingBlock.getHeader());
		}
		
		if (this.progressClock.get() == pendingBlock.getHeight())
		{
			final ProgressRound progressRound = getProgressRound(pendingBlock.getHeight(), false);
	
			final long roundVotePower = this.context.getLedger().getValidatorHandler().getVotePower(progressRound.epoch(), pendingBlock.getHeader().getProposer());
			if (progressRound.propose(pendingBlock.getHeader(), roundVotePower) == false)
//				blocksLog.warn(this.context.getName()+": Progress round "+pendingBlock.getHeight()+" already has a proposal from "+pendingBlock.getHeader().getProposer());
				throw new ValidationException("Progress round "+pendingBlock.getHeight()+" already has a proposal from "+pendingBlock.getHeader().getProposer());
		}

		if (this.pendingBlocks.putIfAbsent(pendingBlock.getHash(), pendingBlock) != null)
			throw new IllegalStateException("Generated block "+pendingBlock.getHash()+" is already inserted");

		if (blocksLog.hasLevel(Logging.DEBUG))
			blocksLog.debug(this.context.getName()+": Inserted pending block "+pendingBlock.getHeader().toString());

		this.blockProcessor.signal();
		return BlockInsertStatus.SUCCESS;
	}
	
	/** Validates that the proposal header does not violate ledger constraints.  
	 * 
	 * TODO improve and test more extensively.  Pending branch validation will catch anything missing here for now
	 * 		but much cheaper to catch as many validation violations here as possible.
	 * 
	 * @throws ValidationException
	 * @throws IOException
	 */
	private void validate(final BlockHeader header) throws ValidationException, IOException
	{
		// TODO CHECK POW
		
		Set<Hash> duplicates = Sets.mutable.empty();
		Set<Hash> exclusions = Sets.mutable.empty();
		
		// Batch fetch all possible commits from store (10x more efficient)
		final Map<Hash, StateAddress> stateAddresses = Maps.mutable.ofInitialCapacity(header.getTotalInventorySize());
		header.getInventory(InventoryType.ACCEPTED).forEach(h -> stateAddresses.put(h, StateAddress.from(Atom.class, h)));
		header.getInventory(InventoryType.UNACCEPTED).forEach(h -> stateAddresses.put(h, StateAddress.from(Atom.class, h)));
		header.getInventory(InventoryType.UNEXECUTED).forEach(h -> stateAddresses.put(h, StateAddress.from(ExecutionTimeout.class, h)));
		header.getInventory(InventoryType.UNCOMMITTED).forEach(h -> stateAddresses.put(h, StateAddress.from(CommitTimeout.class, h)));
		header.getInventory(InventoryType.COMMITTED).forEach(h -> stateAddresses.put(h, StateAddress.from(AtomCertificate.class, h)));
		header.getInventory(InventoryType.PACKAGES).forEach(h -> stateAddresses.put(h, StateAddress.from(PolyglotPackage.class, h)));
		
		// TODO large optimization possibility here as many of the primitives which constitute this block can be validated 
		//		without having to touch the store.  For example if an atom in the inventory is valid, then it should be pending
		//		in the atom handler if the local instance knows about it.
		final Map<StateAddress, SubstateCommit> primitiveSubstateCommits = this.context.getLedger().getLedgerStore().search(stateAddresses.values());
		
		// Check certificates
		for (Hash certificate : header.getInventory(InventoryType.COMMITTED))
		{
			if (duplicates.contains(certificate))
				throw new ValidationException(certificate, "Atom certificate "+certificate+" is duplicated in block "+header);
	
			SubstateCommit substateCommit = primitiveSubstateCommits.get(stateAddresses.get(certificate));
			if (substateCommit != null)
			{
				if (substateCommit.getSubstate().get(NativeField.CERTIFICATE) != null)
					throw new ValidationException(certificate, "Atom certificate exists for "+certificate+" and is committed");
					
				// TODO how to check this with certificates?
				// if (commit.getPath().get(Element.TIMEOUT) != null)
				//     throw new ValidationException(atom, "Atom "+atom+" is already timed out by "+commit.getPath().get(Element.TIMEOUT));

				exclusions.add(substateCommit.getSubstate().get(NativeField.ATOM));
			}
			
			duplicates.add(certificate);
			exclusions.add(certificate);
		}
		duplicates.clear();
			
		// Check unexecuted
		for (Hash timeout : header.getInventory(InventoryType.UNEXECUTED))
		{
			if (duplicates.contains(timeout))
				throw new ValidationException(timeout, "Atom execution timeout "+timeout+" is duplicated in block "+header);

			if (exclusions.contains(timeout))
				throw new ValidationException(timeout, "Atom execution timeout "+timeout+" can not be included in block "+header);
			
			SubstateCommit primitiveSubstateCommit = primitiveSubstateCommits.get(stateAddresses.get(timeout));
			if (primitiveSubstateCommit != null)
			{
				Hash atom = primitiveSubstateCommit.getSubstate().get(NativeField.ATOM);
				if (exclusions.contains(atom))
					throw new ValidationException(atom, "Atom "+atom+" is already execution timedout");

				if (primitiveSubstateCommit.getSubstate().get(NativeField.TIMEOUT) != null)
					throw new ValidationException(atom, "Atom "+atom+" is already timed out by "+primitiveSubstateCommit.getSubstate().get(NativeField.TIMEOUT));

				exclusions.add(atom);
			}
			
			duplicates.add(timeout);
			exclusions.add(timeout);
		}
		duplicates.clear();

		// Check uncommitted
		for (Hash timeout : header.getInventory(InventoryType.UNCOMMITTED))
		{
			if (duplicates.contains(timeout))
				throw new ValidationException(timeout, "Atom commit timeout "+timeout+" is duplicated in block "+header);

			if (exclusions.contains(timeout))
				throw new ValidationException(timeout, "Atom commit timeout "+timeout+" can not be included in block "+header);
			
			SubstateCommit primitiveSubstateCommit = primitiveSubstateCommits.get(stateAddresses.get(timeout));
			if (primitiveSubstateCommit != null)
			{
				Hash atom = primitiveSubstateCommit.getSubstate().get(NativeField.ATOM);
				if (exclusions.contains(atom))
					throw new ValidationException(atom, "Atom "+atom+" can not be commit timedout in block "+header);
				
				if (primitiveSubstateCommit.getSubstate().get(NativeField.TIMEOUT) != null)
					throw new ValidationException(atom, "Atom "+atom+" is already timed out by "+primitiveSubstateCommit.getSubstate().get(NativeField.TIMEOUT));

				exclusions.add(atom);
			}
			
			duplicates.add(timeout);
			exclusions.add(timeout);
		}
		duplicates.clear();

		// Check accepted
		for (Hash accepted : header.getInventory(InventoryType.ACCEPTED))
		{
			if (duplicates.contains(accepted))
				throw new ValidationException(accepted, "Atom "+accepted+" is accept duplicated in block "+header);
			
			if (exclusions.contains(accepted))
				throw new ValidationException(accepted, "Atom "+accepted+" can not be accepted in block "+header);
			
			SubstateCommit primitiveSubstateCommit = primitiveSubstateCommits.get(stateAddresses.get(accepted));
			if (primitiveSubstateCommit != null)
			{
				if (primitiveSubstateCommit.getSubstate().get(NativeField.CERTIFICATE) != null)
					throw new ValidationException(accepted, "Atom "+accepted+" has a certificate "+primitiveSubstateCommit.getSubstate().get(NativeField.CERTIFICATE)+" and is accepted in proposal "+Block.toHeight(primitiveSubstateCommit.getSubstate().<Hash>get(NativeField.BLOCK))+":"+primitiveSubstateCommit.getSubstate().get(NativeField.BLOCK));

				if (primitiveSubstateCommit.getSubstate().get(NativeField.BLOCK) != null)
					throw new ValidationException(accepted, "Atom "+accepted+" is already accepted in proposal "+Block.toHeight(primitiveSubstateCommit.getSubstate().<Hash>get(NativeField.BLOCK))+":"+primitiveSubstateCommit.getSubstate().get(NativeField.BLOCK));

				if (primitiveSubstateCommit.getSubstate().get(NativeField.TIMEOUT) != null)
					throw new ValidationException(accepted, "Atom "+accepted+" has a timeout "+primitiveSubstateCommit.getSubstate().get(NativeField.TIMEOUT)+" and is accepted in proposal "+Block.toHeight(primitiveSubstateCommit.getSubstate().<Hash>get(NativeField.BLOCK))+":"+primitiveSubstateCommit.getSubstate().get(NativeField.BLOCK));
			}
			
			exclusions.add(accepted);
			duplicates.add(accepted);
		}
		duplicates.clear();

		// Check unaccepted
		for (Hash unaccepted : header.getInventory(InventoryType.UNACCEPTED))
		{
			if (duplicates.contains(unaccepted))
				throw new ValidationException(unaccepted, "Atom "+unaccepted+" is duplicated unaccepted in block "+header);

			if (exclusions.contains(unaccepted))
				throw new ValidationException(unaccepted, "Atom "+unaccepted+" can not be unaccepted in block "+header);
			
			SubstateCommit primitiveSubstateCommit = primitiveSubstateCommits.get(stateAddresses.get(unaccepted));
			if (primitiveSubstateCommit != null)
			{
				if (primitiveSubstateCommit.getSubstate().get(NativeField.CERTIFICATE) != null)
					throw new ValidationException(unaccepted, "Atom certificate exists for "+unaccepted+" and is committed");
					
				if (primitiveSubstateCommit.getSubstate().get(NativeField.TIMEOUT) != null)
					throw new ValidationException(unaccepted, "Atom "+unaccepted+" is already timed out by "+primitiveSubstateCommit.getSubstate().get(NativeField.TIMEOUT));
			}
			
			duplicates.add(unaccepted);
			exclusions.add(unaccepted);
		}
		duplicates.clear();

		// SIGNALS
		// Check executables
		for (Hash executable : header.getInventory(InventoryType.EXECUTABLE))
		{
			if (duplicates.contains(executable))
				throw new ValidationException(header, "Atom "+executable+" is executable duplicated in block "+header);
			
			if (exclusions.contains(executable))
				throw new ValidationException(header, "Atom "+executable+" can not be executable in block "+header);

			SubstateCommit primitiveSubstateCommit = primitiveSubstateCommits.get(stateAddresses.get(executable));
			if (primitiveSubstateCommit != null)
			{
				if (primitiveSubstateCommit.getSubstate().get(NativeField.CERTIFICATE) != null)
					throw new ValidationException(header, "Atom certificate exists for "+executable+" and is committed");

				if (primitiveSubstateCommit.getSubstate().get(NativeField.TIMEOUT) != null)
					throw new ValidationException(header, "Atom "+executable+" is already timed out by "+primitiveSubstateCommit.getSubstate().get(NativeField.TIMEOUT));
			}

			exclusions.add(executable);
			duplicates.add(executable);
		}
		duplicates.clear();
		
		// Check latent
		for (Hash latent : header.getInventory(InventoryType.LATENT))
		{
			if (duplicates.contains(latent))
				throw new ValidationException(header, "Atom "+latent+" is latent duplicated in block "+header);
			
			if (exclusions.contains(latent))
				throw new ValidationException(header, "Atom "+latent+" can not be latent in block "+header);

			SubstateCommit primitiveSubstateCommit = primitiveSubstateCommits.get(stateAddresses.get(latent));
			if (primitiveSubstateCommit != null)
			{
				if (primitiveSubstateCommit.getSubstate().get(NativeField.CERTIFICATE) != null)
					throw new ValidationException(header, "Atom certificate exists for "+latent+" and is committed");

				if (primitiveSubstateCommit.getSubstate().get(NativeField.TIMEOUT) != null)
					throw new ValidationException(header, "Atom "+latent+" is already timed out by "+primitiveSubstateCommit.getSubstate().get(NativeField.TIMEOUT));
			}

			exclusions.add(latent);
			duplicates.add(latent);
		}
		duplicates.clear();
		
		// PACKAGES // 
		for (Hash pakage : header.getInventory(InventoryType.PACKAGES))
		{
			if (duplicates.contains(pakage))
				throw new ValidationException(header, "Package "+pakage+" is duplicated in block "+header);
			
			if (exclusions.contains(pakage))
				throw new ValidationException(header, "Package "+pakage+" can not be in block "+header);
			
			SubstateCommit primitiveSubstateCommit = primitiveSubstateCommits.get(stateAddresses.get(pakage));
			if (primitiveSubstateCommit != null)
			{
				if (primitiveSubstateCommit.getSubstate().get(NativeField.BLOCK) != null)
					throw new ValidationException(pakage, "Package "+pakage+" is already accepted in proposal "+Block.toHeight(primitiveSubstateCommit.getSubstate().<Hash>get(NativeField.BLOCK))+":"+primitiveSubstateCommit.getSubstate().get(NativeField.BLOCK));
			}

			exclusions.add(pakage);
			duplicates.add(pakage);
		}
		duplicates.clear();
	}

	
	@VisibleForTesting
	PendingBlock getBlock(Hash block)
	{
		Objects.requireNonNull(block, "Block hash is null");
		Hash.notZero(block, "Block hash is ZERO");

		return this.pendingBlocks.get(block);
	}

	private void updateBranchesWith(final PendingBlock pendingBlock, final BlockHeader head) throws LockException, IOException, ValidationException
	{
		Objects.requireNonNull(head, "Ledger head is null");
		Objects.requireNonNull(pendingBlock, "Pending block is null");
		
		if (pendingBlock.getHeader() == null)
			throw new IllegalStateException("Pending block "+pendingBlock.getHash()+" does not have a header");
		
		if (pendingBlock.isUnbranched() == false)
			throw new IllegalStateException("Pending block "+pendingBlock.getHash()+" is already in a branch");

		PendingBlock currentPendingBlock = pendingBlock;
		LinkedList<PendingBlock> branch = new LinkedList<PendingBlock>();
		while(currentPendingBlock != null)
		{
			// If any vertex in the branch has throw, abort updating entire branch.
			if (currentPendingBlock.thrown() != null)
				return;
			
			branch.add(currentPendingBlock);
			if (currentPendingBlock.getHeader().getPrevious().equals(head.getHash()))
				break;
			
			PendingBlock previous = this.pendingBlocks.get(currentPendingBlock.getHeader().getPrevious());
			if (previous != null && previous.getHeader() != null)
				currentPendingBlock = previous;
			else
				currentPendingBlock = null;
		}
		
		if (branch.isEmpty())
			return;

		if (branch.getLast().getHeader().getPrevious().equals(head.getHash()) == false)
		{
			if (blocksLog.hasLevel(Logging.DEBUG))
				blocksLog.error(BlockHandler.this.context.getName()+": Branch for pending block "+pendingBlock.getHeader()+" does not terminate at ledger head "+head.getHash()+" but at "+branch.getFirst().getHeader());
			
			return;
		}

		Collections.reverse(branch);
		
		synchronized(this.pendingBranches)
		{
			for (PendingBranch pendingBranch : this.pendingBranches)
			{
				if (pendingBranch.isMergable(branch))
					pendingBranch.merge(branch);
			}
		}
		
		// Couldnt merge, check if it forks
		if (pendingBlock.isUnbranched())
		{
			PendingBranch newBranch = null;
			synchronized(this.pendingBranches)
			{
				for (PendingBranch pendingBranch : this.pendingBranches)
				{
					if (pendingBranch.isFork(branch))
					{
						newBranch = pendingBranch.fork(branch);
						break;
					}
				}
			
				if (newBranch == null)
				{
					Entry<BlockHeader, StateAccumulator> current = this.context.getLedger().current();
					newBranch = new PendingBranch(this.context, current.getKey(), current.getValue(), branch);
				}
				
				this.pendingBranches.add(newBranch);
			}
		}
	}
	
	private PendingBranch findHighestWorkBranch(final List<PendingBranch> branches, final long height) 
	{
	    PendingBranch selectedBranch = null;
	    BlockHeader highestWork = null;
	    
	    for (final PendingBranch branch : branches) 
	    {
	        if (height <= branch.getRoot().getHeight())
	            continue;
	        
	        final PendingBlock block = branch.getBlockAtHeight(height);
	        if (block == null)
	        	continue;
	        
	        if (block.isConstructed() == false)
	        	continue;
	        
        	if (branch.isBuildable(block.getHeader()) == false)
        		continue;
	        	
           	final BlockHeader header = block.getHeader();
            if (highestWork == null || header.getTotalWork().compareTo(highestWork.getTotalWork()) > 0) 
            {
                highestWork = header;
                selectedBranch = branch;
	        }
	    }
	    
	    return selectedBranch;
	}
	
	private PendingBranch selectBranch(final ProgressRound round, final QuorumCertificate view, final Predicate<PendingBranch> filter)
	{
		// Filter branches that intersect with the local view 
	    final List<PendingBranch> candidateBranches;
	    synchronized(this.pendingBranches) 
	    {
	        if (this.pendingBranches.isEmpty())
	            throw new IllegalStateException("No pending branches available");
	            
	        candidateBranches = new ArrayList<PendingBranch>();
	        for (final PendingBranch pendingBranch : this.pendingBranches)
	        {
	        	final PendingBlock pendingBlock = pendingBranch.getBlockAtHeight(round.clock());
	        	if (pendingBlock == null)
	        		continue;
	        	
	        	if (filter.test(pendingBranch) == false)
	        		continue;
	        	
	        	candidateBranches.add(pendingBranch);
	        }
	    }
	    
	    if (candidateBranches.isEmpty())
	    {
	    	blocksLog.warn(this.context.getName()+": No suitable branches discovered when selecting on progress round "+round.clock()+" at phase "+round.getState());
	    	return null;
	    }
	    
	    final PendingBranch selectedBranch = findHighestWorkBranch(candidateBranches, round.clock());
	    if (selectedBranch == null)
	    	blocksLog.warn(this.context.getName()+": No branch selected on progress round "+round.clock());
	    return selectedBranch;
	}

	private PendingBranch selectBranchToVote(final ProgressRound round, final QuorumCertificate view) 
	{
		// Only proposal branches in the round which carry a canonical, non-divergent view with the local replica are selected
		return selectBranch(round, view, b -> {
			final PendingBlock candidate = b.getBlockAtHeight(round.clock());
			return verifyView(round, view, candidate.getHeader().getView());
		});
	}

	private PendingBranch selectBranchToExtend(final ProgressRound round, final QuorumCertificate view) 
	{
		final boolean canLockToView;
		// Phase 1
		// Determine if a branch view lock can be applied for this extension 
		synchronized(this.pendingBranches)
		{
			if (Block.toHeight(view.getCurrent()) == 0)
				canLockToView = false;
			else
				canLockToView = this.pendingBranches.stream().anyMatch(b -> b.contains(view.getCurrent()));
		}
		
		// Phase 2
		// If a branch view lock is applied, only return branches which intersect with the view.
		// In the absence of a branch view lock, simply return all branches and let the "branch work" be the decider. 
		// TODO make genesis special cases better 
		return selectBranch(round, view, b -> {
			if (Block.toHeight(view.getCurrent()) == 0)
				return true;

			if (canLockToView == false)
				return true;
			
			if (b.contains(view.getCurrent()))
				return true;
			
			// TODO redundant?
			if (b.getRoot().getHash().equals(view.getCurrent()))
				return true;

			return false;
		});
	}

	private PendingBranch selectBranchWithQuorum(final ProgressRound round) 
	{
	    final BlockHeader head = this.context.getLedger().getHead();

	    int bestScore = 0;
	    int bestLength = 0;
	    PendingBranch selectedBranch = null;

	    synchronized (this.pendingBranches) 
	    {
	        if (this.pendingBranches.isEmpty())
	            throw new IllegalStateException("No pending branches available");

	        for (final PendingBranch pendingBranch : this.pendingBranches) 
	        {
	            try 
	            {
	                if (pendingBranch.isEmpty()) 
	                {
	                    if (pendingBranch.getRoot().getHeight() > 0)
	                        blocksLog.warn(this.context.getName() + ": Unexpected empty branch " + pendingBranch);
	                    continue;
	                }

	                if (pendingBranch.getLow().getHeader().getPrevious().equals(head.getHash()) == false) 
	                {
	                    if (blocksLog.hasLevel(Logging.DEBUG))
	                        blocksLog.debug(this.context.getName() + ": Branch doesn't attach to ledger " + pendingBranch.getLow());
	                    continue;
	                }

	                final PendingBlock commitable = pendingBranch.commitable();
	                if (commitable == null)
	                    continue;

	                int superCount = pendingBranch.supers(SUPR.HARD).size();
	                int branchLength = pendingBranch.size();

	                if (selectedBranch == null || superCount > bestScore ||
	                    (superCount == bestScore && branchLength > bestLength)) 
	                {
	                    selectedBranch = pendingBranch;
	                    bestScore = superCount;
	                    bestLength = branchLength;
	                }
	            } 
	            catch (Exception ex) 
	            {
	                blocksLog.error(this.context.getName() + ": Branch selection with quorum failed on " + pendingBranch, ex);
	            }
	        }

	        if (selectedBranch != null && blocksLog.hasLevel(Logging.INFO))
	            blocksLog.info(this.context.getName() + ": Found branch with quorum " + selectedBranch);
	    }

	    return selectedBranch;
	}
	
	long getTimestampEstimate()
	{
		final BlockHeader head = this.context.getLedger().getHead();

		synchronized(this.pendingBranches)
		{
			if (this.pendingBranches.isEmpty())
				return head.getTimestamp();

			long minHeight = head.getHeight();
			int eligibleBranches = 0;
			for (final PendingBranch pendingBranch : this.pendingBranches)
			{
				synchronized(pendingBranch)
				{
					if (pendingBranch.size() == 0)
						continue;
					
					if (pendingBranch.getHigh().getHeight() <= minHeight)
						continue;
					
					minHeight = Math.min(head.getHeight(), pendingBranch.getHigh().getHeight() - Constants.MIN_COMMIT_SUPERS);
					eligibleBranches++;
				}
			}
			
			if (eligibleBranches == 0)
				return head.getTimestamp();
			
			final List<Long> timestamps = new ArrayList<Long>(eligibleBranches);
			for (final PendingBranch pendingBranch : this.pendingBranches)
			{
				synchronized(pendingBranch)
				{
					if (pendingBranch.size() == 0)
						continue;

					if (pendingBranch.getHigh().getHeight() <= minHeight)
						continue;
					
					timestamps.add(pendingBranch.getHigh().getHeader().getTimestamp());
				}
			}
			Collections.sort(timestamps);
			
			// Get median (middle element)
			int middle = timestamps.size() / 2;
			if (timestamps.size() % 2 == 0)
			    return (timestamps.get(middle - 1) + timestamps.get(middle)) / 2l;
			else
			    return timestamps.get(middle);
		}
	}

	// SYNC PROGRESS LISTENER //
	private EventListener syncProgressListener = new SynchronousEventListener() 
	{
		@Subscribe
		public void on(final ProgressPhaseEvent event)
		{
			if (BlockHandler.this.progressPhaseQueue.offer(event) == false)
				// TODO Anything else to do here other than fail?  Can recover?
				blocksLog.fatal(BlockHandler.this.context.getName()+": Failed to add progress round to queue "+event.getProgressRound());
			
			if (event.getProgressPhase().equals(ProgressRound.State.COMPLETED))
				BlockHandler.this.context.getNode().setDrift(event.getProgressRound().drift());
			
			BlockHandler.this.blockProcessor.signal();
		}
	};
	
	// SYNC BLOCK LISTENER //
	private SynchronousEventListener syncBlockListener = new SynchronousEventListener()
	{
		@Subscribe
		public void on(BlockAppliedEvent blockAppliedEvent)
		{
			BlockHandler.this.blockProcessor.signal();
		}

		@Subscribe
		public void on(BlockConstructedEvent blockConstructedEvent)
		{
			BlockHandler.this.blockProcessor.signal();
		}

		@Subscribe
		public void on(BlockCommittedEvent blockCommittedEvent)
		{
			Set<Hash> headerRemovals = Sets.mutable.withInitialCapacity(BlockHandler.this.headersToVerify.size());
			BlockHandler.this.headersToVerify.forEach((h, bh) -> {
				if (bh.getHeight() <= blockCommittedEvent.getPendingBlock().getHeader().getHeight())
					headerRemovals.add(h);	
			});
			headerRemovals.forEach(h -> BlockHandler.this.headersToVerify.remove(h));
			
			synchronized(BlockHandler.this.blockVoteCollectors)
			{
				final Iterator<BlockVoteCollector> blockVoteCollectorsIterator = BlockHandler.this.blockVoteCollectors.values().iterator();
				while(blockVoteCollectorsIterator.hasNext())
				{
					final BlockVoteCollector blockVoteCollector = blockVoteCollectorsIterator.next();
					if (blockVoteCollector.getHeight() > blockCommittedEvent.getPendingBlock().getHeader().getHeight())
						continue;
					
					blockVoteCollectorsIterator.remove();

					if (blockVoteCollector.hasMetThreshold() == false)
						blocksLog.warn(BlockHandler.this.context.getName()+": Removed incomplete block vote collector for progress round "+blockVoteCollector.getHeight());
					else if (blocksLog.hasLevel(Logging.DEBUG))
						blocksLog.debug(BlockHandler.this.context.getName()+": Removed completed block vote collector for progress round "+blockVoteCollector.getHeight());
				}
			}
			
			if (blockCommittedEvent.getPendingBlock().getHeight() >= BlockHandler.this.reportedLatent)
				BlockHandler.this.reportedLatent = -1;
			
			BlockHandler.this.buildLock = false;
			BlockHandler.this.blockProcessor.signal();
		}
	};

	// ASYNC BLOCK LISTENER //
	private EventListener asyncBlockListener = new EventListener()
	{
		@Subscribe
		public void on(final BlockAppliedEvent blockAppliedEvent)
		{
			if (blocksLog.hasLevel(Logging.DEBUG))
				blocksLog.debug(BlockHandler.this.context.getName()+": Proposal for round "+blockAppliedEvent.getPendingBlock().getHeight()+" has been applied "+blockAppliedEvent.getPendingBlock());
		}
	};
	
	// SYNC CHANGE LISTENER //
	private SynchronousEventListener syncChangeListener = new SynchronousEventListener()
	{
		@Subscribe
		public void on(final SyncAcquiredEvent event) 
		{
			BlockHandler.this.syncLock.writeLock().lock();
			try
			{
				blocksLog.log(BlockHandler.this.context.getName()+": Sync status acquired, setting block handler state");
				BlockHandler.this.pendingBlocks.clear();
				BlockHandler.this.pendingBranches.clear();
				BlockHandler.this.progressPhaseQueue.clear();
				BlockHandler.this.postponedVotes.clear();
				BlockHandler.this.headersToVerify.clear();
				BlockHandler.this.blockVoteCollectors.clear();
				BlockHandler.this.progressRounds.clear();

				// Set the views and clocks from the event head
				BlockHandler.this.progressView = event.getHead().getView(); 
				BlockHandler.this.progressClock.set(event.getHead().getHeight()+1);
				BlockHandler.this.buildClock.set(event.getHead().getHeight());
				BlockHandler.this.buildLock = false;
				
				final ProgressRound progressRound = getProgressRound(BlockHandler.this.progressClock.get(), true);
				progressRound.start(BlockHandler.this.progressView);
				
				blocksLog.info(BlockHandler.this.context.getName()+": Progress round post sync is "+progressRound.toString());
				
				// Directly create and insert the "previous" build round
				final ProgressRound previousRound = new ProgressRound(BlockHandler.this.context, event.getHead());
				BlockHandler.this.progressRounds.put(previousRound.clock(), previousRound);
				
				// TODO meh ... getting head from event, but accumulator from ledger because the accumulator use is referenced based and the 
				// 		event carries a sync accumulator which gets reset and causes all manner of locking issues if used for this initial branch
				BlockHandler.this.pendingBranches.add(new PendingBranch(BlockHandler.this.context, Type.NONE, event.getHead(), BlockHandler.this.context.getLedger().getStateAccumulator()));
			}
			finally
			{
				BlockHandler.this.syncLock.writeLock().unlock();
			}
		}
		
		@Subscribe
		public void on(final SyncLostEvent event) 
		{
			BlockHandler.this.syncLock.writeLock().lock();
			try
			{
				blocksLog.log(BlockHandler.this.context.getName()+": Sync status lost, flushing block handler");
				BlockHandler.this.pendingBlocks.clear();
				BlockHandler.this.pendingBranches.clear();
				BlockHandler.this.progressPhaseQueue.clear();
				BlockHandler.this.postponedVotes.clear();
				BlockHandler.this.headersToVerify.clear();
				BlockHandler.this.blockVoteCollectors.clear();
				BlockHandler.this.progressRounds.clear();
				BlockHandler.this.buildLock = true;
			}
			finally
			{
				BlockHandler.this.syncLock.writeLock().unlock();
			}
		}
	};
}
