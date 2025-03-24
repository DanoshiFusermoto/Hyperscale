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
import org.radix.hyperscale.network.MessageProcessor;
import org.radix.hyperscale.network.messages.InventoryMessage;

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
	
	private enum BlockCertificateStatus
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
				final ProgressRound progressRound = get(BlockHandler.this.progressClock.get());
				
				_processHeaders(progressRound);
				_processVotes(progressRound, head);

				_updateBlocks(head);
				_updateBranches(head);
				
				_livenessTick(progressRound);
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
			final ProgressPhaseEvent progressRoundEvent = BlockHandler.this.progressPhaseQueue.poll(1, TimeUnit.SECONDS);
			if (progressRoundEvent == null)
				return;
			
			if (BlockHandler.this.context.getNode().isSynced() == false)
				return;

			BlockHandler.this.syncLock.readLock().lock();
			try
			{
				final ProgressRound progressRound = progressRoundEvent.getProgressRound();
				final ProgressRound.State progressRoundPhase = progressRoundEvent.getProgressPhase();
					
				// TODO local/secondary proposals on timeout
				if (progressRoundPhase.equals(ProgressRound.State.VOTING))
					prebuild(progressRound, ProgressRound.State.VOTING);
				else if (progressRoundPhase.equals(ProgressRound.State.TRANSITION))
				{
					// Trigger secondaries proposal build
					if (progressRound.isProposalsLatent() && progressRound.getProposers().isEmpty())
						prebuild(progressRound, ProgressRound.State.TRANSITION);
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
	private final AtomicLong shardClock;
	private final AtomicLong progressClock;
	private final MutableLongObjectMap<ProgressRound> progressRounds;

	// TODO temporary fix to ensure that validation of new proposals, and the commitment of existing ones don't cause deadlocks between each other
	private final MonitoredReentrantLock guardLock;
	private final ReentrantReadWriteLock syncLock;

	BlockHandler(Context context)
	{
		this.context = Objects.requireNonNull(context);
		this.blockBuilder = new BlockBuilder(context);
		
		this.pendingBlocks = Collections.synchronizedMap(new HashMap<Hash, PendingBlock>());
		this.pendingBranches = Sets.mutable.<PendingBranch>withInitialCapacity(32).asSynchronized();
		this.headersToVerify = Collections.synchronizedMap(new HashMap<Hash, BlockHeader>());
		this.votesToVerify = Collections.synchronizedMap(new HashMap<Hash, BlockVote>());
		this.postponedVotes = Collections.synchronizedMap(new HashMap<Hash, BlockVote>());
		this.blockVoteCollectors = Collections.synchronizedMap(new HashMap<Long, BlockVoteCollector>());

		this.buildLock = true;
		this.buildClock = new AtomicLong(-1);
		this.shardClock = new AtomicLong(0);
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
			public Set<ShardGroupID> filter(BlockHeader blockHeader)
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
				
				for (BlockHeader header : headers)
				{
					if (blocksLog.hasLevel(Logging.DEBUG))
						blocksLog.debug(BlockHandler.this.context.getName()+": Block header received "+header+" for "+header.getProposer());
	
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
			public Set<ShardGroupID> filter(BlockVote blockVote)
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
			public void receive(Collection<BlockVote> blockVotes, AbstractConnection connection) throws IOException
			{
				if (BlockHandler.this.context.getNode().isSynced() == false)
					return;

				for (final BlockVote blockVote : blockVotes)
				{
					if (blocksLog.hasLevel(Logging.DEBUG))
						blocksLog.debug(BlockHandler.this.context.getName()+": Block vote received "+blockVote.getHash()+" for "+blockVote.getHeight()+"@"+blockVote.getBlock()+" by "+blockVote.getOwner());
		
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
		this.context.getNetwork().getMessaging().register(SyncAcquiredMessage.class, this.getClass(), new MessageProcessor<SyncAcquiredMessage>()
		{
			@Override
			public void process(final SyncAcquiredMessage syncAcquiredMessage, final AbstractConnection connection)
			{
				try
				{
					if (blocksLog.hasLevel(Logging.DEBUG))
						blocksLog.debug(BlockHandler.this.context.getName()+": Block pool inventory request from "+connection);
					
					final Set<Hash> blockVoteInventory = new LinkedHashSet<Hash>();
					final Set<Hash> pendingBlockInventory = new LinkedHashSet<Hash>();
					BlockHandler.this.pendingBlocks.forEach((h, pb) -> pendingBlockInventory.add(pb.getHash()));
					
					long syncInventoryHeight = Math.max(1, syncAcquiredMessage.getHead().getHeight() - Constants.SYNC_INVENTORY_HEAD_OFFSET);
					while (syncInventoryHeight <= BlockHandler.this.context.getLedger().getHead().getHeight())
					{
						Hash syncBlockHash = BlockHandler.this.context.getLedger().getLedgerStore().getSyncBlock(syncInventoryHeight);
						pendingBlockInventory.add(syncBlockHash);
						
						BlockHandler.this.context.getLedger().getLedgerStore().getSyncInventory(syncInventoryHeight, BlockHeader.class).forEach(bh -> pendingBlockInventory.add(bh.getHash()));
						BlockHandler.this.context.getLedger().getLedgerStore().getSyncInventory(syncInventoryHeight, BlockVote.class).forEach(bv -> blockVoteInventory.add(bv.getHash()));
						syncInventoryHeight++;
					}
					
					if (syncLog.hasLevel(Logging.DEBUG))
						syncLog.debug(BlockHandler.this.context.getName()+": Broadcasting blocks "+pendingBlockInventory.size()+" / "+pendingBlockInventory+" to "+connection);
					else
						syncLog.log(BlockHandler.this.context.getName()+": Broadcasting "+pendingBlockInventory.size()+" blocks to "+connection);

					while(pendingBlockInventory.isEmpty() == false)
					{
						InventoryMessage pendingBlockInventoryMessage = new InventoryMessage(pendingBlockInventory, 0, Math.min(Constants.MAX_BROADCAST_INVENTORY_ITEMS, pendingBlockInventory.size()), BlockHeader.class);
						BlockHandler.this.context.getNetwork().getMessaging().send(pendingBlockInventoryMessage, connection);
						pendingBlockInventory.removeAll(pendingBlockInventoryMessage.asInventory().stream().map(ii -> ii.getHash()).collect(Collectors.toList()));
					}

					if (syncLog.hasLevel(Logging.DEBUG))
						syncLog.debug(BlockHandler.this.context.getName()+": Broadcasting block votes "+blockVoteInventory.size()+" / "+blockVoteInventory+" to "+connection);
					else if (syncLog.hasLevel(Logging.INFO))
						syncLog.log(BlockHandler.this.context.getName()+": Broadcasting "+blockVoteInventory.size()+" block votes to "+connection);

					while(blockVoteInventory.isEmpty() == false)
					{
						InventoryMessage blockVoteInventoryMessage = new InventoryMessage(blockVoteInventory, 0, Math.min(Constants.MAX_BROADCAST_INVENTORY_ITEMS, pendingBlockInventory.size()), BlockVote.class);
						BlockHandler.this.context.getNetwork().getMessaging().send(blockVoteInventoryMessage, connection);
						blockVoteInventory.removeAll(blockVoteInventoryMessage.asInventory().stream().map(ii -> ii.getHash()).collect(Collectors.toList()));
					}
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
				final ProgressRound proposalRound = get(pendingBlock.getHeight());
				if (proposalRound.canPropose(pendingBlock.getHeader().getProposer()) == true)
				{
					final long roundVotePower = this.context.getLedger().getValidatorHandler().getVotePower(proposalRound.epoch(), pendingBlock.getHeader().getProposer());
					if (proposalRound.propose(pendingBlock.getHash(), pendingBlock.getHeader().getProposer(), roundVotePower) == false)
						blocksLog.warn(this.context.getName()+": Progress round "+pendingBlock.getHeight()+" already has a proposal from "+pendingBlock.getHeader().getProposer());
					else if (blocksLog.hasLevel(Logging.INFO))
						blocksLog.info(this.context.getName()+": Seen proposal "+pendingBlock.getHash()+" for progress round "+proposalRound.clock()+" from "+pendingBlock.getHeader().getProposer());
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
				if (pendingBlock.isUnbranched() && pendingBlock.isConstructed())
					updateBranchesWith(pendingBlock);
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
			if (bh.getHeight() <= progressRound.clock() + 1)
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
					if (status.equals(BlockInsertStatus.SUCCESS))
					{
						// Header for this round already has a certificate?  Shortcut the round.
						// Even if the proposal isn't fully constructed yet, there is a quorum, therefore 
						// there is no point lingering in the current round and the local instance is clearly
						// behind the majority of the network.
						if (headerToVerify.getHeight() == progressRound.clock() && headerToVerify.getCertificate() != null)
							progressRound.terminate();
					}
					else if (status.equals(BlockInsertStatus.POSTPONED))
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
	
	private void _processVotes(final ProgressRound progressRound, final BlockHeader head)
	{
		_verifyPendingVotes(progressRound, head);
		
		_collateVotesAndApply(progressRound, head);
		
		_tryPostponedVotes(progressRound, head);
	}
	
	private void _verifyPendingVotes(final ProgressRound progressRound, final BlockHeader head)
	{
		if (this.votesToVerify.isEmpty() == false)
		{
			final List<BlockVote> votesToVerify = new ArrayList<BlockVote>(this.votesToVerify.size());
			this.votesToVerify.forEach((h, bv) -> {
				if (bv.getHeight() <= progressRound.clock())
					votesToVerify.add(bv);
				// TODO supermajority this check
				else if (this.shardClock.get() < bv.getHeight())
					this.shardClock.set(bv.getHeight());
			});

			if (votesToVerify.isEmpty() == false)
			{
				for(final BlockVote blockVote : votesToVerify)
				{
					if (blockVote.getHeight() <= head.getHeight())
					{
						if (this.votesToVerify.remove(blockVote.getHash()) == null)
							blocksLog.warn(this.context.getName()+": Stale block vote remove failed "+blockVote.getHash()+" for "+blockVote.getHeight()+"@"+blockVote.getBlock());
						else if (blocksLog.hasLevel(Logging.DEBUG))
							blocksLog.debug(this.context.getName()+": Removed stale block vote "+blockVote.getHash()+" for "+blockVote.getHeight()+"@"+blockVote.getBlock());
						
						continue;
					}

					// Get progress round for this block vote 
					final ProgressRound voteRound = get(blockVote.getHeight());

					// Find or create block vote collector
					final BlockVoteCollector blockVoteCollector = this.blockVoteCollectors.computeIfAbsent(blockVote.getHeight(), h -> new BlockVoteCollector(this.context, voteRound));
					if (blockVoteCollector.hasVoted(blockVote.getOwner().getIdentity()) == false)
					{
						try
						{
							final long roundVotePower = this.context.getLedger().getValidatorHandler().getVotePower(voteRound.epoch(), blockVote.getOwner().getIdentity());
							if (roundVotePower == 0)
								blocksLog.warn(this.context.getName()+": Vote power is zero in epoch "+voteRound.epoch()+" for "+blockVote.getOwner().toString(12));
	
							if (voteRound.vote(blockVote.getOwner().getIdentity(), roundVotePower) == false)
								blocksLog.warn(this.context.getName()+": Block vote "+blockVote.getHash()+" already seen in progress round "+voteRound.clock()+" for "+blockVote.getOwner().toString(12));

							blockVoteCollector.vote(blockVote, roundVotePower);
						}
						catch (IOException ioex)
						{
							blocksLog.warn(this.context.getName()+": Failed to pre-process block vote "+blockVote.getHash()+" in progress round "+voteRound.clock()+" for "+blockVote.getOwner().toString(12));
						}
					}
					// TODO penalties for extra voting
					
					this.votesToVerify.remove(blockVote.getHash());
				}
			}
		}
	}
	
	private void _collateVotesAndApply(final ProgressRound progressRound, final BlockHeader head)
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
							if (currentRoundVoteWeight < progressRound.getVoteThreshold())
							{
								if (progressRound.isVoteCompleted())
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
	
	private void _tryPostponedVotes(final ProgressRound progressRound, final BlockHeader head)
	{
		if (this.postponedVotes.isEmpty() == false)
		{
			// Delayed state votes to count
			if (blocksLog.hasLevel(Logging.DEBUG))
				blocksLog.info(this.context.getName()+": Evaluating "+this.postponedVotes.size()+" postponed block votes");

			boolean signalVotePhaseCompleted = false;
			synchronized(this.postponedVotes)
			{
				final long currentRoundVoteWeight = progressRound.getVoteWeight();
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
						blocksLog.error(this.context.getName()+": Maintenence of delayed block vote failed for "+delayedBlockVote.getHash()+" for block "+delayedBlockVote.getBlock()+" by "+delayedBlockVote.getOwner(), ex);
					}
				}
				
				// Signal vote weight threshold reached?
				if (currentRoundVoteWeight < progressRound.getVoteThreshold())
				{
					if (progressRound.isVoteCompleted())
						signalVotePhaseCompleted = true;
				}
			}
			
			if (signalVotePhaseCompleted)
				this.blockProcessor.signal();
		}
	}
	
	// LIVENESS PIPELINE //
	// TODO separate out 
	private void _livenessTick(final ProgressRound progressRound)
	{
		if (progressRound.getState().equals(ProgressRound.State.NONE))
		{
			progressRound.stepState();
			this.context.getEvents().post(new ProgressPhaseEvent(progressRound));
			return;
		}

		if (progressRound.getState().equals(ProgressRound.State.PROPOSING))
		{
			if (progressRound.isProposalsLatent() || progressRound.isProposalsCompleted())
			{
				if (progressRound.isProposalsLatent())
					blocksLog.warn(this.context.getName()+": Proposal phase latent "+progressRound);
				else if (blocksLog.hasLevel(Logging.INFO))
					blocksLog.info(this.context.getName()+": Proposal phase completed "+progressRound);

				progressRound.stepState();
				this.context.getEvents().post(new ProgressPhaseEvent(progressRound));
			}
			
			return;
		}
		
		if (progressRound.getState().equals(ProgressRound.State.TRANSITION))
		{
			final long transitionThreshold = progressRound.getProposeThreshold();
			long contructedVotePower = 0;

			if (progressRound.isTransitionLatent() || progressRound.isFullyProposed())
			{
				for(final Hash proposal : progressRound.getProposals())
				{
					final PendingBlock pendingBlock = this.pendingBlocks.get(proposal);
					if (pendingBlock == null)
						continue;
					
					if (pendingBlock.isConstructed() == false)
						continue;
					
					try
					{
						contructedVotePower += this.context.getLedger().getValidatorHandler().getVotePower(progressRound.epoch(), pendingBlock.getHeader().getProposer());
					}
					catch (IOException ioex)
					{
						blocksLog.error(this.context.getName()+": Failed to get vote power for proposer "+pendingBlock.getHeader().getProposer().toString(12)+" of proposal "+pendingBlock.getHeight()+":"+pendingBlock.getHash(), ioex);
					}
				}
			}
			
			if (progressRound.isTransitionTimedout() || contructedVotePower >= transitionThreshold)
			{
				if (progressRound.isTransitionTimedout())
					blocksLog.warn(this.context.getName()+": Transition phase timed out "+progressRound);
				else if (blocksLog.hasLevel(Logging.INFO))
					blocksLog.info(this.context.getName()+": Transition phase completed "+progressRound);

				progressRound.stepState();
				this.context.getEvents().post(new ProgressPhaseEvent(progressRound));
			}

			return;
		}
		
		if (progressRound.getState().equals(ProgressRound.State.VOTING))
		{
			// If the local instance shard is behind, don't cast a vote
			if (progressRound.driftClock() <= 0 && 
				progressRound.hasVoted(this.context.getNode().getIdentity()) == false)
			{
				PendingBranch selectedBranch = selectBranchToVote(progressRound);
				try 
				{
					if (selectedBranch != null)
						vote(progressRound, selectedBranch);
				} 
				catch (IOException | CryptoException | ValidationException ex) 
				{
					blocksLog.error(this.context.getName()+": Failed to cast vote on progress round "+progressRound+" to branch "+selectedBranch, ex);
				}
			}

			if (progressRound.isVoteCompleted() || progressRound.isVoteTimedout())
			{
				progressRound.stepState();
				this.context.getEvents().post(new ProgressPhaseEvent(progressRound));
				
				if (progressRound.isVoteTimedout())
					blocksLog.warn(this.context.getName()+": Voting phase timed out "+progressRound);								
				else if (blocksLog.hasLevel(Logging.INFO))
					blocksLog.info(this.context.getName()+": Voting phase completed "+progressRound);
			}

			return;
		}
		
		if (progressRound.getState().equals(ProgressRound.State.COMPLETED))
		{
			final List<Identity> absentProposers = progressRound.getAbsentProposers();
			if (absentProposers.isEmpty() == false)
				blocksLog.warn(this.context.getName()+": Absent proposers ["+absentProposers.stream().map(i -> i.toString(12)).collect(Collectors.joining(", "))+"] in progress round "+progressRound);								
			
			final long nextProgressRoundClock = this.progressClock.incrementAndGet();
			this.shardClock.compareAndSet(progressRound.clock(), nextProgressRoundClock);
			
			final ProgressRound nextProgressRound = get(nextProgressRoundClock);
			this.context.getEvents().post(new ProgressPhaseEvent(nextProgressRound));

			if (blocksLog.hasLevel(Logging.INFO))
				blocksLog.info(this.context.getName()+": Progress round completed "+progressRound);

			// Progress interval & delay
			final long targetRoundDuration = Math.max(Ledger.definitions().roundInterval(), Configuration.getDefault().get("ledger.liveness.delay", 0));
			final long roundDelayDuration = (targetRoundDuration-progressRound.getDuration())+Math.min(progressRound.driftMilli()/2, 0);
			
			// Too fast
			if (roundDelayDuration > 0 && progressRound.driftClock() == 0)
			{
				try 
				{
					if (blocksLog.hasLevel(Logging.INFO))
						blocksLog.info(this.context.getName()+": Round delay for "+progressRound.clock()+" of "+roundDelayDuration+"ms");
					
					// TODO better way to implement this delay as simply sleeping costs potential 
					// processing time which could be used to update / verify proposals and votes
					Thread.sleep(roundDelayDuration);
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
					if (this.shardClock.get() > progressRound.clock())
						blocksLog.info(this.context.getName()+": Skipping round delay for "+progressRound.clock()+" because shard clock is ahead "+this.shardClock.get());
					else
						blocksLog.info(this.context.getName()+": Skipping round delay for "+progressRound.clock()+" because of long round interval "+progressRound.getDuration()+"ms");
				}
			}
			
			this.blockProcessor.signal();
			
			this.context.getMetaData().increment("ledger.interval.progress", progressRound.getDuration());
			
			if (blocksLog.hasLevel(Logging.INFO))
				blocksLog.info(this.context.getName()+": Progress round is now "+nextProgressRound.toString());
		}
	}
	
	private void _decideCommit(final ProgressRound progressRound, final ProgressRound.State progressRoundPhase)
	{
		// If the round was latent on completion then do not attempt a commit and continue swiftly to the next round.  
		// A commit (if pending) will be performed after any required proposal generation.
		if (progressRoundPhase.equals(ProgressRound.State.COMPLETED))
		{
			final long minDuration = Math.max(Ledger.definitions().roundInterval(), Configuration.getDefault().get("ledger.liveness.delay", 0));
			final long durationDelay = minDuration - progressRound.getDuration();
			if (durationDelay < 0)
			{
				if (blocksLog.hasLevel(Logging.DEBUG))
					blocksLog.debug(BlockHandler.this.context.getName()+": Skipping commit on COMPLETED for progress round "+progressRound.clock()+" due to round duration latency of "+(-durationDelay)+"ms");
				return;
			}
		}

		try
		{
			final PendingBranch selectedBranch = selectBranchWithQuorum(progressRound);
			if (selectedBranch != null)
			{
				final int numSupers = Constants.MIN_COMMIT_SUPERS;
				final PendingBlock commitTo = selectedBranch.commitable(numSupers);
				if (commitTo != null)
					commit(commitTo, selectedBranch);
			}
		}
		catch (Exception ex)
		{
			// TODO additional clean up here, trim bad branch, etc
			blocksLog.error(this.context.getName()+": Failed to commit proposal on "+progressRound, ex);
		}
	}

	// METHODS //
	public int size()
	{
		return this.pendingBlocks.size();
	}
	
	private ProgressRound get(final long clock)
	{
		ProgressRound progressRound;
		
		synchronized(this.progressRounds)
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
				
				return new ProgressRound(clock, nextProposers, nextProposersVotePower, nextTotalVotePower, this.shardClock.get()-this.progressClock.get());
			});
		}
		
		return progressRound;
	}
	
	private BlockVoteStatus process(final BlockVote blockVote, final BlockHeader head) throws IOException, CryptoException, ValidationException
	{
		Objects.requireNonNull(blockVote, "Block vote is null");

		// TODO Should be pre-verified, check 

		// Stale?
		if (blockVote.getHeight() < head.getHeight())
		{
			if (blocksLog.hasLevel(Logging.DEBUG))
				blocksLog.warn(BlockHandler.this.context.getName()+": Removed stale delayed block vote "+blockVote);
			
			return BlockVoteStatus.STALE;
		}

		// Progress round is not available yet, postpone
		final ProgressRound voteRound = get(blockVote.getHeight());
		
		// Apply the vote to the progress round.  
		// The vote should be applied even if the pending block is not yet know to ensure responsiveness and liveness of progress rounds 
		if (voteRound.hasVoted(blockVote.getOwner().getIdentity()) == false)
		{
			final long roundVotePower = this.context.getLedger().getValidatorHandler().getVotePower(voteRound.epoch(), blockVote.getOwner().getIdentity());
			if (roundVotePower == 0)
				blocksLog.warn(BlockHandler.this.context.getName()+": Progress vote power is zero for "+blockVote.getOwner());

			if (voteRound.vote(blockVote.getOwner().getIdentity(), roundVotePower) == false)
				blocksLog.warn(BlockHandler.this.context.getName()+": Progress vote "+blockVote.getHash()+" already seen in progress round "+voteRound.clock()+" for "+blockVote.getOwner());
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
		
		// Apply the vote to the pending block
		if (pendingBlock.voted(blockVote.getOwner()) == false)
		{
			final PendingBranch pendingBranch = getPendingBranch(pendingBlock);
			if (pendingBranch == null)
			{
				if (blocksLog.hasLevel(Logging.DEBUG))
					blocksLog.warn(this.context.getName()+": Branch not found for "+blockVote.getHeight()+"@"+blockVote.getBlock()+" when processing block vote "+blockVote.getHash()+" by "+blockVote.getOwner());
				
				return BlockVoteStatus.POSTPONED;
			}

			try
			{
				final long branchVotePower = pendingBranch.getVotePower(blockVote.getHeight(), blockVote.getOwner().getIdentity(), true);
				final long preWeight = pendingBlock.getVoteWeight(); 
				if (pendingBlock.vote(blockVote, branchVotePower) == true)
				{
					if (blocksLog.hasLevel(Logging.INFO))
						blocksLog.info(BlockHandler.this.context.getName()+": "+blockVote.getOwner().toString(12)+" voted on block "+pendingBlock+" "+blockVote.getHash());

					if (preWeight < pendingBlock.getVoteThreshold() && pendingBlock.getVoteWeight() >= pendingBlock.getVoteThreshold())
					{
						if (blocksLog.hasLevel(Logging.INFO))
							blocksLog.info(this.context.getName()+": Pending block is now a super "+pendingBlock.toString());
						
						if (pendingBlock.buildCertificate() == null)
							blocksLog.error(this.context.getName()+": Expected to construct block certificate for "+pendingBlock.toString());
					}

					return BlockVoteStatus.SUCCESS;
				}
				else
					return BlockVoteStatus.SKIPPED;
			}
			finally
			{
				this.context.getMetaData().increment("ledger.blockvote.processed");
				this.context.getMetaData().increment("ledger.blockvote.latency", blockVote.getAge(TimeUnit.MILLISECONDS));
			}
		}
		else
			blocksLog.warn(this.context.getName()+": Already seen block vote "+blockVote.getHash()+" for block "+blockVote.getHeight()+"@"+blockVote.getBlock()+" by "+blockVote.getOwner());

		return BlockVoteStatus.FAILED;
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

		if (round.driftClock() > 0)
			throw new ValidationException("Attempted to vote on progress round "+round.clock()+" but shard clock is "+this.shardClock.get());

		if (round.hasVoted(this.context.getNode().getIdentity()))
		{
			blocksLog.warn(this.context.getName()+": Progress vote is already cast in progress round "+round.clock()+" by "+this.context.getNode().getIdentity());
			return null;
		}

		PendingBlock pendingBlock = branch.getBlockAtHeight(round.clock());
		if (pendingBlock == null)
		{
			blocksLog.warn(this.context.getName()+": No proposal available at progress round "+round.clock()+" to vote on in branch "+branch);
			return null;
		}
		
		if (pendingBlock.isApplied() == false)
			return null;

		if (pendingBlock.isConstructed() == false)
			return null;
		
		BlockVote blockVote = null;
		long votePower = branch.getVotePower(round.clock(), this.context.getNode().getIdentity());
		if (votePower > 0)
		{
			blockVote = new BlockVote(pendingBlock.getHash(), this.context.getNode().getIdentity().getKey());
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
			}
			else
			{
				// TODO handle better?
				blocksLog.error(BlockHandler.this.context.getName()+": Vote on block "+pendingBlock+" failed");
			}
		}
		
		return blockVote;
	}
	
	private void prebuild(final ProgressRound progressRound, final ProgressRound.State progressPhase)
	{
		// TODO This should actually throw, but complex to handle
		if (this.buildClock.get() > progressRound.clock())
			return;
		
		// If the local instances shard is behind, don't build
		if (progressRound.driftClock() > 0)
			return;
		
		final ProgressRound buildRound;
		if (progressPhase.equals(ProgressRound.State.TRANSITION))
		{
			buildRound = progressRound;
		}
		else if (progressPhase.equals(ProgressRound.State.VOTING))
		{
			buildRound = get(progressRound.clock()+1);
			if (buildRound == null)
				throw new IllegalArgumentException("Expected to find next progress round "+(progressRound.clock()+1)+" for build on progress phase "+progressPhase);
		}
		else
			throw new IllegalArgumentException("Called builder with progress phase "+progressPhase+" for progress round "+progressRound);
		
		boolean build = buildRound.canPropose(this.context.getNode().getIdentity()) == true;

		if (this.context.getConfiguration().get("ledger.liveness.recovery", Boolean.FALSE) == Boolean.TRUE && 
			buildRound.getState().equals(ProgressRound.State.TRANSITION))
			build = false;

		if (this.context.getConfiguration().has("ledger.faults.produce_unbuildable_branches_every"))
		{
			int interval = this.context.getConfiguration().get("ledger.faults.produce_unbuildable_branches_every", -1);
			if (interval > 0 && buildRound.clock() % interval == 0)
				build = false;
		}
		
		if (build == true && this.buildLock == false)
		{
			try
			{
				final List<PendingBlock> generatedBlocks = build(buildRound);
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
				blocksLog.error(this.context.getName()+": Failed to build proposal on "+buildRound, ex);
			}
		}
		else if (build && this.buildLock)
			blocksLog.warn(this.context.getName()+": Build lock is set: Skipping build on proposal "+buildRound);
	}

	private List<PendingBlock> build(final ProgressRound progressRound) throws IOException, LockException, ValidationException, CryptoException
	{
		Objects.requireNonNull(progressRound, "Progress round is null");
		
		if (progressRound.getState().equals(ProgressRound.State.COMPLETED))
			blocksLog.warn(this.context.getName()+": Building on progress round "+progressRound.clock()+" which is COMPLETED");
		
		if (this.buildClock.get() > progressRound.clock())
			throw new IllegalStateException("Build clock is "+this.buildClock.get()+" but progress round is "+progressRound);
		
		final Entry<BlockHeader, StateAccumulator> ledgerState = BlockHandler.this.context.getLedger().current();

		// Select a branch to extend
		BlockHeader buildableHeader;
		PendingBranch selectedBranch = selectBranchToExtend(progressRound);
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
				blocksLog.warn(this.context.getName()+": No branch selected at "+progressRound.clock()+" with ledger head "+ledgerState.getKey());
	
				// TODO secondaries?
				
				return null;
			}
		}
		else
		{
			final PendingBlock buildableBlock = selectedBranch.getBlockAtHeight(progressRound.clock()-1);
			if (buildableBlock.isConstructable())
				buildableHeader = buildableBlock.getHeader();
			else
				buildableHeader = null;
		}
		
		if (buildableHeader == null)
		{
			blocksLog.warn(this.context.getName()+": No buildable header available for progress round "+progressRound.clock()+" on selected branch "+selectedBranch);
			return null;
		}

		if (ledgerState.getKey().equals(selectedBranch.getRoot()) == false)
			throw new IllegalStateException("Build branch is stale when preparing, ledger head has changed from expected "+selectedBranch.getRoot()+" to "+ledgerState.getKey());

		final PendingBranch buildableBranch = new PendingBranch(this.context, ledgerState.getKey(), ledgerState.getValue(), selectedBranch.getBlocksTo(buildableHeader.getHash()));
		if (buildableBranch.isBuildable() == false)
			throw new IllegalStateException("Build branch is not buildable "+buildableBranch);
		
		long buildClock = buildableHeader.getHeight();
		final List<PendingBlock> generatedBlocks = new ArrayList<>();
		while(buildClock < progressRound.clock())
		{
			final PendingBlock generatedBlock = this.blockBuilder.build(buildableHeader, buildableBranch, ledgerState.getKey());
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
	
	private PendingBranch getPendingBranch(PendingBlock pendingBlock)
	{
		synchronized(this.pendingBranches)
		{
			for (PendingBranch pendingBranch : this.pendingBranches)
			{
				if (pendingBranch.getRoot().getHash().equals(pendingBlock.getHeader().getPrevious()))
					return pendingBranch;
				
				if (pendingBranch.contains(pendingBlock.getHeader().getPrevious()))
					return pendingBranch;
			}

			return null;
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
		if (header.getCertificate() != null)
			blocksLog.warn(this.context.getName()+": Discovered certificate when inserting block header "+header.toString());

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
			final ProgressRound progressRound = get(pendingBlock.getHeight());
	
			final long roundVotePower = this.context.getLedger().getValidatorHandler().getVotePower(progressRound.epoch(), pendingBlock.getHeader().getProposer());
			if (progressRound.propose(pendingBlock.getHash(), pendingBlock.getHeader().getProposer(), roundVotePower) == false)
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

	
	/**
	 * Verifies and applies a proposal certificate to a proposal.
	 * 
	 * Should be private but is visible for testing
	 * 
	 * @param blockCertificate
	 */
	@VisibleForTesting
	BlockCertificateStatus apply(final BlockCertificate blockCertificate)
	{
		Objects.requireNonNull(blockCertificate, "Block certificate is null");

		if (blockCertificate.getHeight() <= this.context.getLedger().getHead().getHeight())
			return BlockCertificateStatus.STALE;
		
		PendingBlock pendingBlock = this.pendingBlocks.get(blockCertificate.getBlock());
		if (pendingBlock == null)
			return BlockCertificateStatus.POSTPONED;

		if (pendingBlock.getHeader() == null)
			throw new IllegalStateException("Pending block "+pendingBlock.getHash()+" does not have a header");
		
		// TODO verification
		
		if (pendingBlock.getHeader().getCertificate() != null)
			return BlockCertificateStatus.SKIPPED;

		pendingBlock.getHeader().setCertificate(blockCertificate);
		return BlockCertificateStatus.SUCCESS;
	}

	@VisibleForTesting
	PendingBlock getBlock(Hash block)
	{
		Objects.requireNonNull(block, "Block hash is null");
		Hash.notZero(block, "Block hash is ZERO");

		return this.pendingBlocks.get(block);
	}

	private void updateBranchesWith(final PendingBlock pendingBlock) throws LockException, IOException, ValidationException
	{
		Objects.requireNonNull(pendingBlock, "Pending block is null");
		if (pendingBlock.getHeader() == null)
			throw new IllegalStateException("Pending block "+pendingBlock.getHash()+" does not have a header");
		
		if (pendingBlock.isConstructed() == false)
			throw new IllegalStateException("Pending block "+pendingBlock.getHash()+" is not constructed");

		if (pendingBlock.isUnbranched() == false)
			throw new IllegalStateException("Pending block "+pendingBlock.getHash()+" is already in a branch");

		final BlockHeader ledger = this.context.getLedger().getHead();
		
		PendingBlock currentPendingBlock = pendingBlock;
		LinkedList<PendingBlock> branch = new LinkedList<PendingBlock>();
		while(currentPendingBlock != null)
		{
			// If any vertex in the branch has throw, abort updating entire branch.
			if (currentPendingBlock.thrown() != null)
				return;
			
			branch.add(currentPendingBlock);
			if (currentPendingBlock.getHeader().getPrevious().equals(ledger.getHash()))
				break;
			
			PendingBlock previous = this.pendingBlocks.get(currentPendingBlock.getHeader().getPrevious());
			if (previous != null && previous.getHeader() != null)
				currentPendingBlock = previous;
			else
				currentPendingBlock = null;
		}
		
		if (branch.isEmpty())
			return;

		if (branch.getLast().getHeader().getPrevious().equals(this.context.getLedger().getHead().getHash()) == false)
		{
			if (blocksLog.hasLevel(Logging.DEBUG))
				blocksLog.error(BlockHandler.this.context.getName()+": Branch for pending block "+pendingBlock.getHeader()+" does not terminate at ledger head "+this.context.getLedger().getHead().getHash()+" but at "+branch.getFirst().getHeader());
			
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
	        
	        for (final PendingBlock block : branch.getBlocks()) 
	        {
	        	if (branch.isBuildable(block.getHeader()) == false)
	        		break;
	        	
	        	if (block.getHeight() != height)
	        		continue;
	        	
            	final BlockHeader header = block.getHeader();
                if (highestWork == null || header.getTotalWork().compareTo(highestWork.getTotalWork()) > 0) 
                {
                    highestWork = header;
                    selectedBranch = branch;
                }
	        }
	    }
	    
	    return selectedBranch;
	}
	
	private long calculateBranchVoteStrength(final PendingBranch branch, final long height) 
	{
	    long totalVotePower = 0;
	    for (final PendingBlock block : branch.getBlocks()) 
	    {
	        if (block.getHeight() > height)
	            continue;
	            
	        final long votePower = block.getVoteWeight();
	        final long fPlusOne = (block.getVoteThreshold()/2)+1;
	        if (votePower >= fPlusOne) 
	            totalVotePower += votePower;
	    }
	    
	    return totalVotePower;
	}
	
	private PendingBranch selectSuperBranch(final long height)
	{
	    final List<PendingBranch> candidateBranches;
	    synchronized(this.pendingBranches) 
	    {
	        if (this.pendingBranches.isEmpty())
	            throw new IllegalStateException("No pending branches available");
	            
	        candidateBranches = new ArrayList<PendingBranch>();
	        for (final PendingBranch pendingBranch : this.pendingBranches)
	        {
	        	if (pendingBranch.getBlockAtHeight(height) == null)
	        		continue;
	        	
	        	candidateBranches.add(pendingBranch);
	        }
	    }
	    
	    if (candidateBranches.isEmpty())
	    	return null;
	    
	    // FIRST: Check if any branches contain super blocks
	    final List<PendingBranch> branchesWithSupers = new ArrayList<>(candidateBranches.size());
	    for (final PendingBranch branch : candidateBranches) 
	    {
	        if (branch.supers().isEmpty() == false) 
	            branchesWithSupers.add(branch);
	    }
	    
	    // If we have branches with supers, ONLY consider those
	    List<PendingBranch> filteredCandidates;
	    if (branchesWithSupers.isEmpty() == false)
	        filteredCandidates = branchesWithSupers;
	    else
	        filteredCandidates = candidateBranches;

	    // SECOND: Find branch with strongest vote power up to height
	    PendingBranch bestBranch = null;
	    long bestBranchStrength = 0;
	    for (final PendingBranch branch : filteredCandidates)
	    {
	        long strength = calculateBranchVoteStrength(branch, height);
	        if (strength > bestBranchStrength)
	        {
	            bestBranchStrength = strength;
	            bestBranch = branch;
	        }
	    }
	    
	    // THIRD: Among strong consensus branches, pick highest work proposal at height
	    if (bestBranch != null)
	    {
	        final List<PendingBranch> consensusBranches = new ArrayList<>();
	        // Collect branches building on the strong consensus chain
	        for (PendingBranch candidateBranch : filteredCandidates)
	        {
	            if (candidateBranch.intersects(bestBranch))
	                consensusBranches.add(candidateBranch);
	        }

	        // Pick highest work proposal among these branches
	        final PendingBranch selected = findHighestWorkBranch(consensusBranches, height);
	        if (selected != null)
	            return selected;
	    }
	    
	    // Fallback: No supers exist or no valid blocks building on branches with vote power
	    return findHighestWorkBranch(candidateBranches, height);
	}

	private PendingBranch selectBranchToVote(final ProgressRound round) 
	{
		return selectSuperBranch(round.clock());
	}

	private PendingBranch selectBranchToExtend(final ProgressRound round) 
	{
		return selectSuperBranch(round.clock()-1);
	}

	private PendingBranch selectBranchWithQuorum(final ProgressRound round)
	{
		final long targetHeight;
		// Make sure we don't commit an uncompleted progress round
		if (this.progressClock.get() > round.clock())
			targetHeight = round.clock();
		else
			targetHeight = round.clock()-1;
		
		final BlockHeader head = this.context.getLedger().getHead();
		
		PendingBranch candidateBranch = null;
		PendingBlock candidateBlock = null;
		synchronized(this.pendingBranches)
		{
			if (this.pendingBranches.isEmpty())
				throw new IllegalStateException("No pending branches available");

			Iterator<PendingBranch> pendingBranchIterator = this.pendingBranches.iterator();
			while (pendingBranchIterator.hasNext())
			{
				final PendingBranch pendingBranch = pendingBranchIterator.next();

				try
				{
					if (pendingBranch.isEmpty())
					{
						// Alert on all empty branches except a genesis branch
						if (pendingBranch.getRoot().getHeight() > 0)
							blocksLog.warn(this.context.getName()+": Unexpected empty branch "+pendingBranch);
						
						continue;
					}
					
					if (pendingBranch.getLow().getHeader().getPrevious().equals(head.getHash()) == false)
					{
						if (blocksLog.hasLevel(Logging.DEBUG))
							blocksLog.debug(this.context.getName()+": Branch doesn't attach to ledger "+pendingBranch.getLow());
						
						continue;
					}
					
					LinkedList<PendingBlock> supers = pendingBranch.supers();
					if (supers.size() < Constants.MIN_COMMIT_SUPERS)
						continue;
					
					PendingBlock superBlock = null;
					Iterator<PendingBlock> superBlockIterator = supers.iterator();
					while(superBlockIterator.hasNext())
					{
						superBlock = superBlockIterator.next();

						if (superBlock.getHeight() <= targetHeight && supers.getLast().equals(superBlock) == false)
							break;
						
						superBlock = null;
					}
					
					if (superBlock == null)
						continue;
					
					if (candidateBlock != null && candidateBlock.equals(superBlock) == false)
					{
						blocksLog.fatal(this.context.getName()+": Possible safety break");
						blocksLog.fatal(this.context.getName()+":     Candidate - "+candidateBlock);
						blocksLog.fatal(this.context.getName()+":         Super - "+superBlock);
						
						if (superBlock.getHeight() > candidateBlock.getHeight())
							continue;
					}
					
					candidateBlock = superBlock;
					candidateBranch = pendingBranch;
				}
				catch (Exception ex)
				{
					blocksLog.error(this.context.getName()+": Branch selection with quorum "+pendingBranch.toString()+" failed", ex);
				}
			}
		}
		
		return candidateBranch;
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
				
				// NOTE Collecting known block votes is redundant and carries a risk that they can cause safety issues locally on the 
				// 		suncing replica.  In edge cases depending on network state, they being counted towards 2f+1 AFTER the progress round 
				// 		has been completed by the network at large.  The network would be safe, but the syncing replica could suffer a local 
				//		safety / liveness issue.  

				BlockHandler.this.progressClock.set(event.getHead().getHeight()+1);
				BlockHandler.this.shardClock.set(BlockHandler.this.progressClock.get());
				BlockHandler.this.buildClock.set(event.getHead().getHeight());
				BlockHandler.this.buildLock = false;
				
				ProgressRound progressRound = get(BlockHandler.this.progressClock.get());
				blocksLog.info(BlockHandler.this.context.getName()+": Progress round post sync is "+progressRound.toString());
				
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
