package org.radix.hyperscale.ledger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.radix.hyperscale.Constants;
import org.radix.hyperscale.Context;
import org.radix.hyperscale.Service;
import org.radix.hyperscale.Universe;
import org.radix.hyperscale.crypto.CryptoException;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.events.EventListener;
import org.radix.hyperscale.events.SyncLostEvent;
import org.radix.hyperscale.exceptions.StartupException;
import org.radix.hyperscale.exceptions.TerminationException;
import org.radix.hyperscale.exceptions.ValidationException;
import org.radix.hyperscale.executors.Executable;
import org.radix.hyperscale.executors.Executor;
import org.radix.hyperscale.ledger.BlockHeader.InventoryType;
import org.radix.hyperscale.ledger.Substate.NativeField;
import org.radix.hyperscale.ledger.events.SyncAcquiredEvent;
import org.radix.hyperscale.ledger.events.SyncAtomCommitEvent;
import org.radix.hyperscale.ledger.events.SyncBlockEvent;
import org.radix.hyperscale.ledger.events.SyncPrepareEvent;
import org.radix.hyperscale.ledger.exceptions.LockException;
import org.radix.hyperscale.ledger.messages.GetSyncBlockInventoryMessage;
import org.radix.hyperscale.ledger.messages.GetSyncBlockMessage;
import org.radix.hyperscale.ledger.messages.SyncAcquiredMessage;
import org.radix.hyperscale.ledger.messages.SyncBlockInventoryMessage;
import org.radix.hyperscale.ledger.messages.SyncBlockMessage;
import org.radix.hyperscale.ledger.primitives.Atom;
import org.radix.hyperscale.ledger.primitives.AtomCertificate;
import org.radix.hyperscale.ledger.primitives.StateInput;
import org.radix.hyperscale.ledger.primitives.StateOutput;
import org.radix.hyperscale.ledger.sme.ManifestException;
import org.radix.hyperscale.ledger.sme.PolyglotPackage;
import org.radix.hyperscale.ledger.sme.exceptions.StateMachineException;
import org.radix.hyperscale.ledger.sme.exceptions.StateMachineExecutionException;
import org.radix.hyperscale.ledger.sme.exceptions.StateMachinePreparationException;
import org.radix.hyperscale.ledger.timeouts.AtomTimeout;
import org.radix.hyperscale.ledger.timeouts.CommitTimeout;
import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;
import org.radix.hyperscale.network.AbstractConnection;
import org.radix.hyperscale.network.ConnectionState;
import org.radix.hyperscale.network.ConnectionTask;
import org.radix.hyperscale.network.MessageProcessor;
import org.radix.hyperscale.network.StandardConnectionFilter;
import org.radix.hyperscale.network.events.DisconnectedEvent;
import org.radix.hyperscale.network.messages.NodeMessage;
import org.radix.hyperscale.utils.MathUtils;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Ordering;
import com.google.common.collect.SortedSetMultimap;
import com.google.common.collect.TreeMultimap;
import com.google.common.eventbus.Subscribe;

public class SyncHandler implements Service
{
	private static final Logger syncLog = Logging.getLogger("sync");
	
	private static final boolean MULTIPLE_SYNC_BLOCK_REQUESTS = true;
	
	static 
	{
		syncLog.setLevel(Logging.INFO);
	}
	
	enum SyncStatus
	{
		UNSYNCED, SYNCED, CONNECTING
	}
	
	private final class SyncRequestTask extends ConnectionTask 
	{
		final Hash requestedBlock;
		final long requestSeq;
		
		SyncRequestTask(final AbstractConnection connection, final Hash block, final long requestSeq)
		{
			super(connection, 30, TimeUnit.SECONDS);
			
			Objects.requireNonNull(block, "Block hash to request is null");
			this.requestedBlock = block;
			this.requestSeq = requestSeq;
		}
		
		@Override
		public void execute()
		{
			boolean isFailedRequest = false;
			SyncHandler.this.lock.lock();
			try
			{
				SyncHandler.this.requestTasks.remove(getConnection(), this);
				if (SyncHandler.this.blocksRequested.remove(this.requestedBlock, this.requestSeq) == true)
					isFailedRequest = true;
			}
			finally
			{
				SyncHandler.this.lock.unlock();
			}
			
			// Can do the disconnect and re-request outside of the lock
			if (isFailedRequest)
			{
				try
				{
					syncLog.error(SyncHandler.this.context.getName()+": "+getConnection()+" did not respond fully to block request of "+this.requestedBlock);
					if (getConnection().getState().equals(ConnectionState.CONNECTED) || getConnection().getState().equals(ConnectionState.CONNECTING))
						getConnection().disconnect("Did not respond to block request of "+this.requestedBlock);
				}
				catch (Throwable t)
				{
					syncLog.error(SyncHandler.this.context.getName()+": "+getConnection(), t);
				}
				
				rerequest(this.requestedBlock);
			}
		}

		public boolean completed()
		{
			SyncHandler.this.lock.lock();
			try
			{
				if (SyncHandler.this.blocksRequested.containsKey(this.requestedBlock) == false)
					return true;
				
				return Long.valueOf(this.requestSeq).equals(SyncHandler.this.blocksRequested.get(this.requestedBlock)) == false;
			}
			finally
			{
				SyncHandler.this.lock.unlock();
			}
		}

		@Override
		public void onCancelled()
		{
			boolean wasPending = false;
			SyncHandler.this.lock.lock();
			try
			{
				SyncHandler.this.requestTasks.remove(getConnection(), this);
				if (SyncHandler.this.blocksRequested.remove(this.requestedBlock, this.requestSeq) == true)
					wasPending = true;

				if (wasPending)
					rerequest(this.requestedBlock);
			}
			finally
			{
				SyncHandler.this.lock.unlock();
			}
		}
		
		private void rerequest(final Hash block)
		{
			// Build the re-requests
			ShardGroupID rerequestShardGroupID = ShardMapper.toShardGroup(getConnection().getNode().getIdentity(), SyncHandler.this.context.getLedger().numShardGroups());
			StandardConnectionFilter standardPeerFilter = StandardConnectionFilter.build(SyncHandler.this.context).setStates(ConnectionState.CONNECTED).setShardGroupID(rerequestShardGroupID);
			List<AbstractConnection> rerequestConnectedPeers = SyncHandler.this.context.getNetwork().get(standardPeerFilter);
			if (rerequestConnectedPeers.isEmpty() == false)
			{
				AbstractConnection rerequestPeer = rerequestConnectedPeers.get(0);
				try
				{
					SyncHandler.this.request(rerequestPeer, block);
				}
				catch (IOException ioex)
				{
					syncLog.error(SyncHandler.this.context.getName()+": Failed to re-request "+blocks+" blocks from "+rerequestPeer, ioex);
				}
			}
			else
				syncLog.error(SyncHandler.this.context.getName()+": Unable to re-request "+blocks.size()+" blocks");
		}
	}

	private final class SyncInventoryTask extends ConnectionTask 
	{
		final Hash block;
		final long requestSeq;
		
		SyncInventoryTask(final AbstractConnection connection, final Hash block, final long requestSeq)
		{
			super(connection, 10, TimeUnit.SECONDS);
			
			this.requestSeq = requestSeq;
			this.block = block;
		}
		
		@Override
		public void execute()
		{
			SyncHandler.this.lock.lock();
			try
			{
				SyncInventoryTask currentTask = SyncHandler.this.inventoryTasks.get(getConnection());
				if (currentTask == null)
					return;
				
				if (currentTask.requestSeq == this.requestSeq)
				{
					SyncHandler.this.inventoryTasks.remove(getConnection(), this);

					syncLog.error(SyncHandler.this.context.getName()+": "+getConnection()+" did not respond to inventory request "+this.requestSeq+":"+this.block);
					if (getConnection().getState().equals(ConnectionState.CONNECTED) || getConnection().getState().equals(ConnectionState.CONNECTING))
						getConnection().disconnect("Did not respond to inventory request "+this.requestSeq+":"+this.block);
				}
			}
			catch (Throwable t)
			{
				syncLog.error(SyncHandler.this.context.getName()+": "+getConnection(), t);
			}
			finally
			{
				SyncHandler.this.lock.unlock();
			}
		}

		@Override
		public void onCancelled()
		{
			SyncHandler.this.lock.lock();
			try
			{
				SyncHandler.this.inventoryTasks.remove(getConnection(), this);
			}
			finally
			{
				SyncHandler.this.lock.unlock();
			}
		}
	}

	private final Context context;
	private final Map<Hash, PendingAtom> 	atoms;
	private final Map<Hash, Block> 			blocks;
	private final Map<Hash, Long> 			blocksRequested;
	
	private final Multimap<Hash, StateVoteCollector> stateVoteCollectors;
	private final Multimap<AbstractConnection, SyncRequestTask> requestTasks;
	private final Map<AbstractConnection, SyncInventoryTask> inventoryTasks;
	private final SortedSetMultimap<AbstractConnection, Hash> blockInventories;
	
	private volatile boolean isSynced;
	private volatile boolean isPrepared;
	private volatile boolean isSingleton;
	
	private BlockHeader syncHead;
	private BlockHeader desyncHead;
	private StateAccumulator syncAccumulator;
	
	private long forcedDesyncDuration;

	private final ReentrantLock lock = new ReentrantLock(true);

	private Semaphore  syncProcessorLatch = new Semaphore(0);
	private Thread syncProcessorThread = null;
	private Executable syncProcessor = new Executable()
	{
		@Override
		public void execute()
		{
			try 
			{
				while (this.isTerminated() == false)
				{
					try
					{
						if (SyncHandler.this.syncProcessorLatch.tryAcquire(1000, TimeUnit.MILLISECONDS))
							SyncHandler.this.syncProcessorLatch.drainPermits();

						if (SyncHandler.this.checkSynced().equals(SyncStatus.UNSYNCED) == false)
							continue;

						// Need to prepare for a sync?
						if (SyncHandler.this.isSynced == false && SyncHandler.this.isPrepared == false)
						{
							prepare();
							continue;
						}
						
						// Check if preparing after desync
						if (SyncHandler.this.isPrepared == false)
							continue;
						
						if (syncLog.hasLevel(Logging.DEBUG))
							syncLog.debug(SyncHandler.this.context.getName()+": Performing sync iteration");
						
						// Clean out inventories that are committed and maybe ask for some blocks //
						final int numShardGroups = SyncHandler.this.context.getLedger().numShardGroups();
						final ShardGroupID localShardGroupID = ShardMapper.toShardGroup(SyncHandler.this.context.getNode().getIdentity(), numShardGroups);
						final StandardConnectionFilter standardPeerFilter = StandardConnectionFilter.build(SyncHandler.this.context).setShardGroupID(localShardGroupID).setStates(ConnectionState.CONNECTED);
						final List<AbstractConnection> syncPeers = SyncHandler.this.context.getNetwork().get(standardPeerFilter);

						SyncHandler.this.lock.lock();
						try
						{
							if (syncPeers.isEmpty() == false)
							{
								final int numSyncBlockRequestsPerPeer = MULTIPLE_SYNC_BLOCK_REQUESTS ? Math.max(1, MathUtils.sqr(Constants.DEFAULT_TCP_CONNECTIONS_OUT_SYNC) / syncPeers.size()) : 1;
								for (AbstractConnection syncPeer : syncPeers)
								{
									if (SyncHandler.this.blockInventories.containsKey(syncPeer) == false)
										continue;
	
									int numRequestTasksAvailable = numSyncBlockRequestsPerPeer;
									try
									{
										synchronized(SyncHandler.this.requestTasks)
										{
											final Collection<SyncRequestTask> pendingRequestTasks = SyncHandler.this.requestTasks.get(syncPeer);
											numRequestTasksAvailable -= pendingRequestTasks.size();
											for (SyncRequestTask pendingRequestTask : pendingRequestTasks)
											{
												if (pendingRequestTask.completed())
													numRequestTasksAvailable++;
											}
											
											if (numRequestTasksAvailable <= 0)
												continue;
										}
	
										synchronized(SyncHandler.this.blockInventories)
										{
											final Iterator<Hash> inventoryIterator = SyncHandler.this.blockInventories.get(syncPeer).iterator();
											while(inventoryIterator.hasNext())
											{
												Hash block = inventoryIterator.next();
												final long inventoryBlockHeight = Block.toHeight(block);
												if (SyncHandler.this.blocks.containsKey(block) || inventoryBlockHeight <= SyncHandler.this.syncHead.getHeight()) 
												{
													inventoryIterator.remove();
													if (syncLog.hasLevel(Logging.DEBUG))
														syncLog.debug(SyncHandler.this.context.getName()+": Removing "+block+" from block inventory for "+syncPeer);
												}
												
												if (SyncHandler.this.context.getLedger().getLedgerStore().has(StateAddress.from(Block.class, block))) 
												{
													if (inventoryBlockHeight > SyncHandler.this.syncHead.getHeight())
													{
														syncLog.warn(SyncHandler.this.context.getName()+": Inventory block "+inventoryBlockHeight+"@"+block+" is committed after sync head "+SyncHandler.this.syncHead+" in block inventory for "+syncPeer);
															
														BlockHeader header = SyncHandler.this.context.getLedger().getLedgerStore().get(block, BlockHeader.class);
														SyncHandler.this.syncHead = header;
													}
												}
												else
													break;
											}
										}
									}
									catch (Exception ex)
									{
										syncLog.error(SyncHandler.this.context.getName()+": Inventory management of "+syncPeer+" failed", ex);
									}
	
									// Fetch some blocks if block cache is below threshold?
									if (SyncHandler.this.blocks.size() < Constants.MAX_BROADCAST_INVENTORY_ITEMS)
									{
										try
										{
											synchronized(SyncHandler.this.blockInventories)
											{
												if (SyncHandler.this.blockInventories.get(syncPeer).isEmpty() == false)
												{
													final Iterator<Hash> inventoryIterator = SyncHandler.this.blockInventories.get(syncPeer).iterator();
													while(inventoryIterator.hasNext())
													{
														Hash block = inventoryIterator.next();
														if (SyncHandler.this.blocksRequested.containsKey(block))
															continue;
													
														if (SyncHandler.this.blocks.containsKey(block))
															continue;
	
														if (SyncHandler.this.request(syncPeer, block))
														{
															numRequestTasksAvailable--;
														
															if (numRequestTasksAvailable == 0)
																break;
														}
													}
												}
											}
										}
										catch (Exception ex)
										{
											syncLog.error(SyncHandler.this.context.getName()+": Block request from "+syncPeer+" failed", ex);
										}
									}
								}
							}
							
							// Find best committable branch //
							final List<Block> syncBlocks;
							synchronized(SyncHandler.this.blocks)
							{
								syncBlocks = new ArrayList<Block>(SyncHandler.this.blocks.values());
								Collections.sort(syncBlocks, Collections.reverseOrder());
							}

							// Find a canonical branch section
							final List<Block> branchBlocks = new ArrayList<Block>();

							// Should be in highest order
							int lastHeight = -1;
							for(int i = 0 ; i < syncBlocks.size() ; i++)
							{
								final Block syncBlock = syncBlocks.get(i);
								
								// Stale?
								if (syncBlock.getHeader().getHeight() <= SyncHandler.this.syncHead.getHeight())
								{
									SyncHandler.this.blocks.remove(syncBlock.getHash());
									continue;
								}

								// Check duplicates / forks
								if (syncBlock.getHeader().getHeight() == lastHeight)
								{
									syncLog.fatal(SyncHandler.this.context.getName()+": Detected competing blocks at "+syncBlock.getHeader().getHeight());
									continue;
								}

								if (SyncHandler.this.blocksRequested.containsKey(syncBlock.getHeader().getPrevious()))
								{
									// Cant be canonical, clear branch
									branchBlocks.clear();
									continue;
								}
									
								final boolean atRoot = syncBlock.getHeader().getPrevious().equals(SyncHandler.this.syncHead.getHash());
								final Block prevSyncBlock = SyncHandler.this.blocks.get(syncBlock.getHeader().getPrevious());
								if (prevSyncBlock == null && atRoot == false)
								{
									if (syncLog.hasLevel(Logging.DEBUG))
										syncLog.warn(SyncHandler.this.context.getName()+": Previous sync block "+syncBlock.getHeader().getPrevious()+" of "+syncBlock.getHeader().toString()+" is not yet known");
										
									// Cant be canonical, clear branch
									branchBlocks.clear();
									continue;
								}
								
								branchBlocks.add(syncBlock);
								
								if (atRoot == true)
									break;
							}
							
							// Add discovered sync blocks to branch and determine a commit
							if (branchBlocks.isEmpty() == false)
							{
								final SyncBranch syncBranch = new SyncBranch(SyncHandler.this.context, SyncHandler.this.syncHead);
								for (final Block branchBlock : branchBlocks)
									syncBranch.push(branchBlock.getHeader());
								
								syncLog.info(SyncHandler.this.context.getName()+": Discovered sync branch "+syncBranch.getRoot()+" -> "+syncBranch.getHigh()+" as commit candidate");

								if (syncBranch.isCanonical() == false)
								{
									syncLog.fatal(SyncHandler.this.context.getName()+": Selected branch in range:");
									syncLog.fatal(SyncHandler.this.context.getName()+":   R: "+syncBranch.getRoot());
									syncLog.fatal(SyncHandler.this.context.getName()+":   L: "+syncBranch.getLow());
									syncLog.fatal(SyncHandler.this.context.getName()+":   H: "+syncBranch.getHigh());
									syncLog.fatal(SyncHandler.this.context.getName()+":   does not link to "+SyncHandler.this.syncHead);
									Thread.sleep(1000);
								}
								else
								{
									try
									{
										commit(syncBranch);
									}
									catch (Exception ex)
									{
										syncLog.error(SyncHandler.this.context.getName()+": Commit of branch with head "+syncBranch.getHigh()+" failed, resetting", ex);
										SyncHandler.this.reset();
									}
								}
							}
							
							// Fetch some inventory? //
							if (syncPeers.isEmpty() == false)
							{
								for (final AbstractConnection connection : syncPeers)
								{
									if (SyncHandler.this.syncHead.isAheadOf(connection.getNode().getHead()))
										continue;
									
									if (SyncHandler.this.inventoryTasks.containsKey(connection) == false &&
										(SyncHandler.this.blockInventories.containsKey(connection) == false || SyncHandler.this.blockInventories.get(connection).size() < Constants.MAX_BROADCAST_INVENTORY_ITEMS))
									{
										SyncInventoryTask syncInventoryTask = null;
										Hash highestInventory = SyncHandler.this.syncHead.getHash();
										if (SyncHandler.this.blockInventories.containsKey(connection) == true && SyncHandler.this.blockInventories.get(connection).isEmpty() == false) 
											highestInventory = SyncHandler.this.blockInventories.get(connection).last();
	
										try
										{
											final GetSyncBlockInventoryMessage getSyncBlockInventoryMessage = new GetSyncBlockInventoryMessage(highestInventory);
											
											syncInventoryTask = new SyncInventoryTask(connection, highestInventory, getSyncBlockInventoryMessage.getSeq());
											SyncHandler.this.inventoryTasks.put(connection, syncInventoryTask);
											Executor.getInstance().schedule(syncInventoryTask);
											
											SyncHandler.this.context.getNetwork().getMessaging().send(getSyncBlockInventoryMessage, connection);
											syncLog.info(SyncHandler.this.context.getName()+": Requested block inventory "+getSyncBlockInventoryMessage.getSeq()+":"+Block.toHeight(highestInventory)+"@"+highestInventory+" from "+connection);
										}
										catch (Exception ex)
										{
											if (syncInventoryTask != null)
												syncInventoryTask.cancel();
											
											syncLog.error(SyncHandler.this.context.getName()+": Unable to send GetSyncBlockInventoryMessage from "+highestInventory+" to "+connection, ex);
										}
									}
								}
							}
						}
						finally
						{
							SyncHandler.this.lock.unlock();
						}
					} 
					catch (InterruptedException e) 
					{
						// DO NOTHING //
						continue;
					}
				}
			}
			catch (Throwable throwable)
			{
				// TODO want to actually handle this?
				syncLog.fatal(SyncHandler.this.context.getName()+": Error processing sync", throwable);
			}
		}
	};

	SyncHandler(Context context)
	{
		this.context = Objects.requireNonNull(context);

		this.atoms = Collections.synchronizedMap(new HashMap<Hash, PendingAtom>());
		this.blocks = Collections.synchronizedMap(new HashMap<Hash, Block>());
		this.requestTasks = Multimaps.synchronizedSetMultimap(HashMultimap.create());
		this.inventoryTasks = Collections.synchronizedMap(new HashMap<AbstractConnection, SyncInventoryTask>());
		this.blocksRequested = Collections.synchronizedMap(new HashMap<Hash, Long>());
		this.blockInventories = Multimaps.synchronizedSortedSetMultimap(TreeMultimap.<AbstractConnection, Hash>create(Ordering.natural(), Ordering.natural()));
		this.stateVoteCollectors = Multimaps.synchronizedMultimap(HashMultimap.create());
	}

	@Override
	public void start() throws StartupException
	{
		try
		{
			this.context.getNetwork().getMessaging().register(SyncBlockMessage.class, this.getClass(), new MessageProcessor<SyncBlockMessage>()
			{
				@Override
				public void process(final SyncBlockMessage syncBlockMessage, final AbstractConnection connection)
				{
					SyncHandler.this.lock.lock();
					try
					{
						if (SyncHandler.this.isPrepared == false)
						{
							syncLog.warn(SyncHandler.this.context.getName()+": Received SyncBlockMessage, but sync is not prepared");
							return;
						}
	
						if (SyncHandler.this.isSynced == true)
						{
							syncLog.warn(SyncHandler.this.context.getName()+": Received SyncBlockMessage, but not out of sync");
							return;
						}

						if (syncBlockMessage.getBlock().getHeader().getHeight() <= SyncHandler.this.syncHead.getHeight())
						{
							syncLog.warn(SyncHandler.this.context.getName()+": Block is old "+syncBlockMessage.getBlock().getHeader()+" from "+connection);
							return;
						}
	
						if (SyncHandler.this.context.getLedger().getLedgerStore().has(StateAddress.from(Block.class, syncBlockMessage.getBlock().getHeader().getHash())))
						{
							syncLog.warn(SyncHandler.this.context.getName()+": Block is committed "+syncBlockMessage.getBlock().getHeader()+" from "+connection);
							return;
						}
	
//						if (syncLog.hasLevel(Logging.DEBUG))
							syncLog.info(SyncHandler.this.context.getName()+": Block "+syncBlockMessage.getBlock().getHeader().getHash()+" from "+connection);
						
						if (syncBlockMessage.getBlock().getHeader().getView() == null)
						{
							syncLog.warn(SyncHandler.this.context.getName()+": Block "+syncBlockMessage.getBlock().getHeader()+" does not have a view certificate!");
							return;
						}
						
						SyncHandler.this.blocks.put(syncBlockMessage.getBlock().getHash(), syncBlockMessage.getBlock());
						if (SyncHandler.this.blocksRequested.remove(syncBlockMessage.getBlock().getHash()) == null)
						{
							syncLog.error(SyncHandler.this.context.getName()+": Received unrequested block "+syncBlockMessage.getBlock().getHeader()+" from "+connection);
							connection.disconnect("Received unrequested block "+syncBlockMessage.getBlock().getHeader());
							return;
						}
					}
					catch (Exception ex)
					{
						syncLog.error(SyncHandler.this.context.getName()+": ledger.messages.block.sync "+connection, ex);
					}
					finally
					{
						SyncHandler.this.lock.unlock();
						SyncHandler.this.syncProcessorLatch.release();
					}
				}
			});
			
			this.context.getNetwork().getMessaging().register(GetSyncBlockMessage.class, this.getClass(), new MessageProcessor<GetSyncBlockMessage>()
			{
				@Override
				public void process(final GetSyncBlockMessage getSyncBlockMessage, final AbstractConnection connection)
				{
					try
					{
						if (syncLog.hasLevel(Logging.DEBUG))
							syncLog.debug(SyncHandler.this.context.getName()+": Block request for "+getSyncBlockMessage.getBlock()+" from "+connection);
						
						Block block = SyncHandler.this.context.getLedger().getLedgerStore().get(getSyncBlockMessage.getBlock(), Block.class);
						if (block == null)
						{
							syncLog.error(SyncHandler.this.context.getName()+": Requested block "+getSyncBlockMessage.getBlock()+" not found for "+connection);
							// TODO disconnect and ban?  Asking for blocks we don't have
							return;
						}
						
						try
						{
							SyncHandler.this.context.getNetwork().getMessaging().send(new SyncBlockMessage(block), connection);
						}
						catch (IOException ex)
						{
							syncLog.error(SyncHandler.this.context.getName()+": Unable to send SyncBlockMessage for "+getSyncBlockMessage.getBlock()+" to "+connection, ex);
						}
					}
					catch (Exception ex)
					{
						syncLog.error(SyncHandler.this.context.getName()+": ledger.messages.block.sync.get " +connection, ex);
					}
				}
			});
			
			this.context.getNetwork().getMessaging().register(GetSyncBlockInventoryMessage.class, this.getClass(), new MessageProcessor<GetSyncBlockInventoryMessage>()
			{
				@Override
				public void process(final GetSyncBlockInventoryMessage getSyncBlockInventoryMessage, final AbstractConnection connection)
				{
					try
					{
						if (syncLog.hasLevel(Logging.DEBUG))
							syncLog.info(SyncHandler.this.context.getName()+": Block inventory request from "+getSyncBlockInventoryMessage.getHead()+" from "+connection);

							final List<Hash> inventory = new ArrayList<Hash>();
						Hash current = getSyncBlockInventoryMessage.getHead();
						do
						{
							Hash next = SyncHandler.this.context.getLedger().getLedgerStore().getSyncBlock(Block.toHeight(current)+1);
							if (next != null)
								inventory.add(next);
							
							current = next;
						}
						while(current != null && inventory.size() < Constants.MAX_BROADCAST_INVENTORY_ITEMS);

						// Always respond, even if nothing to send
						final SyncBlockInventoryMessage syncBlockInventoryMessage;
						if (inventory.isEmpty())
							syncBlockInventoryMessage = new SyncBlockInventoryMessage(getSyncBlockInventoryMessage.getSeq());
						else
							syncBlockInventoryMessage = new SyncBlockInventoryMessage(getSyncBlockInventoryMessage.getSeq(), inventory);
							
						try
						{
							SyncHandler.this.context.getNetwork().getMessaging().send(syncBlockInventoryMessage, connection);
						}
						catch (IOException ex)
						{
							syncLog.error(SyncHandler.this.context.getName()+": Unable to send SyncBlockInventoryMessage "+getSyncBlockInventoryMessage.getSeq()+":"+getSyncBlockInventoryMessage.getHead()+" for "+inventory.size()+" blocks to "+connection, ex);
						}
					}
					catch (Exception ex)
					{
						syncLog.error(SyncHandler.this.context.getName()+": ledger.messages.block.sync.inv.get " + connection, ex);
					}
				}
			});
			
			this.context.getNetwork().getMessaging().register(SyncBlockInventoryMessage.class, this.getClass(), new MessageProcessor<SyncBlockInventoryMessage>()
			{
				@Override
				public void process(final SyncBlockInventoryMessage syncBlockInventoryMessage, final AbstractConnection connection)
				{
					SyncHandler.this.lock.lock();
					try
					{
						if (SyncHandler.this.isPrepared == false)
						{
							syncLog.warn(SyncHandler.this.context.getName()+": Received SyncBlockInventoryMessage, but sync is not prepared");
							return;
						}
						
						if (syncBlockInventoryMessage.getInventory().isEmpty())
							syncLog.warn(SyncHandler.this.context.getName()+": Received empty block inventory from " + connection);
						else if (syncLog.hasLevel(Logging.DEBUG))
							syncLog.debug(SyncHandler.this.context.getName()+": Received block header inventory "+syncBlockInventoryMessage.getInventory()+" from " + connection);

						final SyncInventoryTask syncInventoryTask = SyncHandler.this.inventoryTasks.get(connection);
						if (syncInventoryTask == null || syncInventoryTask.requestSeq != syncBlockInventoryMessage.getResponseSeq())
						{
							syncLog.error(SyncHandler.this.context.getName()+": Received unrequested inventory "+syncBlockInventoryMessage.getResponseSeq()+" with "+syncBlockInventoryMessage.getInventory().size()+" items from "+connection);
							connection.disconnect("Received unrequested inventory "+syncBlockInventoryMessage.getResponseSeq()+" with "+syncBlockInventoryMessage.getInventory().size()+" items from "+connection);
							return;
						}
						
						if (syncBlockInventoryMessage.getInventory().isEmpty() == false)
						{
							SyncHandler.this.blockInventories.putAll(connection, syncBlockInventoryMessage.getInventory());
							SyncHandler.this.syncProcessorLatch.release();
						}
						
						SyncHandler.this.inventoryTasks.remove(connection, syncInventoryTask);
					}
					catch (Exception ex)
					{
						syncLog.error(SyncHandler.this.context.getName()+": ledger.messages.block.header.inv "+connection, ex);
					}
					finally
					{
						SyncHandler.this.lock.unlock();
					}
				}
			});
			
			this.context.getEvents().register(this.peerListener);
			
			// SyncHandler starts as OOS, prepare the last known good state
			reset();
			prepare();
			
			// Handle singletons force sync state
			this.isSingleton = this.isPrepared && this.context.getConfiguration().get("ledger.synced.always", false);

			syncProcessorThread = new Thread(this.syncProcessor);
			syncProcessorThread.setDaemon(true);
			syncProcessorThread.setName(this.context.getName()+" Sync Processor");
			syncProcessorThread.start();
		}
		catch (Throwable t)
		{
			throw new StartupException(t);
		}
	}

	@Override
	public void stop() throws TerminationException
	{
		this.syncProcessor.terminate(true);
		this.syncProcessorThread.interrupt();
		this.syncProcessorThread = null;
		
		this.context.getEvents().unregister(this.peerListener);
		this.context.getNetwork().getMessaging().deregisterAll(this.getClass());
	}
	
	private void reset()
	{
		this.lock.lock();
		try
		{
			this.atoms.clear();
			this.blockInventories.clear();
			this.blocks.clear();
			this.blocksRequested.clear();
			this.stateVoteCollectors.clear();
			
			final Collection<SyncRequestTask> requestTasks = new ArrayList<SyncRequestTask>(this.requestTasks.values());
			for (SyncRequestTask requestTask : requestTasks)
				requestTask.cancel();
			this.requestTasks.clear();
			
			final Collection<SyncInventoryTask> inventoryTasks = new ArrayList<SyncInventoryTask>(this.inventoryTasks.values());
			for (SyncInventoryTask inventoryTask : inventoryTasks)
				inventoryTask.cancel();
			this.inventoryTasks.clear();
			
			this.isPrepared = false;
		}
		finally
		{
			this.lock.unlock();
		}
	}
	
	private boolean request(final AbstractConnection connection, final Hash block) throws IOException
	{
		SyncRequestTask syncRequestTask = null;
		SyncHandler.this.lock.lock();
		try
		{
			if (this.blocksRequested.containsKey(block))
			{
				syncLog.warn(SyncHandler.this.context.getName()+": Block "+block+" is already requested when requesting from "+connection);
				return false;
			}
		
			final long requestSeq = ThreadLocalRandom.current().nextLong();
			this.blocksRequested.put(block, requestSeq);
			syncRequestTask = new SyncRequestTask(connection, block, requestSeq);
			this.requestTasks.put(connection, syncRequestTask);
			Executor.getInstance().schedule(syncRequestTask);
			
//			if (syncLog.hasLevel(Logging.DEBUG))
				syncLog.info(SyncHandler.this.context.getName()+": Requesting block "+block+" from "+connection);
	
			GetSyncBlockMessage getBlockMessage = new GetSyncBlockMessage(block); 
			this.context.getNetwork().getMessaging().send(getBlockMessage, connection);
		}
		catch (Throwable t)
		{
			if (syncRequestTask != null)
			{
				if (syncRequestTask.cancel() == true)
					this.requestTasks.remove(connection, syncRequestTask);
			}
			
			this.blocksRequested.remove(block);
			
			throw t;
		}
		finally
		{
			SyncHandler.this.lock.unlock();
		}
		
		return true;
	}

	/**
	 * Prepares the SyncHandler for a sync attempt, loading the latest known snapshot of state
	 * 
	 * @throws IOException 
	 * @throws ValidationException 
	 * @throws CryptoException 
	 * @throws StateMachineExecutionException 
	 * @throws ManifestException 
	 * @throws StateLockedException 
	 */
	private void prepare() throws IOException, ValidationException, LockException, CryptoException, StateMachineException, ManifestException
	{
		final Entry<BlockHeader, StateAccumulator> ledgerState = this.context.getLedger().current("sync");
		this.syncHead = ledgerState.getKey();
		this.syncAccumulator = ledgerState.getValue();
		this.desyncHead = this.syncHead;

		// Nothing to prepare if at genesis head
		if (this.desyncHead.getHash().equals(Universe.get().getGenesis().getHash()))
		{
			this.isPrepared = true;
			return;
		}
		
		// Signal to any sub-systems that they must prepare for a sync
		this.context.getEvents().post(new SyncPrepareEvent(this.desyncHead));
		
		// Collect the recent block headers within the commit timeout window from the head 
		final LinkedList<BlockHeader> recentBlockHeaders = new LinkedList<BlockHeader>();
		
		// Fairly large replay space to account for latency between commit timeout 
		// being triggered and inclusion of a timeout in an actual block
		BlockHeader current = this.context.getLedger().getLedgerStore().get(this.desyncHead.getHash(), BlockHeader.class);
		do
		{
			recentBlockHeaders.add(current);
			if (current.getPrevious().equals(Universe.get().getGenesis().getHash()))
				break;
			
			current = this.context.getLedger().getLedgerStore().get(current.getPrevious(), BlockHeader.class);
		}
		while (current.getHeight() >= this.desyncHead.getHeight() - Constants.OOS_PREPARE_BLOCKS);
		
		Collections.reverse(recentBlockHeaders);
		
		for (final BlockHeader header : recentBlockHeaders)
		{
			final Block block = this.context.getLedger().getLedgerStore().get(header.getHash(), Block.class);

			//  Process certificates contained in block
			for (final AtomCertificate certificate : block.getCertificates())
			{
				final PendingAtom pendingAtom;
				if (this.atoms.containsKey(certificate.getAtom()) == false)
				{
					final Atom atom = this.context.getLedger().getLedgerStore().get(certificate.getAtom(), Atom.class);
					if (atom == null)
						throw new ValidationException(certificate, "Pending atom "+certificate.getAtom()+" not found for certificate "+certificate.getHash()+" in block "+block.getHash());
					
					final SubstateCommit substateCommit = this.context.getLedger().getLedgerStore().search(StateAddress.from(Atom.class, certificate.getAtom()));
					if (substateCommit == null)
						throw new ValidationException(certificate, "Substate for pending atom "+certificate.getAtom()+" not found for certificate "+certificate.getHash()+" in block "+block.getHash());
					
					final BlockHeader commitHeader = this.context.getLedger().getLedgerStore().get(substateCommit.getSubstate().<Hash>get(NativeField.BLOCK), BlockHeader.class);
					if (commitHeader == null)
						throw new ValidationException(certificate, "Block header "+substateCommit.getSubstate().get(NativeField.BLOCK)+" for pending atom "+certificate.getAtom()+" not found for certificate "+certificate.getHash()+" in block "+block.getHash());
						
					pendingAtom = prepare(new Atom(atom), commitHeader);
				}
				else
					pendingAtom = this.atoms.get(certificate.getAtom());
				
				List<StateInput> stateInputs = new ArrayList<StateInput>(certificate.getInventorySize());
				for (StateOutput stateOutput : certificate.getInventory(StateOutput.class))
					stateInputs.add(new StateInput(pendingAtom.getHash(), stateOutput.getStates().toSubstate(SubstateOutput.READS)));
				provision(pendingAtom, stateInputs, block.getPackages());

				pendingAtom.complete(certificate);
				
				vote(pendingAtom);
				
				this.syncAccumulator.unlock(pendingAtom);
				this.atoms.remove(pendingAtom.getHash());
			}
			
			// Process the atoms contained in block
			for (final Atom atom : block.getAccepted())
				prepare(new Atom(atom), block.getHeader());

			// Process execute signals in block
			for (final Hash executable : block.getExecutables())
			{
				PendingAtom pendingAtom = this.atoms.get(executable); 
				if (pendingAtom == null)
				{
					final Atom atom = this.context.getLedger().getLedgerStore().get(executable, Atom.class);
					if (atom == null)
						throw new ValidationException("Pending atom "+executable+" for executable signal not found in block "+block.getHash());
					
					final SubstateCommit substateCommit = this.context.getLedger().getLedgerStore().search(StateAddress.from(Atom.class, executable));
					if (substateCommit == null)
						throw new ValidationException("Substate for pending atom "+executable+" with executable signal not found in block "+block.getHash());
					
					final BlockHeader commitHeader = this.context.getLedger().getLedgerStore().get(substateCommit.getSubstate().<Hash>get(NativeField.BLOCK), BlockHeader.class);
					if (commitHeader == null)
						throw new ValidationException("Block header "+substateCommit.getSubstate().get(NativeField.BLOCK)+" for pending atom "+executable+" with executable signal not found in block "+block.getHash());
						
					pendingAtom = prepare(new Atom(atom), commitHeader);
				}
				
				pendingAtom.setExecuteSignalledAtBlock(block.getHeader());
			}

			// Process latent execution signals in block
			for (final Hash latent : block.getLatent())
			{
				PendingAtom pendingAtom = this.atoms.get(latent); 
				if (pendingAtom == null)
				{
					final Atom atom = this.context.getLedger().getLedgerStore().get(latent, Atom.class);
					if (atom == null)
						throw new ValidationException("Pending atom "+latent+" for latent execution signal not found in block "+block.getHash());
						
					final SubstateCommit substateCommit = this.context.getLedger().getLedgerStore().search(StateAddress.from(Atom.class, latent));
					if (substateCommit == null)
						throw new ValidationException("Substate for pending atom "+latent+" with latent execution signal not found in block "+block.getHash());
					
					final BlockHeader commitHeader = this.context.getLedger().getLedgerStore().get(substateCommit.getSubstate().<Hash>get(NativeField.BLOCK), BlockHeader.class);
					if (commitHeader == null)
						throw new ValidationException("Block header "+substateCommit.getSubstate().get(NativeField.BLOCK)+" for pending atom "+latent+" with latent execution signal not found in block "+block.getHash());
					
					pendingAtom = prepare(new Atom(atom), commitHeader);
				}

				processExecuteLatent(block.getHeader(), pendingAtom);
			}

			// Process unexecuted/ uncommited atoms in block
			final List<AtomTimeout> timeouts = new ArrayList<AtomTimeout>();
			timeouts.addAll(block.getUnexecuted()); timeouts.addAll(block.getUncommitted());
			
			// Phase 1 apply the timeouts to the pending atoms and perform the execute & SVC voting
			for (final AtomTimeout timeout : timeouts)
			{
				PendingAtom pendingAtom = this.atoms.get(timeout.getAtom());
				if (pendingAtom == null)
				{
					final Atom atom = this.context.getLedger().getLedgerStore().get(timeout.getAtom(), Atom.class);
					if (atom == null)
						throw new ValidationException(timeout, "Pending atom "+timeout.getAtom()+" not found for timeout "+timeout.getHash()+" in block "+block.getHash());
						
					final SubstateCommit substateCommit = this.context.getLedger().getLedgerStore().search(StateAddress.from(Atom.class, timeout.getAtom()));
					if (substateCommit == null)
						throw new ValidationException(timeout, "Commit for pending atom "+timeout.getAtom()+" with timeout "+timeout.getHash()+" found in block "+block.getHash());
					
					final BlockHeader commitHeader = this.context.getLedger().getLedgerStore().get(substateCommit.getSubstate().<Hash>get(NativeField.BLOCK), BlockHeader.class);
					if (commitHeader == null)
						throw new ValidationException(timeout, "Block header "+substateCommit.getSubstate().get(NativeField.BLOCK)+" for pending atom "+timeout.getAtom()+" with timeout "+timeout.getHash()+" not found in block "+block.getHash());

					pendingAtom = prepare(new Atom(atom), commitHeader);
				}

				if (timeout instanceof CommitTimeout commitTimeout)
				{
					provision(pendingAtom, commitTimeout.getStateInputs(), block.getPackages());

					pendingAtom.execute();

					vote(pendingAtom);
				}
				
				pendingAtom.setTimeout(timeout);
				
				this.syncAccumulator.unlock(pendingAtom);
			}
			
			// Phase 2 modify the SVCs with respect to the timeouts.  
			// IMPORTANT: Needed so that remaining SVCs post sync can be completed and validated correctly.
			for (final AtomTimeout timeout : timeouts)
			{
				final PendingAtom pendingAtom = this.atoms.remove(timeout.getAtom());
				if (pendingAtom == null)
					throw new ValidationException(timeout, "Pending atom "+timeout.getAtom()+" not found when processing timeout in block "+block.getHash());

				processTimeout(pendingAtom, timeout);
			}
			
			// Don't need to do anything with unaccepted here
			
			// Create state vote collector for this block
			createStateVoteCollector(block);
			
			// Remove any state vote collectors made stale by this block
			removeStaleStateVoteCollectors();
		}
		
		this.isPrepared = true;
		syncLog.info(this.context.getName()+": State accumulator prepared "+this.syncAccumulator.locked().size()+" locked "+this.syncAccumulator.checksum());
	}

	private void commit(final SyncBranch branch) throws IOException, ValidationException, LockException, CryptoException, StateMachineException, ManifestException
	{
		// TODO limited validation here currently, just accepts blocks and atoms almost on faith
		// TODO remove reference to ledger StateAccumulator and use a local instance with a push on sync
		final List<BlockHeader> supers = branch.supers();
		if (supers.size() < Constants.MIN_COMMIT_SUPERS)
			return;

        BlockHeader previousSuper = null;
        BlockHeader superToCommit = null;
        for (final BlockHeader zuper : supers)
        {
            if (previousSuper != null && previousSuper.getHash().equals(zuper.getPrevious())) 
                superToCommit = previousSuper;
            
            previousSuper = zuper;
        }
        
        if (superToCommit == null)
        	return;
		
        final Hash commitBlocksTo = superToCommit.getView().getCommittable();
		final List<Block> committedBlocks = new ArrayList<Block>();
		final List<PendingAtom> committedAtoms = new ArrayList<PendingAtom>();
		final List<CommitOperation> commitOperations = new ArrayList<CommitOperation>();

		final Iterator<BlockHeader> blockHeaderIterator = branch.getHeaders().iterator();
		while(blockHeaderIterator.hasNext())
		{
			final BlockHeader header = blockHeaderIterator.next();
			final Block block = this.blocks.get(header.getHash());
			
			// Persist the packages contained in block, may have already been seen but not committed
			for (final PolyglotPackage pakage : block.getPackages())
			{
				if (this.context.getLedger().getLedgerStore().has(pakage.getHash(), PolyglotPackage.class) == false)
					this.context.getLedger().getLedgerStore().store(pakage);
				else
					syncLog.warn(this.context.getName()+": Package "+pakage+" in block header "+header.getHeight()+"@"+header.getHash()+" already seen and persisted before de-sync");
			}

			// Provision any missing atoms required by certificates
			for (final AtomCertificate certificate : block.getCertificates())
			{
				PendingAtom pendingAtom = this.atoms.get(certificate.getAtom());
				if (pendingAtom != null)
					continue;
				
				final SubstateCommit substateCommit = this.context.getLedger().getLedgerStore().search(StateAddress.from(Atom.class, certificate.getAtom()));
				if (substateCommit == null)
					throw new ValidationException(certificate, "Atom "+certificate.getAtom()+" required for provisioning by certificate "+certificate.getHash()+" not committed");
				
				final Atom atom = this.context.getLedger().getLedgerStore().get(substateCommit.getSubstate().<Hash>get(NativeField.ATOM), Atom.class);
				if (atom == null)
					throw new ValidationException(certificate, "Atom "+certificate.getAtom()+" required for provisioning by certificate "+certificate.getHash()+" committed but not found");
				
				final BlockHeader commitHeader = this.context.getLedger().getLedgerStore().get(substateCommit.getSubstate().<Hash>get(NativeField.BLOCK), BlockHeader.class);
				if (commitHeader == null)
					throw new ValidationException(certificate, "Atom "+certificate.getAtom()+" required for provisioning by certificate "+certificate.getHash()+" committed but block "+substateCommit.getSubstate().get(NativeField.BLOCK)+" not found");
				
				prepare(new Atom(atom), commitHeader);
			}
			
			// Provision atoms contained in block 
			for (final Atom atom : block.getAccepted())
				prepare(new Atom(atom), block.getHeader());
			
			//  Process certificates contained in block
			for (final AtomCertificate certificate : block.getCertificates())
			{
				PendingAtom pendingAtom = this.atoms.get(certificate.getAtom());
				if (pendingAtom == null)
					throw new ValidationException(certificate, "Pending atom "+certificate.getAtom()+" not found for certificate "+certificate.getHash()+" in block "+block.getHash());
				
				List<StateInput> stateInputs = new ArrayList<StateInput>(certificate.getInventorySize());
				for (StateOutput stateOutput : certificate.getInventory(StateOutput.class))
					stateInputs.add(new StateInput(pendingAtom.getHash(), stateOutput.getStates().toSubstate(SubstateOutput.READS)));
				provision(pendingAtom, stateInputs, block.getPackages());

				pendingAtom.execute();
				
				vote(pendingAtom);

				pendingAtom.setCertificate(certificate);
				
				committedAtoms.add(pendingAtom);
				commitOperations.add(pendingAtom.getCommitOperation());

				this.syncAccumulator.unlock(pendingAtom);
				this.atoms.remove(pendingAtom.getHash());
			}
			
			// Process execution signals, but don't actually execute on the signal processing.
			// If a AtomCertificates are received via sync for executable signalled Atom we can
			// bypass the execution and "trust" the outputs of the certificates.
			for (final Hash executable : block.getExecutables())
			{
				PendingAtom pendingAtom = this.atoms.get(executable);
				if (pendingAtom == null)
					throw new ValidationException("Pending atom "+executable+" not found when processing execution signal in block "+block.getHash());

				if (pendingAtom.getStatus().before(AtomStatus.State.ACCEPTED) || pendingAtom.getStatus().after(AtomStatus.State.FINALIZING))
					syncLog.warn(this.context.getName()+": Pending atom "+executable+" has invalid execution status "+pendingAtom.getStatus());

				pendingAtom.setExecuteSignalledAtBlock(block.getHeader());
			}

			// Process latent execution signals
			for (final Hash latent : block.getLatent())
			{
				PendingAtom pendingAtom = this.atoms.get(latent);
				if (pendingAtom == null)
					throw new ValidationException("Pending atom "+latent+" not found when processing execution latency signal in block "+block.getHash());

				if (pendingAtom.getStatus().before(AtomStatus.State.ACCEPTED) || pendingAtom.getStatus().after(AtomStatus.State.FINALIZING))
					syncLog.warn(this.context.getName()+": Pending atom "+latent+" has invalid execution latency status "+pendingAtom.getStatus());

				processExecuteLatent(block.getHeader(), pendingAtom);
			}
			
			// Process atom timeouts in block
			final List<AtomTimeout> timeouts = new ArrayList<AtomTimeout>();
			timeouts.addAll(block.getUnexecuted()); timeouts.addAll(block.getUncommitted());
			
			// Phase 1 apply the timeouts to the pending atoms and perform the execute & SVC voting
			for (final AtomTimeout timeout : timeouts)
			{
				PendingAtom pendingAtom = this.atoms.get(timeout.getAtom());
				if (pendingAtom == null)
					throw new ValidationException(timeout, "Pending atom "+timeout.getAtom()+" not found when processing timeout in block "+block.getHash());

				if (pendingAtom.getStatus().before(AtomStatus.State.ACCEPTED))
					syncLog.warn(this.context.getName()+": Pending atom "+timeout.getAtom()+" is not ACCEPTED for timeout");

				if (timeout instanceof CommitTimeout commitTimeout)
				{
					provision(pendingAtom, commitTimeout.getStateInputs(), block.getPackages());

					pendingAtom.execute();

					vote(pendingAtom);
				}
				
				pendingAtom.setTimeout(timeout);

				this.syncAccumulator.unlock(pendingAtom);
			}
			
			// Phase 2 modify the SVCs with respect to the timeouts.  
			// IMPORTANT: Needed so that remaining SVCs post sync can be completed and validated correctly.
			for (final AtomTimeout timeout : timeouts)
			{
				PendingAtom pendingAtom = this.atoms.remove(timeout.getAtom());
				if (pendingAtom == null)
					throw new ValidationException(timeout, "Pending atom "+timeout.getAtom()+" not found when processing timeout in block "+block.getHash());

				if (pendingAtom.getStatus().before(AtomStatus.State.ACCEPTED))
					syncLog.warn(this.context.getName()+": Pending atom "+timeout.getAtom()+" is not ACCEPTED for timeout");

				processTimeout(pendingAtom, timeout);
			}

			// Process unaccepted
			for (final Atom atom : block.getUnaccepted())
			{
				PendingAtom pendingAtom = this.atoms.remove(atom.getHash());
				if (pendingAtom != null)
					throw new ValidationException(atom, "Found pending atom "+atom.getHash()+" when processing unaccepted in block "+block.getHash());
			}
			
			// Store the block header in preparation for a commit
			if (this.context.getLedger().getLedgerStore().has(header.getHash(), BlockHeader.class) == false)
				this.context.getLedger().getLedgerStore().store(header);
			else
				syncLog.warn(this.context.getName()+": Block header "+header.getHeight()+"@"+header.getHash()+" already seen and persisted before de-sync");

			this.context.getLedger().getLedgerStore().commit(block, commitOperations);
			
			for(int i = 0 ; i < committedAtoms.size() ; i++)
			{
				final PendingAtom committedAtom = committedAtoms.get(i);
				this.context.getEvents().post(new SyncAtomCommitEvent(block.getHeader(), committedAtom));
			}

			committedAtoms.clear();
			commitOperations.clear();
			committedBlocks.add(block);

			// Create state vote collector for this block
			createStateVoteCollector(block);
			
			if (block.getHeader().getHash().equals(commitBlocksTo))
				break;
		}
		
		// Remove any state vote collectors made stale by this branch portion
		removeStaleStateVoteCollectors();
		
		for (Block committedBlock : committedBlocks)
		{
			this.syncHead = committedBlock.getHeader(); 
			this.blocks.remove(committedBlock.getHash());
			
			// TODO Probably need to catch exceptions on this from synchronous listeners
			this.context.getEvents().post(new SyncBlockEvent(committedBlock)); 
		}
		
		if (syncLog.hasLevel(Logging.INFO))
			syncLog.info(SyncHandler.this.context.getName()+": Committed blocks in range "+committedBlocks.getFirst()+" -> "+committedBlocks.getLast());

		this.syncProcessorLatch.release();
	}
	
	public void desyncFor(final long duration, final TimeUnit unit)
	{
		this.forcedDesyncDuration = unit.toMillis(duration);
	}
	
	private PendingAtom prepare(final Atom atom, final BlockHeader header) throws ManifestException, IOException, StateMachineException, LockException
	{
		final PendingAtom pendingAtom = new PendingAtom(this.context, atom, header.getTimestamp());
		
		// Prepare statemachine and meta info
		pendingAtom.prepare();
		
		// Set as accepted with the provided header
		pendingAtom.accepted(header);
		
		// Update the state accumulator with the locks required
		this.syncAccumulator.lock(pendingAtom);
		
		// Add to known sync atoms, should not already exist
		if (this.atoms.putIfAbsent(pendingAtom.getHash(), pendingAtom) != null)
			throw new IllegalStateException("Pending atom "+pendingAtom.getHash()+" is already known and potentially loaded & prepared");
		
		return pendingAtom;
	}
	
	private void provision(final PendingAtom pendingAtom, final Collection<StateInput> stateInputs, final Collection<PolyglotPackage> packages) throws StateMachinePreparationException
	{
		pendingAtom.provision();
		
		// Provision all the state inputs (will load any known or embedded packages too)
		for (final StateInput stateInput : stateInputs)
			pendingAtom.provision(stateInput);
		
		// Might need to load some packages if not yet provisioned
		if (pendingAtom.isProvisioned() == false)
		{
			final Collection<PolyglotPackage> loadedPackages = pendingAtom.load(packages, false);
			if (loadedPackages.isEmpty() == false && syncLog.hasLevel(Logging.DEBUG))
				syncLog.debug(this.context.getName()+": Loaded packages for atom "+pendingAtom.getHash()+" "+loadedPackages.stream().map(p -> p.getAddress()).collect(Collectors.toList()));
		}
	}
	
	private SyncStatus checkSynced() throws IOException
	{
		final List<AbstractConnection> syncPeers; 
		final int numShardGroups = this.context.getLedger().numShardGroups();
		final ShardGroupID localShardGroupID = ShardMapper.toShardGroup(this.context.getNode().getIdentity(), numShardGroups);
		
		if (this.isSingleton == false)
		{
			final StandardConnectionFilter standardPeerFilter = StandardConnectionFilter.build(this.context).setShardGroupID(localShardGroupID).setStates(ConnectionState.CONNECTED);
			
			syncPeers = this.context.getNetwork().get(standardPeerFilter); 
			if (syncPeers.isEmpty())
			{
				if (isSynced())
				{
					syncLog.info(this.context.getName()+": Out of sync state triggered due to no sync peers available");
					syncLost();
				}
	
				return SyncStatus.CONNECTING;
			}
			
			// Forced desync?
			if (this.forcedDesyncDuration > 0)
			{
				if (isSynced())
				{
					syncLog.info(this.context.getName()+": Out of sync state triggered via forced desync");
					syncLost();
					
					try 
					{ 
						Thread.sleep(this.forcedDesyncDuration); 
					} 
					catch (InterruptedException ex) 
					{ 
						Thread.currentThread().interrupt();
						syncLog.warn(this.context.getName()+": Forced de-sync duration was interrupted", ex);
					}
				}
	
				this.forcedDesyncDuration = 0;
				return SyncStatus.UNSYNCED;
			}
		}
		else
			syncPeers = Collections.emptyList();
	
		SyncStatus syncStatus = isSynced() ? SyncStatus.SYNCED : SyncStatus.UNSYNCED;
		if (syncStatus.equals(SyncStatus.SYNCED))
		{
			// Out of sync?
			// TODO need to deal with forks that don't converge with local chain due to safety break
			boolean isUnsynced = false;
			if (isUnsynced == false && this.isSingleton == false)
			{
				// TODO check header certificate for a quorum of peers that are ahead
				// 	    2f+1 style method to go into unsynced
				for (final AbstractConnection syncPeer : syncPeers)
				{
					if (syncPeer.getNode().getHead().isInRange(this.context.getLedger().getHead(), Constants.OOS_TRIGGER_LIMIT_BLOCKS) == true)
						continue;
					
					if (syncPeer.getNode().getHead().isAheadOf(this.context.getLedger().getHead(), Constants.OOS_TRIGGER_LIMIT_BLOCKS) == true)
					{
						isUnsynced = true;
						break;
					}
				
					if (this.context.getLedger().getHead().getHash().equals(Universe.get().getGenesis().getHash()) == true && 
						syncPeer.getNode().getHead().isAheadOf(this.context.getLedger().getHead(), Constants.OOS_RESOLVED_LIMIT_BLOCKS) == true)
					{
						isUnsynced = true;
						break;
					}
				}
				
				if (syncPeers.isEmpty())
					isUnsynced = true;
			}
			else
				isUnsynced = this.isSingleton == false;

			if (isUnsynced)
			{
				syncLog.info(this.context.getName()+": Out of sync state detected with OOS_TRIGGER limit of "+Constants.OOS_TRIGGER_LIMIT_BLOCKS);
				System.out.println(this.context.getName()+": Out of sync state detected with OOS_TRIGGER limit of "+Constants.OOS_TRIGGER_LIMIT_BLOCKS);
				syncLost();
				
				syncStatus = SyncStatus.UNSYNCED;
			}
		}
		else if (syncStatus.equals(SyncStatus.UNSYNCED))
		{
			// Can consider in sync if singleton instance //
			syncStatus = this.isSingleton ? SyncStatus.SYNCED : SyncStatus.UNSYNCED;
			if (syncStatus.equals(SyncStatus.UNSYNCED))
			{
				// Set target sync tip based on validated blocks
				BlockHeader syncTip = this.syncHead;
				synchronized(this.blocks)
				{
					if (this.blocks.isEmpty() == false)
					{
						for (final Block block : this.blocks.values())
						{
							if (block.getHeader().getHeight() > syncTip.getHeight())
								syncTip = block.getHeader();
						}
					
						if (syncLog.hasLevel(Logging.INFO))
							syncLog.info(this.context.getName()+": Sync tip is "+syncTip+" sync head is "+this.syncHead.toString());
					}
					else
						syncLog.info(this.context.getName()+": Sync tip is sync head is "+this.syncHead.toString());
				}
				
				AbstractConnection strongestPeer = null;
				for (final AbstractConnection syncPeer : syncPeers)
				{
					if (strongestPeer == null || 
						syncPeer.getNode().getHead().getHeight() > strongestPeer.getNode().getHead().getHeight())
						strongestPeer = syncPeer;
				}
				
				if (strongestPeer != null && syncTip.isAheadOf(strongestPeer.getNode().getHead()) == true)
					syncStatus = SyncStatus.SYNCED;
			}
			
			// Is block processing completed and local replica considered in sync?
			if (syncStatus.equals(SyncStatus.SYNCED))
			{
				// Check we have a full quota of peers across the shard space
				final List<AbstractConnection> allPeers = this.context.getNetwork().get(StandardConnectionFilter.build(this.context).setStates(ConnectionState.CONNECTED));
				final boolean[] shardsConnected = new boolean[numShardGroups];
				for (final AbstractConnection connectedPeer : allPeers)
				{
					ShardGroupID shardGroupID = ShardMapper.toShardGroup(connectedPeer.getNode().getIdentity(), numShardGroups);
					shardsConnected[shardGroupID.intValue()] = true;
				}
				
				for (int i = 0 ; i < shardsConnected.length ; i++)
				{
					// Can skip own shard group if a singleton instance
					if (this.isSingleton && i == localShardGroupID.intValue())
						continue;
					
					if (shardsConnected[i] == false)
					{
						syncStatus = SyncStatus.CONNECTING;
						break;
					}
				}
				
				if (syncStatus.equals(SyncStatus.SYNCED))
				{
					// Tell all peers we're synced
					final SyncAcquiredMessage syncAcquiredMessage = syncAcquired();
					final NodeMessage nodeMessage = new NodeMessage(SyncHandler.this.context.getNode());
					for (final AbstractConnection connectedPeer : allPeers)
					{
						try
						{
							this.context.getNetwork().getMessaging().send(nodeMessage, connectedPeer);
						}
						catch (IOException ioex)
						{
							syncLog.error("Could not send synced declaration to "+connectedPeer, ioex);
						}
					}
					
					// Requests for current block pool, atom pool and state consensus primitives
					for (final AbstractConnection syncPeer : syncPeers)
					{
						try
						{
							this.context.getNetwork().getMessaging().send(syncAcquiredMessage, syncPeer);
						}
						catch (Exception ex)
						{
							syncLog.error(this.context.getName()+": Unable to send SyncAcquiredMessage "+syncAcquiredMessage+" to "+syncPeer, ex);
						}
					}
	
					syncLog.info(this.context.getName()+": Synced state reaquired "+this.syncHead.toString());
				}
			}
		}				
		
		return syncStatus;
	}
	
	void createStateVoteCollector(final Block block) throws IOException
	{
		final Epoch epoch = Epoch.from(block.getHeader());
		final int numShardGroups = this.context.getLedger().numShardGroups(epoch);
		final ShardGroupID localShardGroupID = ShardMapper.toShardGroup(this.context.getNode().getIdentity(), numShardGroups);
		final long votePower = this.context.getLedger().getValidatorHandler().getVotePower(epoch, this.context.getNode().getIdentity());
		final long voteThreshold = this.context.getLedger().getValidatorHandler().getVotePowerThreshold(epoch, localShardGroupID);

		// Create state vote collectors //
		final Multimap<ShardGroupID, PendingState> atomsByShardGroup = LinkedHashMultimap.create();
		for (final Hash atom : block.getHeader().getInventory(InventoryType.ACCEPTED))
		{
			final PendingAtom pendingAtom = this.atoms.get(atom);
			if (pendingAtom == null)
				throw new IllegalStateException("Pending atom "+atom+" required for SVC creation not found in proposal "+block.getHeader());
			
			if (pendingAtom.getStatus().before(AtomStatus.State.ACCEPTED))
				throw new IllegalStateException("Pending atom "+pendingAtom+" is not ACCEPTED for SVC creation in proposal "+block.getHeader());

			pendingAtom.forStates(StateLockMode.WRITE, pendingState -> {
				final ShardGroupID provisionShardGroupID = ShardMapper.toShardGroup(pendingState.getAddress(), numShardGroups);
				if (provisionShardGroupID.equals(localShardGroupID) == false)
					return;

				if (StateVoteCollector.DEBUG_SIMPLE_UNGROUPED)
					atomsByShardGroup.put(ShardGroupID.from(0), pendingState);
				else
					atomsByShardGroup.put(provisionShardGroupID, pendingState);
			});
		}
		
		for (final ShardGroupID shardGroupID : atomsByShardGroup.keySet())
		{
			final List<PendingState> stateVoteCollectorStates = new ArrayList<PendingState>(atomsByShardGroup.get(shardGroupID));
			if (stateVoteCollectorStates.isEmpty() == false)
			{
				if (syncLog.hasLevel(Logging.DEBUG))
					syncLog.debug(this.context.getName()+": Creating StateVoteCollector for proposal "+block.getHeader()+" with state keys "+stateVoteCollectorStates.stream().map(sk -> sk.getAddress()).collect(Collectors.toList()));
				
				final StateVoteCollector stateVoteCollector = new StateVoteCollector(this.context, block.getHash(), stateVoteCollectorStates, votePower, voteThreshold);
				for (final PendingState pendingState : stateVoteCollectorStates)
					pendingState.setStateVoteCollector(stateVoteCollector);
				this.stateVoteCollectors.put(block.getHash(), stateVoteCollector);
				
				this.context.getMetaData().increment("ledger.pool.state.vote.collectors.total");
				stateVoteCollectorStates.clear();
			}
		}
	}
	
	private Collection<StateVoteCollector> removeStaleStateVoteCollectors()
	{
		final List<StateVoteCollector> staleStateVoteCollectors = new ArrayList<StateVoteCollector>();
		// Clean up state vote collectors
		synchronized(this.stateVoteCollectors)
		{
			final Iterator<StateVoteCollector> stateVoteCollectorIterator = this.stateVoteCollectors.values().iterator();
			while(stateVoteCollectorIterator.hasNext())
			{
				final StateVoteCollector stateVoteCollector = stateVoteCollectorIterator.next();
				boolean shouldRemoveSVC = true;
				for (final PendingState pendingState : stateVoteCollector.getStates())
				{
					if (this.atoms.containsKey(pendingState.getAtom().getHash()))
					{
						shouldRemoveSVC = false;
						break;
					}
				}
				
				if (shouldRemoveSVC)
				{
					staleStateVoteCollectors.add(stateVoteCollector);
					stateVoteCollectorIterator.remove();
				}
			}
		}
		
		return staleStateVoteCollectors;
	}
	
	private void vote(final PendingAtom pendingAtom) throws IOException
	{
		Objects.requireNonNull(pendingAtom, "Atom is null");
		
		if (pendingAtom.getStatus().after(AtomStatus.State.ACCEPTED) == false)
			throw new IllegalStateException("Pending atom "+pendingAtom.getHash()+" is not ACCEPTED");
		
		final Epoch epoch = Epoch.from(pendingAtom.getBlockHeader());
		final long votePower = this.context.getLedger().getValidatorHandler().getVotePower(epoch, this.context.getNode().getIdentity());
		final int numShardGroups = this.context.getLedger().numShardGroups(epoch);
		final ShardGroupID localShardGroupID = ShardMapper.toShardGroup(this.context.getNode().getIdentity(), numShardGroups);
		for (final PendingState pendingState : pendingAtom.getStates(StateLockMode.WRITE))
		{
			final ShardGroupID stateShardGroupID = ShardMapper.toShardGroup(pendingState.getAddress(), numShardGroups);
			if (stateShardGroupID.equals(localShardGroupID) == false)
				continue;
			
			final StateVoteCollector stateVoteCollector = pendingState.getStateVoteCollector();
			if (stateVoteCollector == null)
			{
				if (pendingAtom.getBlockHeader().getHeight() > (this.desyncHead.getHeight() - Constants.OOS_PREPARE_BLOCKS))
					syncLog.error(this.context.getName()+": State vote collector for "+pendingState.getAddress()+" in atom "+pendingAtom.getAtom()+" in block "+pendingState.getBlockHeader()+" not found");
				
				continue;
			}

			stateVoteCollector.vote(pendingState, votePower);
		}
	}
	
	private void processExecuteLatent(final BlockHeader header, final PendingAtom pendingAtom) throws IOException
	{
		if (pendingAtom.getStatus().after(AtomStatus.State.PREPARED) == false)
			throw new IllegalStateException("Pending atom "+pendingAtom.getHash()+" is not ACCEPTED");

		if (pendingAtom.isExecuteSignalled() || pendingAtom.getStatus().after(AtomStatus.State.PROVISIONED))
			throw new IllegalStateException("Pending atom "+pendingAtom.getHash()+" is signalled executable");

		final Epoch epoch = Epoch.from(pendingAtom.getBlockHeader());
		final int numShardGroups = this.context.getLedger().numShardGroups(epoch);
		final ShardGroupID localShardGroupID = ShardMapper.toShardGroup(this.context.getNode().getIdentity(), numShardGroups);
		for (final PendingState pendingState : pendingAtom.getStates(StateLockMode.WRITE))
		{
			final ShardGroupID stateShardGroupID = ShardMapper.toShardGroup(pendingState.getAddress(), numShardGroups);
			if (stateShardGroupID.equals(localShardGroupID) == false)
				continue;
				
			if (pendingState.getStateOutput() != null)
				throw new IllegalStateException("Latent state "+pendingState+" has state certificate");

			final StateVoteCollector stateVoteCollector = pendingState.getStateVoteCollector();
			if (stateVoteCollector != null)
			{
				// Modify the existing SVC if there is one
				if (stateVoteCollector.isCompletable())
					throw new IllegalStateException("SVC "+stateVoteCollector+" is completable containing latent state "+pendingState);
				
				if (stateVoteCollector.remove(pendingState) == false)
					throw new IllegalStateException("Latent state "+pendingState+" not removed from SVC "+stateVoteCollector);
				
				// Original SVC might just have contained a single item and is now empty
				if (stateVoteCollector.isEmpty())
					this.stateVoteCollectors.remove(stateVoteCollector.getBlock(), stateVoteCollector);
			}

			final long votePower = this.context.getLedger().getValidatorHandler().getVotePower(epoch, this.context.getNode().getIdentity());
			final long voteThreshold = this.context.getLedger().getValidatorHandler().getVotePowerThreshold(epoch, localShardGroupID);
			final StateVoteCollector latentStateVoteCollector = new StateVoteCollector(this.context, pendingAtom.getBlockHeader().getHash(), pendingState, votePower, voteThreshold);
			this.stateVoteCollectors.put(pendingAtom.getBlockHeader().getHash(), latentStateVoteCollector);
			pendingState.setStateVoteCollector(latentStateVoteCollector);
		}

		pendingAtom.setExecuteLatentSignalledBlock(header);
	}

	private void processTimeout(final PendingAtom pendingAtom, final AtomTimeout timeout)
	{
		Objects.requireNonNull(pendingAtom, "Atom is null");
		Objects.requireNonNull(timeout, "Atom timeout is null");
		
		if (pendingAtom.getStatus().after(AtomStatus.State.PREPARED) == false)
			throw new IllegalStateException("Pending atom "+pendingAtom.getHash()+" is not ACCEPTED");
		
		syncLog.info(this.context.getName()+": Pending atom "+pendingAtom.getHash()+" has timeout "+timeout);
		
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
			{
				if (pendingAtom.getBlockHeader().getHeight() > this.context.getLedger().getHead().getHeight())
					syncLog.error(this.context.getName()+": SVC not found for timeout "+timeout+" of atom "+pendingAtom.getHash()+" with state "+pendingState.getAddress()+" in block "+pendingState.getBlockHeader());
				
				continue;
			}
			
			// Test if completable as cant perform a completion in sync (TODO or can we?)
			if (stateVoteCollector.isCompletable() == false)
			{
				stateVoteCollector.remove(pendingState);
				
				if (stateVoteCollector.isEmpty())
					this.stateVoteCollectors.remove(stateVoteCollector.getBlock(), stateVoteCollector);
			}
		}
	}

    public boolean isSynced()
	{
		return this.isSynced;
	}
    
    public boolean isSyncedInRange(final int range)
    {
    	if (range <= 0) throw new IllegalArgumentException("Range "+range+" is invalid");
    	
		// Forced desync?
		if (this.forcedDesyncDuration > 0)
			return false;
		
		if (this.context.getConfiguration().get("ledger.synced.always", false) == true)
			return true;

		final List<AbstractConnection> syncPeers; 
		final int numShardGroups = this.context.getLedger().numShardGroups();
		final ShardGroupID localShardGroupID = ShardMapper.toShardGroup(this.context.getNode().getIdentity(), numShardGroups);
		final StandardConnectionFilter standardPeerFilter = StandardConnectionFilter.build(this.context).setShardGroupID(localShardGroupID).setStates(ConnectionState.CONNECTED);
		syncPeers = this.context.getNetwork().get(standardPeerFilter); 
		if (syncPeers.isEmpty())
			return false;
    			
		boolean isSynced = true;
		for (final AbstractConnection syncPeer : syncPeers)
		{
			if (syncPeer.getNode().getHead().isInRange(this.context.getLedger().getHead(), range))
				continue;
    					
			if (syncPeer.getNode().getHead().isAheadOf(this.context.getLedger().getHead(), range))
			{
				isSynced = false;
    			break;
    		}
    	}
    	
    	return isSynced;
    }
    
    void syncLost()
    {
		this.context.getNode().setSynced(false);
		this.context.getNode().setProgressing(false);
		this.context.getEvents().post(new SyncLostEvent());
		this.isSynced = false;
		reset();
    }
    
    SyncAcquiredMessage syncAcquired()
    {
		this.lock.lock();
		try
		{
			// Reset the ledger store state (inventories, caches, etc)
			this.context.getLedger().getLedgerStore().reset();

			// Send a SyncAcquiredEvent to all listeners.  SyncAcquiredEvent listeners MUST be synchronous listeners!
			this.context.getEvents().post(new SyncAcquiredEvent(this.syncHead, this.syncAccumulator, this.atoms.values(), this.stateVoteCollectors.values()));
		
			BlockHeader lowSyncHead = this.syncHead;
			for (final PendingAtom pendingAtom : this.atoms.values())
			{
				if (pendingAtom.getBlockHeader() == null)
					continue;
				
				if (pendingAtom.getBlockHeader().getHeight() >= lowSyncHead.getHeight())
					continue;
				
				lowSyncHead = pendingAtom.getBlockHeader();
			}

			return new SyncAcquiredMessage(lowSyncHead, this.atoms.keySet());
		}
		finally
		{
			this.isSynced = true;
			this.context.getNode().setHead(this.syncHead);
			this.context.getNode().setSynced(true);

			reset();
			
			this.lock.unlock();
		}
    }
    
	// PEER LISTENER //
    private EventListener peerListener = new EventListener()
    {
    	@Subscribe
		public void on(final DisconnectedEvent event)
		{
   			SyncHandler.this.lock.lock();
    		try
    		{
    			SyncHandler.this.blockInventories.removeAll(event.getConnection());
    			synchronized(SyncHandler.this.requestTasks)
    			{
    				final Collection<SyncRequestTask> requestTasks = SyncHandler.this.requestTasks.removeAll(event.getConnection());
    				for (SyncRequestTask requestTask : requestTasks)
    				{
    					try
    					{
    						if (requestTask.isCancelled() == false)
    							requestTask.cancel();

    						syncLog.info(SyncHandler.this.context.getName()+": Cancelled block sync task requesting block "+requestTask.requestedBlock+" from "+event.getConnection());
    					}
    					catch (Throwable t)
    					{
    						syncLog.error(SyncHandler.this.context.getName()+": Failed to cancel block sync task requesting block "+requestTask.requestedBlock+" from "+event.getConnection());
    					}
    				}
    			}
    			
    			final SyncInventoryTask inventoryTask = SyncHandler.this.inventoryTasks.remove(event.getConnection());
    			if (inventoryTask != null)
    			{
    				try
    				{
    					if (inventoryTask.isCancelled() == false)
    						inventoryTask.cancel();

    					syncLog.info(SyncHandler.this.context.getName()+": Cancelled inventory sync task of "+inventoryTask.requestSeq+":"+inventoryTask.block+" of blocks from "+event.getConnection());
    				}
    	    		catch (Throwable t)
    	    		{
    	    			syncLog.error(SyncHandler.this.context.getName()+": Failed to cancel inventory sync task of "+inventoryTask.requestSeq+":"+inventoryTask.block+" of blocks from "+event.getConnection());
    	    		}
    			}
    		}
    		finally
    		{
    			SyncHandler.this.lock.unlock();
    		}
		}
    };
}
