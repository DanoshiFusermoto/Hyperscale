package org.radix.hyperscale.network;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.eclipse.collections.api.factory.Maps;
import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.map.MutableMap;
import org.eclipse.collections.api.multimap.Multimap;
import org.eclipse.collections.api.multimap.MutableMultimap;
import org.eclipse.collections.api.multimap.set.MutableSetMultimap;
import org.eclipse.collections.api.set.MutableSet;
import org.eclipse.collections.impl.factory.Multimaps;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectLongHashMap;
import org.radix.hyperscale.Constants;
import org.radix.hyperscale.Context;
import org.radix.hyperscale.Service;
import org.radix.hyperscale.common.Primitive;
import org.radix.hyperscale.concurrency.MonitoredReentrantLock;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.events.EventListener;
import org.radix.hyperscale.events.SyncLostEvent;
import org.radix.hyperscale.events.SynchronousEventListener;
import org.radix.hyperscale.exceptions.QueueFullException;
import org.radix.hyperscale.exceptions.StartupException;
import org.radix.hyperscale.exceptions.TerminationException;
import org.radix.hyperscale.executors.Executor;
import org.radix.hyperscale.executors.PollingProcessor;
import org.radix.hyperscale.executors.ScheduledExecutable;
import org.radix.hyperscale.ledger.ShardGroupID;
import org.radix.hyperscale.ledger.ShardMapper;
import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;
import org.radix.hyperscale.network.events.DisconnectedEvent;
import org.radix.hyperscale.network.messages.GetItemsMessage;
import org.radix.hyperscale.network.messages.InventoryMessage;
import org.radix.hyperscale.network.messages.ItemsMessage;
import org.radix.hyperscale.serialization.Serialization;
import org.radix.hyperscale.serialization.SerializerId2;
import org.radix.hyperscale.time.Time;
import org.radix.hyperscale.utils.MathUtils;
import org.radix.hyperscale.utils.Numbers;

import com.google.common.eventbus.Subscribe;

import io.netty.util.internal.ThreadLocalRandom;

public class GossipHandler implements Service
{
	private static final Logger gossipLog = Logging.getLogger("gossip");
	
	private final class GossipRequestTask extends ConnectionTask 
	{
		private static enum SourceType { NONE, STALE, AVAILABLE }
		private static enum Status { PENDING, DELIVERED }
		
		private final long timestamp;
		private final Map<InventoryItem, Status> inventory;
		
		private volatile int delivered;
		
		GossipRequestTask(final AbstractConnection connection, final Set<InventoryItem> inventory, long timeout, TimeUnit timeunit)
		{
			super(connection, timeout, timeunit);
			
			Objects.requireNonNull(inventory, "Inventory is null");
			Numbers.isZero(inventory.size(), "Inventory is empty");
			
			this.timestamp = Time.getSystemTime();
			this.inventory = Maps.mutable.withInitialCapacity(inventory.size());
			for (InventoryItem item : inventory)
				this.inventory.put(item, Status.PENDING);
			this.delivered = 0;
		}
		
		public Status getStatus(final InventoryItem item)
		{
			final Status status;
			synchronized(this.inventory)
			{
				status = this.inventory.get(item);
			}
			
			if (status == null)
				throw new IllegalStateException("Gossip task "+this+" does not contain item "+item);
			
			return status;
		}
		
		public List<InventoryItem> getRemaining()
		{
			final List<InventoryItem> undelivered;
			synchronized(this.inventory)
			{
				undelivered = this.inventory.entrySet().stream().filter(e -> e.getValue().equals(Status.PENDING)).map(e -> e.getKey()).toList();
			}
			return undelivered;
		}
		
		public void received(final InventoryItem item)
		{
			Objects.requireNonNull(item, "InventoryItem is null");
			
			synchronized(this.inventory)
			{
				if (this.inventory.replace(item, Status.DELIVERED) == null)
					throw new IllegalStateException("Gossip task "+this+" does not contain item "+item);
				
				this.delivered++;
				
				final long latency = Time.getSystemTime() - this.timestamp;
				if (this.delivered == this.inventory.size())
				{
					getConnection().updateLatency(latency);
					getConnection().decrementPendingRequests();
					
					if (latency > Constants.MIN_GOSSIP_REQUEST_TIMEOUT_MILLISECONDS)
						gossipLog.warn(GossipHandler.this.context.getName()+": Fulfilled GossipTask "+hashCode()+" latently "+getID()+" in "+latency+"ms for "+getConnection());
					else if (gossipLog.hasLevel(Logging.INFO))
						gossipLog.info(GossipHandler.this.context.getName()+": Fulfilled GossipTask "+getID()+" in "+latency+"ms for "+getConnection());
				}
				
				getConnection().decrementPendingRequested();
				getConnection().decrementPendingWeight(item.getWeight());
				
				GossipHandler.this.context.getMetaData().increment("gossip.received.items");
				GossipHandler.this.context.getMetaData().increment("gossip.received.items.interval", latency);
					
				if (latency > Constants.MIN_GOSSIP_REQUEST_TIMEOUT_MILLISECONDS)
				{
					GossipHandler.this.context.getMetaData().increment("gossip.received.items.latent");
					GossipHandler.this.context.getMetaData().increment("gossip.received.items.latent.interval", latency);
				}
			}
		}
		
		public int numRemaining()
		{
			return this.inventory.size()-this.delivered;
		}
		
		public int numRequested()
		{
			return this.inventory.size();
		}

		private SourceType auditSources(final InventoryItem item)
		{
			SourceType sourceType = SourceType.NONE;
			Set<AbstractConnection> sources = GossipHandler.this.itemSources.get(item);
			if (sources.isEmpty() == false)
			{
				for (final AbstractConnection source : sources)
				{
					if (source.getState() != ConnectionState.CONNECTED)
						continue;
					
					if (source.isStale() && sourceType == SourceType.NONE)
					{
						sourceType = SourceType.STALE;
						continue;
					}
					
					sourceType = SourceType.AVAILABLE;
					break;
				}
			}
			return sourceType;
		}
		
		@Override
		public void execute()
		{
			GossipHandler.this.lock.lock();
			try
			{
				synchronized(this.inventory)
				{
					int remaining = this.inventory.size() - this.delivered;
					if (remaining > 0)
					{
						try
						{
							if (getConnection().getState().equals(ConnectionState.CONNECTED) || getConnection().getState().equals(ConnectionState.CONNECTING))
								getConnection().strikeOrDisconnect("Did not respond fully to request containing "+this.inventory.keySet());
						}
						catch (Throwable t)
						{
							gossipLog.error(GossipHandler.this.context.getName()+": "+getConnection().toString(), t);
						}
	
						final List<InventoryItem> undelivered = getRemaining();
						gossipLog.warn(GossipHandler.this.context.getName()+": Detected "+remaining+"/"+this.inventory.size()+" "+undelivered+" unfulfilled requests when processing GossipTask "+hashCode()+":"+getInitialDelay()+"ms for "+getConnection());
					
						final List<InventoryItem> noRetrySources = new ArrayList<InventoryItem>(undelivered.size());
						final List<InventoryItem> staleRetrySources = new ArrayList<InventoryItem>(undelivered.size());
						for (final InventoryItem item : undelivered)
						{
							if (GossipHandler.this.itemsRequested.remove(item, this) == false)
							{
								if (GossipHandler.this.itemsRequested.containsKey(item) == false)
									gossipLog.warn(GossipHandler.this.context.getName()+": Requested item "+item+" not found when processing unfulfilled requests in Gossip Task "+hashCode());
							}

							if (item.isStale() == false)
							{
								GossipHandler.this.toRequest.add(item);
								
								final SourceType sourceType = auditSources(item);
								if (sourceType == SourceType.NONE)
									noRetrySources.add(item);
								else if (sourceType == SourceType.STALE)
									staleRetrySources.add(item);
							}
							else
							{
								GossipHandler.this.itemSources.removeAll(item);
								gossipLog.warn(GossipHandler.this.context.getName()+": Inventory item "+item+" is stale, not re-requesting in Gossip Task "+hashCode());
							}
						}

						if (noRetrySources.isEmpty() == false)
							gossipLog.warn(GossipHandler.this.context.getName()+": No retry sources available for unfulfilled items "+noRetrySources+" request in Gossip Task "+hashCode());
						if (staleRetrySources.isEmpty() == false)
							gossipLog.warn(GossipHandler.this.context.getName()+": Only stale retry sources available for unfulfilled items "+staleRetrySources+" request in Gossip Task "+hashCode());
	
						getConnection().decrementPendingRequests();
						getConnection().decrementPendingRequested(undelivered.size());
						getConnection().decrementPendingWeight(undelivered.stream().collect(Collectors.summingInt(i -> i.getWeight())));
						getConnection().updateLatency(Time.getSystemTime() - this.timestamp);
					}
				}
			}
			finally
			{
				if (GossipHandler.this.requestTasks.remove(getConnection(), this) == false)
					gossipLog.error(GossipHandler.this.context.getName()+": Executed GossipTask "+hashCode()+" not found for "+getConnection());

				GossipHandler.this.lock.unlock();
			}
		}

		@Override
		public void onCancelled()
		{
			GossipHandler.this.lock.lock();
			try
			{
				synchronized(this.inventory)
				{
					int remaining = this.inventory.size() - this.delivered;
					if (remaining > 0)
					{
						final List<InventoryItem> undelivered = getRemaining();
						if (GossipHandler.this.context.getNode().isSynced())
						{
							gossipLog.warn(GossipHandler.this.context.getName()+": Detected "+remaining+"/"+this.inventory.size()+" "+undelivered+" unfulfilled requests when cancelling GossipTask "+hashCode()+":"+getInitialDelay()+"ms for "+getConnection());
		
							final List<InventoryItem> noRetrySources = new ArrayList<InventoryItem>(undelivered.size());
							final List<InventoryItem> staleRetrySources = new ArrayList<InventoryItem>(undelivered.size());
							for (final InventoryItem item : undelivered)
							{
								if (GossipHandler.this.itemsRequested.remove(item, this) == false)
								{
									if (GossipHandler.this.itemsRequested.containsKey(item) == false)
										gossipLog.warn(GossipHandler.this.context.getName()+": Requested item "+item+" not found when processing unfulfilled requests in Gossip Task "+hashCode());
								}
								
								if (item.isStale() == false)
								{
									GossipHandler.this.toRequest.add(item);
									
									final SourceType sourceType = auditSources(item);
									if (sourceType == SourceType.NONE)
										noRetrySources.add(item);
									else if (sourceType == SourceType.STALE)
										staleRetrySources.add(item);
								}
								else
								{
									GossipHandler.this.itemSources.removeAll(item);
									gossipLog.warn(GossipHandler.this.context.getName()+": Inventory item "+item+" is stale, not re-requesting in Gossip Task "+hashCode());
								}
							}
							
							if (noRetrySources.isEmpty() == false)
								gossipLog.warn(GossipHandler.this.context.getName()+": No retry sources available for unfulfilled items "+noRetrySources+" request in Gossip Task "+hashCode());
							if (staleRetrySources.isEmpty() == false)
								gossipLog.warn(GossipHandler.this.context.getName()+": Only stale retry sources available for unfulfilled items "+staleRetrySources+" request in Gossip Task "+hashCode());
						}
					
						getConnection().decrementPendingRequests();
						getConnection().decrementPendingRequested(undelivered.size());
						getConnection().decrementPendingWeight(undelivered.stream().collect(Collectors.summingInt(i -> i.getWeight())));
					}
				}					
			}
			catch (Throwable t)
			{
				gossipLog.error(GossipHandler.this.context.getName()+": "+getConnection().toString(), t);
			}
			finally
			{
				if (GossipHandler.this.requestTasks.remove(getConnection(), this) == false)
					gossipLog.error(GossipHandler.this.context.getName()+": Cancelled GossipTask "+hashCode()+" not found for "+getConnection());

				GossipHandler.this.lock.unlock();
			}
		}
	}
	
	private final Context context;

	private final BlockingQueue<GossipEvent> eventQueue;
	private final BlockingQueue<GossipBroadcast> broadcastQueue;
	
	// TODO can merge these to just a map with InventoryItem and Optional GossipTask?
	private final Set<InventoryItem> toRequest = Collections.synchronizedSet(new LinkedHashSet<InventoryItem>());
	private final MutableMap<InventoryItem, GossipRequestTask> itemsRequested = Maps.mutable.<InventoryItem, GossipRequestTask>ofInitialCapacity(1<<12).asSynchronized();
	private final MutableSetMultimap<InventoryItem, AbstractConnection> itemSources = Multimaps.mutable.set.<InventoryItem, AbstractConnection>empty().asSynchronized();
	private final MutableSetMultimap<AbstractConnection, GossipRequestTask> requestTasks = Multimaps.mutable.set.<AbstractConnection, GossipRequestTask>empty().asSynchronized();

	private final MutableMap<Class<? extends Primitive>, GossipFilter> broadcastFilters = Maps.mutable.<Class<? extends Primitive>, GossipFilter>empty().asSynchronized();
	private final MutableMap<Class<? extends Primitive>, GossipFetcher> fetcherProcessors = Maps.mutable.<Class<? extends Primitive>, GossipFetcher>empty().asSynchronized();
	private final MutableMap<Class<? extends Primitive>, GossipReceiver> receiverProcessors = Maps.mutable.<Class<? extends Primitive>, GossipReceiver>empty().asSynchronized();
	private final MutableMap<Class<? extends Primitive>, GossipInventory> inventoryProcessors = Maps.mutable.<Class<? extends Primitive>, GossipInventory>empty().asSynchronized();

	private final MonitoredReentrantLock lock;
	
	private PollingProcessor broadcastProcessor = new PollingProcessor()
	{
		private long lastQueueSizeWarn = 0;

		@Override
		public void process() throws InterruptedException
		{
			if (System.currentTimeMillis() - this.lastQueueSizeWarn > 1000)
			{
				final int broadcastQueueSize = GossipHandler.this.broadcastQueue.size();
				final long EWMAResult = MathUtils.EWMA(GossipHandler.this.context.getMetaData().get("gossip.broadcast.queue", 0l), broadcastQueueSize, 0.9);
				GossipHandler.this.context.getMetaData().put("gossip.broadcast.queue", EWMAResult);
				
				if (broadcastQueueSize > Constants.WARN_ON_QUEUE_SIZE)
					gossipLog.warn(GossipHandler.this.context.getName()+": Broadcast queue is "+broadcastQueueSize);
				
				this.lastQueueSizeWarn = System.currentTimeMillis();
			}

			try
			{
				// Dont want to broadcast if local instance is unsynced, or has just resumed from a sync but not making progress yet
				if (GossipHandler.this.context.getNode().isSynced() == false)
				{
					Thread.sleep(1000);
					return;
				}
				else if (GossipHandler.this.context.getNode().isProgressing() == false)
				{
					if (gossipLog.hasLevel(Logging.DEBUG))
						gossipLog.debug(GossipHandler.this.context.getName()+": Broadcast cancelled, liveness is not resumed post sync");
					
					GossipHandler.this.broadcastQueue.clear();
					Thread.sleep(1000);
					return;
				}
			}
			catch (InterruptedException e) 
			{
		        Thread.currentThread().interrupt();
		        gossipLog.warn(GossipHandler.this.context.getName()+": Broadcast sleep interrupted, continuing", e);
		        return;
		    }

			_processBroadcasts();
		}

		@Override
		public void onError(Throwable thrown)
		{
			gossipLog.fatal(GossipHandler.this.context.getName()+": Error processing gossip queue", thrown);
		}

		@Override
		public void onTerminated()
		{
			gossipLog.fatal(GossipHandler.this.context.getName()+": Gossip broadcast processing has exited");
		}
	};
		
	private PollingProcessor gossipProcessor = new PollingProcessor()
	{
		private long lastQueueSizeWarn = 0;

		@Override
		public void process() throws InterruptedException
		{
			if (System.currentTimeMillis() - this.lastQueueSizeWarn > 1000)
			{
				GossipHandler.this.context.getMetaData().put("gossip.events.queue", MathUtils.EWMA(GossipHandler.this.context.getMetaData().get("gossip.events.queue", 0l), GossipHandler.this.eventQueue.size(), 0.9));

				if (GossipHandler.this.eventQueue.size() > Constants.WARN_ON_QUEUE_SIZE)
					gossipLog.warn(GossipHandler.this.context.getName()+": Gossip event queue is "+GossipHandler.this.eventQueue.size());
					
				this.lastQueueSizeWarn = System.currentTimeMillis();
			}

			try
			{
				if (GossipHandler.this.context.getNode().isSynced() == false)
				{
					GossipHandler.this.eventQueue.clear();
					Thread.sleep(1000);
					return;
				}
			}
			catch (InterruptedException e) 
			{
		        Thread.currentThread().interrupt();
		        gossipLog.warn(GossipHandler.this.context.getName()+": Gossip sleep interrupted, continuing", e);
		        return;
		    }
				
			_processGossipEvents();
		}

		@Override
		public void onError(Throwable thrown)
		{
			gossipLog.fatal(GossipHandler.this.context.getName()+": Error processing gossip requests ", thrown);
		}

		@Override
		public void onTerminated()
		{
			gossipLog.fatal(GossipHandler.this.context.getName()+": Gossip processor has exited");
		}
	};

	GossipHandler(final Context context)
	{
		this.context = Objects.requireNonNull(context, "Context is null");
		this.lock = new MonitoredReentrantLock(this.context.getName()+" Gossip Handler Lock", true);

		this.broadcastQueue = new PriorityBlockingQueue<GossipBroadcast>(this.context.getConfiguration().get("ledger.gossip.broadcast.queue", 1<<12), new Comparator<GossipBroadcast>() {
			@Override
			public int compare(final GossipBroadcast m1, final GossipBroadcast m2) 
			{
				if (m1.isUrgent() == true && m2.isUrgent() == false)
					return -1;
				if (m1.isUrgent() == false && m2.isUrgent() == true)
					return 1;
	            
	            return 0;
	        }
		});
		
		this.eventQueue = new PriorityBlockingQueue<GossipEvent>(this.context.getConfiguration().get("ledger.gossip.message.queue", 1<<12), new Comparator<GossipEvent>() {
			@Override
			public int compare(final GossipEvent m1, final GossipEvent m2) 
			{
				if (m1.isUrgent() == true && m2.isUrgent() == false)
					return -1;
				if (m1.isUrgent() == false && m2.isUrgent() == true)
					return 1;
				
				if (m1.getMessage().getTimestamp() / Constants.BROADCAST_POLL_TIMEOUT < m2.getMessage().getTimestamp() / Constants.BROADCAST_POLL_TIMEOUT)
					return -1;
				if (m1.getMessage().getTimestamp() / Constants.BROADCAST_POLL_TIMEOUT > m2.getMessage().getTimestamp() / Constants.BROADCAST_POLL_TIMEOUT)
					return 1;
				
				if (m1.getConnection().getLatency() > m2.getConnection().getLatency())
					return -1;
				if (m1.getConnection().getLatency() < m2.getConnection().getLatency())
					return 1;

				return 0;
			}
		});
	}

	@Override
	public void start() throws StartupException
	{
		this.context.getNetwork().getMessaging().register(InventoryMessage.class, this.getClass(), new MessageProcessor<InventoryMessage>()
		{
			@Override
			public void process(final InventoryMessage inventoryMessage, final AbstractConnection connection)
			{
				try
				{
					if (GossipHandler.this.eventQueue.offer(new GossipEvent(inventoryMessage, connection)) == false)
						throw new QueueFullException();
					
					if (gossipLog.hasLevel(Logging.DEBUG))
						gossipLog.debug(GossipHandler.this.context.getName()+": Queued "+inventoryMessage.getClass().getAnnotation(SerializerId2.class).value()+" containing "+inventoryMessage.asInventory()+" items from "+connection);
				}
				catch (Throwable t)
				{
					gossipLog.error(GossipHandler.this.context.getName()+": gossip.inventory "+connection.toString(), t);
				}
			}
		});
		
		this.context.getNetwork().getMessaging().register(GetItemsMessage.class, this.getClass(), new MessageProcessor<GetItemsMessage>()
		{
			@Override
			public void process(final GetItemsMessage getItemsMessage, final AbstractConnection connection)
			{
				try
				{
					if (GossipHandler.this.eventQueue.offer(new GossipEvent(getItemsMessage, connection)) == false)
						throw new QueueFullException();
					
					if (gossipLog.hasLevel(Logging.DEBUG))
						gossipLog.debug(GossipHandler.this.context.getName()+": Queued "+getItemsMessage.getClass().getAnnotation(SerializerId2.class).value()+" containing "+getItemsMessage.asInventory()+" items from "+connection);
				}
				catch (Throwable t)
				{
					gossipLog.error(GossipHandler.this.context.getName()+": gossip.items.get "+connection.toString(), t);
				}
			}
		});

		this.context.getNetwork().getMessaging().register(ItemsMessage.class, this.getClass(), new MessageProcessor<ItemsMessage>()
		{
			@Override
			public void process(final ItemsMessage itemsMessage, final AbstractConnection connection)
			{
				try
				{
					// Unlike the other gossip message types, it's worth waiting for a full queue to release some slots for requested items
					if (GossipHandler.this.eventQueue.offer(new GossipEvent(itemsMessage, connection), 1, TimeUnit.SECONDS) == false)
						throw new QueueFullException();
					
					if (gossipLog.hasLevel(Logging.DEBUG))
						gossipLog.debug(GossipHandler.this.context.getName()+": Queued "+itemsMessage.getClass().getAnnotation(SerializerId2.class).value()+" containing "+itemsMessage.asInventory()+" items from "+connection);
				}
				catch (Throwable t)
				{
					// TODO need some clean up here
					gossipLog.error(GossipHandler.this.context.getName()+": gossip.items "+connection.toString(), t);
				}
			}
		});

		this.context.getEvents().register(this.syncChangeListener);
		this.context.getEvents().register(this.asyncPeerListener);
		
		Thread broadcastProcessorThread = new Thread(this.broadcastProcessor);
		broadcastProcessorThread.setDaemon(true);
		broadcastProcessorThread.setName(this.context.getName()+" Broadcast Processor");
		broadcastProcessorThread.setPriority(8);
		broadcastProcessorThread.start();

		Thread requestProcessorThread = new Thread(this.gossipProcessor);
		requestProcessorThread.setDaemon(true);
		requestProcessorThread.setName(this.context.getName()+" Gossip Processor");
		requestProcessorThread.setPriority(8);
		requestProcessorThread.start();
		
		Executor.getInstance().scheduleAtFixedRate(new ScheduledExecutable(1, 1, TimeUnit.MINUTES) {
			@Override
			public void execute()
			{
				GossipHandler.this.lock.lock();
				try
				{
					final Iterator<InventoryItem> itemIterator = GossipHandler.this.toRequest.iterator();
					while(itemIterator.hasNext())
					{
						final InventoryItem item = itemIterator.next();

						boolean hasSource = false;
						for (final AbstractConnection connection : GossipHandler.this.itemSources.get(item))
						{
							if (connection.getState().equals(ConnectionState.CONNECTED) == false || connection.isStale())
								continue;
								
							hasSource = true;
							break;
						}
							
						if (hasSource == false)
						{
							itemIterator.remove();
							GossipHandler.this.itemSources.removeAll(item);
							gossipLog.warn(GossipHandler.this.context.getName()+": No sources available for "+item);
						}
					}
				}
				finally
				{
					GossipHandler.this.lock.unlock();
				}
			}
		});
	}

	@Override
	public void stop() throws TerminationException
	{
		this.gossipProcessor.terminate(true);
		this.broadcastProcessor.terminate(true);
		this.context.getEvents().unregister(this.asyncPeerListener);
		this.context.getEvents().unregister(this.syncChangeListener);
		this.context.getNetwork().getMessaging().deregisterAll(getClass());
	}
	
	// BROADCAST PIPELINE //
	private void _processBroadcasts()
	{
		boolean hasUrgentTransport = false;
		long pollLatchTimer = System.currentTimeMillis();
		long nextPollTimer = Constants.BROADCAST_POLL_TIMEOUT;

		final MutableMultimap<ShardGroupID, GossipBroadcast> toBroadcast = Multimaps.mutable.set.empty();
		try
		{
			GossipBroadcast broadcast;
			while((broadcast = this.broadcastQueue.poll(hasUrgentTransport ? 0 : nextPollTimer, TimeUnit.MILLISECONDS)) != null)
			{
				if (this.context.getNode().isSynced() == false)
					break;
				
				if (this.context.getConfiguration().get("gossip.faults.force.nonbroadcast.interval", 0l) > 0 && 
					ThreadLocalRandom.current().nextLong() % this.context.getConfiguration().get("gossip.faults.force.nonbroadcast.interval", 0l) == 0)
				{
					gossipLog.warn(this.context.getName()+": Not broadcasting primitive "+broadcast.getPrimitive().getHash()+" of type "+broadcast.getPrimitive().getClass()+" as per failure configuration");
					continue;
				}

				final GossipFilter filter = this.broadcastFilters.get(broadcast.getPrimitive().getClass());
				if (filter == null)
				{
					gossipLog.error(this.context.getName()+": Gossip filter not found for primitive "+broadcast.getPrimitive().getHash()+" of type "+broadcast.getPrimitive().getClass());
					continue;
				}
				
				try
				{
					if (broadcast.getShardGroups().isEmpty())
						broadcast.setShardGroups(filter.filter(broadcast.getPrimitive()));
				}
				catch (Exception ex)
				{
					gossipLog.error(this.context.getName()+": Filter for "+broadcast.getPrimitive().getClass()+" failed on "+broadcast.getPrimitive(), ex);
					continue;
				}
				
				// Priority broadcast?
				if (broadcast.isUrgent())
					hasUrgentTransport = true;
						
				final List<ShardGroupID> broadcastShardGroups = broadcast.getShardGroups(); 
				for(int s = 0 ; s < broadcastShardGroups.size() ; s++)
					toBroadcast.put(broadcastShardGroups.get(s), broadcast);
				
				// Ok to be over as a message goes to each shard group ID so wont ever be over quota
				if (toBroadcast.size() >= Constants.MAX_BROADCAST_INVENTORY_ITEMS)
					break;

				// Adjust the poll timer based on elapsed time so we have a consistent Constants.BROADCAST_POLL_TIMEOUT interval
				nextPollTimer = Constants.BROADCAST_POLL_TIMEOUT - (System.currentTimeMillis() - pollLatchTimer);
				if (nextPollTimer <= 0)
					break;
			}
		} 
		catch (InterruptedException e) 
		{
			Thread.currentThread().interrupt();
	        gossipLog.warn(GossipHandler.this.context.getName()+": Broadcast processing interrupted, proceeding to broadcast "+toBroadcast.size()+" items", e);
		}
			
		if (toBroadcast.isEmpty() == false && this.context.getNode().isSynced())
			_actionBroadcast(toBroadcast);
	}

	private void _actionBroadcast(final Multimap<ShardGroupID, GossipBroadcast> toBroadcast)
	{
		final int numShardGroups = this.context.getLedger().numShardGroups();
		final ShardGroupID localShardGroupID = ShardMapper.toShardGroup(this.context.getNode().getIdentity(), numShardGroups);
		final StandardConnectionFilter shardGroupConnectionFilter = StandardConnectionFilter.build(this.context).setStates(ConnectionState.CONNECTED).setSynced(true).setStale(false);
		
		final ObjectLongHashMap<String> broadcastStatistics = ObjectLongHashMap.newMap();
		final List<AbstractConnection> broadcastConnections = new ArrayList<AbstractConnection>(this.context.getNetwork().count(ConnectionState.CONNECTED));
		final Map<ShardGroupID, InventoryMessage> broadcastInventoryMessages = Maps.mutable.ofInitialCapacity(numShardGroups);
		for (final ShardGroupID shardGroupID : toBroadcast.keySet())
		{
			final InventoryMessage broadcastInventoryMessage = new InventoryMessage();
			for (final GossipBroadcast broadcastItem : toBroadcast.get(shardGroupID))
			{
				broadcastInventoryMessage.add(broadcastItem.getPrimitive().getClass(), broadcastItem.getPrimitive().getHash());
				broadcastStatistics.addToValue(broadcastItem.getPrimitive().getClass().getSimpleName(), 1);
			}
			
			broadcastInventoryMessages.put(shardGroupID, broadcastInventoryMessage);
			
			shardGroupConnectionFilter.setShardGroupID(shardGroupID);
			final List<AbstractConnection> shardGroupConnections = this.context.getNetwork().get(shardGroupConnectionFilter);
			if (shardGroupConnections.isEmpty())
			{
				if (shardGroupID.equals(localShardGroupID) == false || 
					(shardGroupID.equals(localShardGroupID) && this.context.getConfiguration().get("ledger.synced.always", false) == false))
				{
					final Multimap<Class<? extends Primitive>, Hash> itemsByType = broadcastInventoryMessage.getTyped();
					for (Class<? extends Primitive> type : itemsByType.keySet())
						gossipLog.error(this.context.getName()+": No connections available to send BroadcastInventoryMessage type "+type+" containing "+itemsByType.get(type)+" items in shard group ID "+shardGroupID);
				}
			}
			else
				broadcastConnections.addAll(shardGroupConnections);
		}
		
		for (final AbstractConnection connection : broadcastConnections)
		{
			final ShardGroupID shardGroupID = ShardMapper.toShardGroup(connection.getNode().getIdentity(), numShardGroups);
			final InventoryMessage broadcastInventoryMessage = broadcastInventoryMessages.get(shardGroupID);

			if (connection.isStale())
			{
				final Multimap<Class<? extends Primitive>, Hash> itemsByType = broadcastInventoryMessage.getTyped();
				for (Class<? extends Primitive> type : itemsByType.keySet())
					gossipLog.error(this.context.getName()+": Unable to send BroadcastInventoryMessage type "+type+" containing "+itemsByType.get(type)+" items in shard group ID "+shardGroupID+" to stale connection "+connection.toString());

				continue;
			}
				
			try
			{
				if (gossipLog.hasLevel(Logging.DEBUG))
				{
					final Multimap<Class<? extends Primitive>, Hash> itemsByType = broadcastInventoryMessage.getTyped();
					for (Class<? extends Primitive> type : itemsByType.keySet())
						gossipLog.debug(this.context.getName()+": Broadcasting inv type "+type+" containing "+itemsByType.get(type)+" to "+connection.toString());
				}

				this.context.getNetwork().getMessaging().send(broadcastInventoryMessage, connection);
			}
			catch (IOException ex)
			{
				final Multimap<Class<? extends Primitive>, Hash> itemsByType = broadcastInventoryMessage.getTyped();
				for (Class<? extends Primitive> type : itemsByType.keySet())
					gossipLog.error(this.context.getName()+": Unable to send BroadcastInventoryMessage type "+type+" containing "+itemsByType.get(type)+" items in shard group ID "+shardGroupID+" to "+connection.toString(), ex);
			}
		}

		broadcastStatistics.forEachKeyValue((s, l) -> {
			this.context.getMetaData().increment("gossip.broadcasts."+s.toLowerCase(), l);
			this.context.getMetaData().increment("gossip.broadcasts.items", l);
		});
		this.context.getMetaData().increment("gossip.broadcasts.total");
	}

	// GOSSIP PIPELINE //
	private void _processGossipEvents()
	{
		GossipEvent event;
		long pollLatchTimer = System.currentTimeMillis();
		long nextPollTimer = Constants.BROADCAST_POLL_TIMEOUT;
		
		try
		{
			while((event = this.eventQueue.poll(nextPollTimer, TimeUnit.MILLISECONDS)) != null)
			{
				if (this.context.getNode().isSynced() == false)
					break;
				
				if (event.getMessage() instanceof InventoryMessage inventoryMessage)
					inventory(inventoryMessage, event.getConnection());
				else if (event.getMessage() instanceof ItemsMessage itemsMessage)
					received(itemsMessage, event.getConnection());
				else if (event.getMessage() instanceof GetItemsMessage getItemsMessage)
					fetch(getItemsMessage, event.getConnection());
					
				// Adjust the poll timer based on elapsed time so we have a consistent Constants.BROADCAST_POLL_TIMEOUT interval
				nextPollTimer = Constants.BROADCAST_POLL_TIMEOUT - (System.currentTimeMillis() - pollLatchTimer);
				if (nextPollTimer <= 0)
					break;
			}
		} 
		catch (InterruptedException ex) 
		{
			Thread.currentThread().interrupt();
	        gossipLog.warn(GossipHandler.this.context.getName()+": Gossip event processing interrupted, proceeding to request actioning", ex);
		}

		if (this.context.getNode().isSynced())
			_actionRequests();
	}
	
	private void _actionRequests()
	{
		// Get weighted connections using filter
		final StandardConnectionFilter standardPeerFilter = StandardConnectionFilter.build(this.context).setStates(ConnectionState.CONNECTED).setSynced(true).setStale(false);
		final List<AbstractConnection> connections = this.context.getNetwork().get(standardPeerFilter);

		final Map<AbstractConnection, List<InventoryItem>> nextToRequest = new HashMap<>(connections.size());
		this.lock.lock();
		try
		{
			for (final InventoryItem item : this.toRequest)
			{
				if (this.inventoryProcessors.get(item.getType()) == null)
				{
					gossipLog.error(this.context.getName()+": Inventory processor for "+item+" is not found");
					continue;
				}
				
				if (connections.isEmpty())
					break;
				
				final Set<AbstractConnection> connectionsWithItem = this.itemSources.get(item);
				final Iterator<AbstractConnection> connectionsIterator = connections.iterator();
				while(connectionsIterator.hasNext())
				{
					final AbstractConnection connection = connectionsIterator.next();
					if (connectionsWithItem.contains(connection) == false)
						continue;

					// Connection went stale during this request iteration
					if (connection.isStale())
					{
						connectionsIterator.remove();
						continue;
					}
					
					// Max quota reached for this connection
					int outstandingRequests = (connection.pendingRequests() * connection.pendingRequests()) + connection.pendingWeight() + nextToRequest.getOrDefault(connection, Collections.emptyList()).size();
					if (outstandingRequests >= Constants.MAX_REQUEST_INVENTORY_ITEMS)
					{
						connectionsIterator.remove();
						continue;
					}
					
					// Check if connection can only serve one pending request task at a time 
					if (this.context.getConfiguration().get("network.gossip.requests.singleton", false).equals(Boolean.TRUE))
					{
						boolean connectionAvailable = true;
						for (final GossipRequestTask gossipRequestTask : this.requestTasks.get(connection))
						{
							if (gossipRequestTask.numRemaining() > 0)
							{
								connectionAvailable = false;
								break;
							}
						}

						if (connectionAvailable == false)
							continue;
					}
					
					// Add item to request inventory for this connection
					nextToRequest.computeIfAbsent(connection, c -> new ArrayList<InventoryItem>());
					nextToRequest.get(connection).add(item);
					
					// Perform only one urgent request per connection per iteration
					if (item.isUrgent())
						connectionsIterator.remove();

					break;
				}
			}
			
			for (final Entry<AbstractConnection, List<InventoryItem>> entry : nextToRequest.entrySet())
			{
				try
				{
					request(entry.getKey(), entry.getValue());
				}
				catch (IOException ex)
				{
					gossipLog.error(this.context.getName()+": Unable to send request of "+entry.getValue()+" items in shard group to "+entry.getKey().toString(), ex);
				}
			}
		}
		finally
		{
			this.lock.unlock();
		}
	}
	
	// METHODS //
	public void register(final Class<? extends Primitive> type, final GossipFilter<?> filter)
	{
		Objects.requireNonNull(type, "Type is null");
		Objects.requireNonNull(filter, "Filter is null");
		
		synchronized(this.broadcastFilters)
		{
			if (this.broadcastFilters.containsKey(type))
				throw new IllegalStateException("Already exists a gossip filter for type "+type);
		
			this.broadcastFilters.put(type, filter);
		}
	}
	
	public void register(final Class<? extends Primitive> type, final GossipInventory inventory)
	{
		Objects.requireNonNull(type, "Type is null");
		Objects.requireNonNull(inventory, "Inventory is null");
		
		synchronized(this.inventoryProcessors)
		{
			if (this.inventoryProcessors.containsKey(type))
				throw new IllegalStateException("Already exists a inventory processors for type "+type);
			
			this.inventoryProcessors.put(type, inventory);
		}
	}

	public void register(final Class<? extends Primitive> type, final GossipFetcher fetcher)
	{
		Objects.requireNonNull(type, "Type is null");
		Objects.requireNonNull(fetcher, "Fetcher is null");
		
		synchronized(this.fetcherProcessors)
		{
			if (this.fetcherProcessors.containsKey(type))
				throw new IllegalStateException("Already exists a fetcher processor for type "+type);
			
			this.fetcherProcessors.put(type, fetcher);
		}
	}

	public void register(final Class<? extends Primitive> type, final GossipReceiver<?> receiver)
	{
		Objects.requireNonNull(type, "Type is null");
		Objects.requireNonNull(receiver, "Receiver is null");
		
		synchronized(this.receiverProcessors)
		{
			if (this.receiverProcessors.containsKey(type))
				throw new IllegalStateException("Already exists a receiver processor for type "+type);
			
			this.receiverProcessors.put(type, receiver);
		}
	}
	
	public long getBroadcastQueueSize() 
	{
		return this.broadcastQueue.size();
	}

	public long getEventQueueSize() 
	{
		return this.eventQueue.size();
	}

	public long getRequestedQueueSize() 
	{
		return this.itemsRequested.size();
	}

	public long getRequestTasksQueueSize() 
	{
		return this.requestTasks.size();
	}

	public boolean broadcast(final Primitive object)
	{
		Objects.requireNonNull(object, "Primitive is null");
		
		if (Serialization.getInstance().getIdForClass(object.getClass()) == null)
			throw new IllegalArgumentException("Type "+object.getClass()+" is an unregistered class");

		final GossipBroadcast broadcast = new GossipBroadcast(object);
		if (gossipLog.hasLevel(Logging.DEBUG))
			gossipLog.debug(GossipHandler.this.context.getName()+": Queueing broadcast "+broadcast);

		boolean result = this.broadcastQueue.offer(broadcast);
		if (result == false)
			gossipLog.warn(this.context.getName()+": Failed to queue broadcast "+broadcast);

		return result;
	}
	
	public boolean broadcast(final Primitive object, final ShardGroupID shardGroupID)
	{
		Objects.requireNonNull(object, "Primitive is null");

		if (Serialization.getInstance().getIdForClass(object.getClass()) == null)
			throw new IllegalArgumentException("Type "+object.getClass()+" is an unregistered class");

		final GossipBroadcast broadcast = new GossipBroadcast(object, shardGroupID);
		if (gossipLog.hasLevel(Logging.DEBUG))
			gossipLog.debug(GossipHandler.this.context.getName()+": Queueing broadcast "+broadcast);

		boolean result = this.broadcastQueue.offer(broadcast);
		if (result == false)
			gossipLog.warn(this.context.getName()+": Failed to queue broadcast "+broadcast);
		
		return result;
	}

	public boolean broadcast(final Primitive object, final ShardGroupID ... shardGroupIDs)
	{
		Objects.requireNonNull(object, "Primitive is null");
		Objects.requireNonNull(shardGroupIDs, "Shard group IDs is null");
		
		if (Serialization.getInstance().getIdForClass(object.getClass()) == null)
			throw new IllegalArgumentException("Type "+object.getClass()+" is an unregistered class");
		
		final GossipBroadcast broadcast = new GossipBroadcast(object, shardGroupIDs);
		if (gossipLog.hasLevel(Logging.DEBUG))
			gossipLog.debug(GossipHandler.this.context.getName()+": Queueing broadcast "+broadcast);
		
		boolean result = this.broadcastQueue.offer(broadcast);
		if (result == false)
			gossipLog.warn(this.context.getName()+": Failed to queue broadcast "+broadcast);
		
		return result;
	}

	public boolean broadcast(final Primitive object, final Set<ShardGroupID> shardGroupIDs)
	{
		Objects.requireNonNull(object, "Primitive is null");
		Objects.requireNonNull(shardGroupIDs, "Shard group IDs is null");
		
		if (Serialization.getInstance().getIdForClass(object.getClass()) == null)
			throw new IllegalArgumentException("Type "+object.getClass()+" is an unregistered class");
		
		final GossipBroadcast broadcast = new GossipBroadcast(object, shardGroupIDs);
		if (gossipLog.hasLevel(Logging.DEBUG))
			gossipLog.debug(GossipHandler.this.context.getName()+": Queueing broadcast "+broadcast);
		
		boolean result = this.broadcastQueue.offer(broadcast);
		if (result == false)
			gossipLog.warn(this.context.getName()+": Failed to queue broadcast "+broadcast);
		
		return result;
	}

	public Collection<? extends Primitive> broadcast(final Class<? extends Primitive> type, final Collection<? extends Primitive> objects)
	{
		Objects.requireNonNull(type, "Type is null");
		Objects.requireNonNull(objects, "Primitives is null");
		
		if (Serialization.getInstance().getIdForClass(type) == null)
			throw new IllegalArgumentException("Type "+type+" is an unregistered class");
		
		final List<Primitive> queued = new ArrayList<Primitive>(objects.size());
		for (final Primitive object : objects)
		{
			final GossipBroadcast broadcast = new GossipBroadcast(object);
			if (gossipLog.hasLevel(Logging.DEBUG))
				gossipLog.debug(GossipHandler.this.context.getName()+": Queueing broadcast "+broadcast);

			if (this.broadcastQueue.offer(broadcast))
				queued.add(object);
			else
				break;
		}
		
		if (queued.size() != objects.size())
			gossipLog.warn(this.context.getName()+": Failed to queue all objects of type "+type+" for broadcast "+queued.size()+"/"+objects.size());
		
		return queued;
	}
	
	private void fetch(final GetItemsMessage message, final AbstractConnection connection)
	{
		Objects.requireNonNull(message, "Message is null");
		Objects.requireNonNull(connection, "Connection is null");

		try
		{
			final List<InventoryItem> inventory = message.asInventory();

			if (gossipLog.hasLevel(Logging.DEBUG))
				gossipLog.debug(this.context.getName()+": Processing fetch request from "+connection.toString()+" of items "+inventory);
			
			final Set<InventoryItem> normalizedInventory = Sets.immutable.ofAll(inventory).castToSet();
			if (normalizedInventory.size() != inventory.size())
				gossipLog.warn(this.context.getName()+": Received un-normalized fetch from "+connection.toString());
			
			final List<Primitive> fetched = new ArrayList<Primitive>(inventory.size()); 
			final MutableSetMultimap<Class<? extends Primitive>, Hash> itemsByType = Multimaps.mutable.set.empty();
			inventory.forEach(item -> itemsByType.put(item.getType(), item.getHash()));
			
			for (final Class<? extends Primitive> type : itemsByType.keySet())
			{
				final GossipFetcher fetcher = this.fetcherProcessors.get(type);
				if (fetcher == null)
				{
					gossipLog.warn(this.context.getName()+": No fetcher found for type "+type);
					return;
				}

				final Collection<? extends Primitive> results = fetcher.fetch(itemsByType.get(type), connection);
				fetched.addAll(results);
			}
			
			// Send items with priority > 0 or urgent first
			ItemsMessage itemsMessage = null;
			for(final Primitive primitive : fetched)
			{
				if (this.context.getConfiguration().get("gossip.faults.force.nondelivery.interval", 0l) > 0 && 
					ThreadLocalRandom.current().nextLong() % this.context.getConfiguration().get("gossip.faults.force.nondelivery.interval", 0l) == 0)
				{
					gossipLog.warn(GossipHandler.this.context.getName()+": Not delivering primitive "+primitive.getHash()+" of type "+primitive.getClass()+" as per failure configuration");
					continue;
				}
				
				final TransportParameters transportParameters = primitive.getClass().getAnnotation(TransportParameters.class);
				if (transportParameters != null && transportParameters.urgent())
				{
					ItemsMessage urgentItemMessage = new ItemsMessage(primitive);
					this.context.getNetwork().getMessaging().send(urgentItemMessage, connection);
					
					if (gossipLog.hasLevel(Logging.DEBUG))
						gossipLog.debug(this.context.getName()+": Sent urgent item "+urgentItemMessage.asInventory()+" to "+connection.toString());
				}
				else
				{
					if (itemsMessage == null)
						itemsMessage = new ItemsMessage();
					
					itemsMessage.add(primitive);
					
					// TODO 
					if (itemsMessage.isAtCapacity())
					{
						this.context.getNetwork().getMessaging().send(itemsMessage, connection);
	
						if (gossipLog.hasLevel(Logging.DEBUG))
							gossipLog.debug(this.context.getName()+": Sent items "+itemsMessage.asInventory()+" to "+connection.toString());
	
						itemsMessage = null;
					}
				}
			}
			
			if (itemsMessage != null)
			{
				this.context.getNetwork().getMessaging().send(itemsMessage, connection);

				if (gossipLog.hasLevel(Logging.DEBUG))
					gossipLog.debug(this.context.getName()+": Sent items "+itemsMessage.asInventory()+" to "+connection.toString());
			}

			// Some items were not fetched?
			if (fetched.size() < inventory.size())
			{
				final Map<Hash, InventoryItem> itemsMissing = inventory.stream().collect(Collectors.toMap(i -> i.getHash(), i -> i));
				for(final Primitive primitive : fetched)
					itemsMissing.remove(primitive.getHash());
				gossipLog.warn(this.context.getName()+": Did not fetch "+itemsMissing.size()+"/"+inventory.size()+" "+itemsMissing.values()+" for "+connection);
			}
		}
		catch (Throwable t)
		{
			gossipLog.error(GossipHandler.this.context.getName()+": Failed to process fetch of "+message.asInventory()+" to "+connection, t);
		}
	}
	
	// ATOM SUBMIT ONLY! //
	public Collection<Hash> required(final Collection<Hash> inventory, final Class<? extends Primitive> type, final AbstractConnection connection)
	{
		final Set<Hash> required = Sets.mutable.<Hash>withInitialCapacity(inventory.size());

		try
		{
			this.lock.lock();
			try
			{
				boolean isNormalized = true;
				final Iterator<Hash> inventoryIterator = inventory.iterator();
				while(inventoryIterator.hasNext())
				{
					final Hash item = inventoryIterator.next();
					final InventoryItem inventoryItem = new InventoryItem(type, item);
						
					if (this.toRequest.contains(inventoryItem))
						continue;
					
					if (this.itemsRequested.containsKey(inventoryItem))
						continue;
						
					if (required.add(item) == false && isNormalized)
					{
						gossipLog.warn(this.context.getName()+": Inventory containing "+inventory.size()+" items of type "+type+" is not normalized from "+connection);
						isNormalized = false;
						// TODO Disconnect?  Ban?
					}
				}
			}
			finally
			{
				this.lock.unlock();
			}
	
			final GossipInventory inventoryProcessor = this.inventoryProcessors.get(type);
			if (inventoryProcessor == null)
			{
				gossipLog.error(this.context.getName()+": Inventory processor for "+type+" is not found");
				return null;
			}
				
			final Collection<Hash> results = inventoryProcessor.required(type, required, connection);
			this.context.getMetaData().increment("gossip.required."+type.getSimpleName().toLowerCase(), results.size());
			return results;
		}
		catch (Throwable t)
		{
			gossipLog.error(GossipHandler.this.context.getName()+": Failed to process inventory requirements of "+inventory+" from "+connection, t);
			return Collections.emptySet();
		}
	}
	
	private void inventory(final InventoryMessage message, final AbstractConnection connection)
	{
		Objects.requireNonNull(message, "Message is null");
		Objects.requireNonNull(connection, "Connection is null");

		try
		{
			if (connection.isStale())
			{
				if (gossipLog.hasLevel(Logging.DEBUG))
					gossipLog.warn(this.context.getName()+": Inventory containing "+message.asInventory()+" received from stale connection "+connection);
			}
			else
			{
				if (gossipLog.hasLevel(Logging.DEBUG))
					gossipLog.debug(this.context.getName()+": Processing inventory containing "+message.asInventory()+" items from "+connection);
			}

			int cacheHits = 0;
			int cacheMisses = 0;
			boolean isNormalized = true;
			final List<InventoryItem> inventory = message.asInventory();
			final Set<InventoryItem> required = Sets.mutable.<InventoryItem>withInitialCapacity(inventory.size());
			final MutableSetMultimap<Class<? extends Primitive>, Hash> itemsByType = Multimaps.mutable.set.empty();
			
			// Sort to types for efficient inventory processing
			for (int i = 0 ; i < inventory.size() ; i++)
			{
				final InventoryItem inventoryItem = inventory.get(i);
				itemsByType.put(inventoryItem.getType(), inventoryItem.getHash());
			}
			
			// Query inventory processors
			for (final Class<? extends Primitive> type : itemsByType.keySet())
			{
				final GossipInventory inventoryProcessor = this.inventoryProcessors.get(type);
				if (inventoryProcessor == null)
				{
					gossipLog.error(this.context.getName()+": Inventory processor for "+type+" is not found");
					return;
				}
				
				final Collection<Hash> results = inventoryProcessor.required(type, itemsByType.get(type), connection);
				for (Hash result : results)
				{
					if (required.add(new InventoryItem(type, result)) == false && isNormalized)
					{
						gossipLog.warn(this.context.getName()+": Inventory containing "+inventory.size()+" items is not normalized from "+connection);
						isNormalized = false;
						// TODO Disconnect?  Ban?
					}
				}
				
				final String typeSimpleName = type.getSimpleName().toLowerCase();
				this.context.getMetaData().increment("gossip.required."+typeSimpleName, results.size());
				this.context.getMetaData().increment("gossip.inventories."+typeSimpleName, itemsByType.get(type).size());
			}

			if (required.isEmpty() == false)
			{
				this.lock.lock();
				try
				{
					// Add required items to the connections available
					// Need to check here the status of any required items already in the gossip pipeline
					for (final InventoryItem item : required)
					{
						final GossipRequestTask requestTask = this.itemsRequested.get(item);
						if (requestTask == null)
						{
							this.itemSources.put(item, connection);
							this.toRequest.add(item);
						}
						else
						{
							// Was already delivered?
							if (requestTask.getStatus(item).equals(GossipRequestTask.Status.DELIVERED))
								continue;
							
							// Is pending?
							if (requestTask.getStatus(item).equals(GossipRequestTask.Status.PENDING))
							{
								this.itemSources.put(item, connection);
								continue;
							}
								
							// WTF?
							throw new IllegalStateException("Gossip state for item "+item+" is invalid");
						}
					}
				}
				finally
				{
					this.lock.unlock();
				}
			}

			this.context.getMetaData().increment("gossip.cache.misses", cacheMisses);
			this.context.getMetaData().increment("gossip.cache.hits", cacheHits);

			this.context.getMetaData().increment("gossip.inventories.items", inventory.size());
			this.context.getMetaData().increment("gossip.inventories.total");

			this.context.getMetaData().increment("gossip.required.items", required.size());
			this.context.getMetaData().increment("gossip.required.total");
		}
		catch(Throwable t)
		{
			gossipLog.error(GossipHandler.this.context.getName()+": Failed to process "+message.getClass().getAnnotation(SerializerId2.class).value()+" containing inventory "+message.asInventory()+" from "+connection, t);
		}
	}
	
	private Collection<InventoryItem> request(final AbstractConnection connection, final List<InventoryItem> items) throws IOException
	{
		if (items.size() > Constants.MAX_REQUEST_INVENTORY_ITEMS)
			throw new IOException("Request size of "+items.size()+" exceeds allowed maximum of "+Constants.MAX_REQUEST_INVENTORY_ITEMS);
		
		int itemsWeight = 0;
		int itemsPending = 0;
		final MutableSet<InventoryItem> itemsToRequest = Sets.mutable.withInitialCapacity(items.size());
		this.lock.lock();
		try
		{
			if (GossipHandler.this.context.getConfiguration().get("network.gossip.requests.singleton", false).equals(Boolean.TRUE))
			{
				for (final GossipRequestTask gossipRequestTask : GossipHandler.this.requestTasks.get(connection))
				{
					if (gossipRequestTask.numRemaining() != 0)
						throw new IOException("Gossip task already pending for "+connection);
				}
			}

			for (int i = 0 ; i < items.size() ; i++)
			{
				final InventoryItem item = items.get(i);
				if (this.itemsRequested.containsKey(item) == false)
					itemsToRequest.add(item);

				itemsPending++;
			}

			if (itemsPending == 0)
			{
				gossipLog.warn(GossipHandler.this.context.getName()+": No items required from "+connection.toString());
				return Collections.emptySet();
			}
			
			if (itemsToRequest.isEmpty())
				return Collections.emptySet();

			final long gossipRequestTimeout = connection.getNextTimeout(itemsToRequest.size(), TimeUnit.MILLISECONDS);
			final GossipRequestTask gossipRequestTask = new GossipRequestTask(connection, itemsToRequest, Math.min(gossipRequestTimeout, Constants.MAX_GOSSIP_REQUEST_TIMEOUT_MILLISECONDS), TimeUnit.MILLISECONDS);
			try
			{
				for (final InventoryItem itemToRequest : itemsToRequest)
				{
					if (this.itemsRequested.putIfAbsent(itemToRequest, gossipRequestTask) != null)
						throw new IllegalStateException("Request task already associated with item "+itemToRequest);
					
					itemsWeight += itemToRequest.getWeight();
				}
				
				this.toRequest.removeAll(itemsToRequest);
				this.requestTasks.put(connection, gossipRequestTask);
				
				this.context.getNetwork().getMessaging().send(new GetItemsMessage(itemsToRequest), connection);
				
				Executor.getInstance().schedule(gossipRequestTask);
				
				if (gossipLog.hasLevel(Logging.INFO))
					gossipLog.info(GossipHandler.this.context.getName()+": Requested "+itemsToRequest.size()+" items "+itemsToRequest+" with request ID "+gossipRequestTask.getID()+":"+gossipRequestTimeout+"ms from "+connection.toString());
			}
			catch (Throwable t)
			{
				gossipRequestTask.cancel();
				
				for (final InventoryItem itemToRequest : itemsToRequest)
				{
					this.toRequest.add(itemToRequest);
					this.itemsRequested.remove(itemToRequest);
				}
				throw t;
			}
		}
		finally
		{
			this.lock.unlock();
		}
		
		final ObjectLongHashMap<String> itemCountsByType = ObjectLongHashMap.newMap();
		itemsToRequest.forEachWith((item, p) -> p.addToValue(item.getType().getSimpleName(), 1), itemCountsByType);
		itemCountsByType.forEachKeyValue((s, l) -> this.context.getMetaData().increment("gossip.requests."+s.toLowerCase(), l));
		
		this.context.getMetaData().increment("gossip.requests.items", itemsToRequest.size());
		this.context.getMetaData().increment("gossip.requests.total");
		
		connection.incrementPendingRequests();
		connection.incrementPendingRequested(itemsToRequest.size());
		connection.incrementPendingWeight(itemsWeight);
		
		connection.updateRequests(1);
		connection.updateRequested(itemsToRequest.size());
		
		return itemsToRequest;
	}
	
	private void received(final ItemsMessage message, final AbstractConnection connection)
	{
		Objects.requireNonNull(message, "Message is null");
		Objects.requireNonNull(connection, "Connection is null");
		
		try
		{
			final List<InventoryItem> inventory = message.asInventory();
			final List<InventoryItem> unrequested = new ArrayList<InventoryItem>(Math.min(inventory.size(), 4));

			if (gossipLog.hasLevel(Logging.INFO))
				gossipLog.info(GossipHandler.this.context.getName()+": Processing received items "+inventory.stream().map(i -> i.getHash()).collect(Collectors.toList())+" from "+connection.toString());

			final MutableSetMultimap<Class<? extends Primitive>, Primitive> itemsByType = Multimaps.mutable.set.empty();
			this.lock.lock();
			try
			{
				for (int i = 0 ; i < inventory.size() ; i++)
				{
					final InventoryItem item = inventory.get(i);
					final GossipRequestTask itemRequestTask = this.itemsRequested.remove(item);
					if (itemRequestTask == null)
					{
						unrequested.add(item);
						continue;
					}
					
					itemRequestTask.received(item);
					itemsByType.put(item.getType(), item.getPrimitive());
					this.itemSources.removeAll(item);

					if (gossipLog.hasLevel(Logging.TRACE) && item.getHash().asLong() % 1000 == 0)
						gossipLog.trace(GossipHandler.this.context.getName()+": Witnessed item "+item.getType()+":"+item.getHash());
				}
			}
			finally
			{
				this.lock.unlock();
			}
			
			for (final Class<? extends Primitive> type : itemsByType.keySet())
			{
				final GossipReceiver receiver = this.receiverProcessors.get(type);
				if (receiver == null)
				{
					gossipLog.warn(this.context.getName()+": No receiver found for type "+type);
					continue;
				}
				
				receiver.receive(itemsByType.get(type), connection);
			}

			if (unrequested.isEmpty() == false)
				connection.strikeOrDisconnect("Received unrequested items "+unrequested);
		}
		catch(Throwable t)
		{
			gossipLog.error(GossipHandler.this.context.getName()+": Failed to process received items "+message.asInventory()+" items from "+connection, t);
		}
	}
	
   	// ASYNC PEER LISTENER //
   	private EventListener asyncPeerListener = new EventListener()
   	{
    	@Subscribe
		public void on(final DisconnectedEvent event)
		{
   			GossipHandler.this.lock.lock();
    		try
    		{
     			List<GossipRequestTask> gossipRequestTasks = GossipHandler.this.requestTasks.get(event.getConnection()).toList();
				for (int i = 0 ; i < gossipRequestTasks.size() ; i++)
				{
					GossipRequestTask gossipRequestTask = gossipRequestTasks.get(i);
    				try
    				{
    					if (gossipRequestTask.isFinished() == false && gossipRequestTask.isCancelled() == false)
    					{
    						if (gossipRequestTask.cancel() == true && gossipLog.hasLevel(Logging.INFO))
    							gossipLog.info(GossipHandler.this.context.getName()+": Cancelled pending gossip task with remaining items "+gossipRequestTask.numRemaining()+" from "+event.getConnection());
    					}
    				}
    	    		catch (Throwable t)
    	    		{
    	    			gossipLog.error(GossipHandler.this.context.getName()+": Failed to cancel pending gossip task with remaining items "+gossipRequestTask.numRemaining()+" from "+event.getConnection());
    	    		}
    			}
    		}
    		finally
    		{
    			GossipHandler.this.lock.unlock();
    		}
		}
	};
	
	// SYNC CHANGE LISTENER //
	private SynchronousEventListener syncChangeListener = new SynchronousEventListener()
	{
		@Subscribe
		public void on(final SyncLostEvent event)
		{
			gossipLog.log(GossipHandler.this.context.getName()+": Sync status lost, resetting");
   			GossipHandler.this.lock.lock();
    		try
    		{
				GossipHandler.this.broadcastQueue.clear();
				GossipHandler.this.eventQueue.clear();
				GossipHandler.this.itemSources.clear();
				GossipHandler.this.toRequest.clear();
				GossipHandler.this.itemsRequested.clear();
	
				for (final GossipRequestTask requestTask : GossipHandler.this.requestTasks.valuesView())
				{
					if (requestTask.isCancelled() == false)
					{
						if (requestTask.cancel())
							gossipLog.warn(GossipHandler.this.context.getName()+": Cancelled GossipTask "+hashCode()+" on sync change with "+requestTask.numRemaining()+"/"+requestTask.numRequested()+" "+requestTask.getRemaining()+" for "+requestTask.getConnection());
					}
				}
				GossipHandler.this.requestTasks.clear();
    		}
    		finally
    		{
    			GossipHandler.this.lock.unlock();
    		}
		}
	};
}
