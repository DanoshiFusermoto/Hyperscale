package org.radix.hyperscale.ledger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.radix.hyperscale.Constants;
import org.radix.hyperscale.Context;
import org.radix.hyperscale.Service;
import org.radix.hyperscale.concurrency.MonitoredReentrantLock;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.events.SynchronousEventListener;
import org.radix.hyperscale.exceptions.StartupException;
import org.radix.hyperscale.exceptions.TerminationException;
import org.radix.hyperscale.executors.Executor;
import org.radix.hyperscale.executors.ScheduledExecutable;
import org.radix.hyperscale.ledger.messages.GetSubstateMessage;
import org.radix.hyperscale.ledger.messages.SubstateMessage;
import org.radix.hyperscale.ledger.primitives.StateInput;
import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;
import org.radix.hyperscale.network.AbstractConnection;
import org.radix.hyperscale.network.ConnectionState;
import org.radix.hyperscale.network.MessageProcessor;
import org.radix.hyperscale.network.StandardConnectionFilter;
import org.radix.hyperscale.network.events.DisconnectedEvent;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.eventbus.Subscribe;

import io.netty.util.internal.ThreadLocalRandom;

public final class SubstateRequestHandler implements Service
{
	private static final Logger gossipLog = Logging.getLogger("gossip");

	private static final int SUBSTATE_RECORD_LIFETIME_MS = (int) (Ledger.definitions().roundInterval() * (Constants.MIN_COMMIT_ROUNDS / 2));
	
	final class SubstateRequest extends CompletableFuture<Substate>
	{
		private final long requestedAt;
		private final StateAddress address;
		private final Hash version;
		
		private volatile int reattempts;
		private volatile long reattemptedAt;
		private volatile long witnessedAt;
		private volatile long timeout;

		private volatile AbstractConnection connection;
		
		SubstateRequest(final StateAddress address, final Hash version)
		{
			this.requestedAt = System.currentTimeMillis();
			this.witnessedAt = Long.MAX_VALUE;
			this.address = address;
			this.version = version;
			this.reattempts = 0;
			this.connection = null;
		}
		
		private void reset()
		{
			// Reset request quotas on old connection
			if (this.connection != null)
			{
				this.connection.decrementPendingRequests();
				this.connection.decrementPendingRequested();
				this.connection.decrementPendingWeight();
			}
			
			this.connection = null;
		}
		
		boolean isTimeout()
		{
			if (isDone())
				return false;
			
			if (this.reattempts == 0)
				return (System.currentTimeMillis() - this.requestedAt) > this.timeout;
			else
				return (System.currentTimeMillis() - this.reattemptedAt) > this.timeout;
		}
		
		boolean isStale()
		{
			if (isDone() == false)
				return false;
			
			if (System.currentTimeMillis() - this.witnessedAt < SubstateRequestHandler.SUBSTATE_RECORD_LIFETIME_MS)
				return false;
			
			return true;
		}
		
		@Override
		public boolean complete(final Substate substate)
		{
			Objects.requireNonNull(substate, "Substate is null");
			if (substate.getAddress().equals(this.address) == false)
				throw new IllegalArgumentException("Substate address "+substate.getAddress()+" does not match expected "+this.address);

			this.witnessedAt = System.currentTimeMillis();
			if (this.reattempts == 0)
				this.connection.updateLatency(this.witnessedAt - this.requestedAt);
			else
				this.connection.updateLatency(this.witnessedAt - this.reattemptedAt);
			
			reset();
			
			return super.complete(substate);
		}
		
		@Override
		public boolean completeExceptionally(final Throwable ex)
		{
		    reset();
		    
		    return super.completeExceptionally(ex);
		}

		protected long getRequestedAt()
		{
			return this.requestedAt;
		}

		protected Hash getVersion()
		{
			return this.version;
		}
		
		protected StateAddress getAddress()
		{
			return this.address;
		}

		protected long getWitnessedAt()
		{
			return this.witnessedAt;
		}
		
		private AbstractConnection getConnection()
		{
			return this.connection;
		}
		
		private int reattempts()
		{
			return this.reattempts;
		}

		private void reattempt(final AbstractConnection connection, long timeout, TimeUnit timeunit) throws IOException
		{
			send(connection);
			this.timeout = TimeUnit.MILLISECONDS.convert(timeout, timeunit);
			this.reattempts++;
			this.reattemptedAt = System.currentTimeMillis();
		}
		
		private void request(final AbstractConnection connection, long timeout, TimeUnit timeunit) throws IOException
		{
			send(connection);
			this.timeout = TimeUnit.MILLISECONDS.convert(timeout, timeunit);
		}
		
		private void send(final AbstractConnection connection) throws IOException
		{
			reset();
			
			this.connection = connection;
			
			final GetSubstateMessage getSubstateMessage = new GetSubstateMessage(this.address, this.version);
			SubstateRequestHandler.this.context.getNetwork().getMessaging().send(getSubstateMessage, connection);
			
			connection.incrementPendingRequests();
			connection.incrementPendingRequested();
			connection.incrementPendingWeight();
			connection.updateRequests(1);
			connection.updateRequested(1);
			
			if (gossipLog.hasLevel(Logging.DEBUG))
				gossipLog.debug(SubstateRequestHandler.this.context.getName()+": Sent remote substate read for "+this.address+" version "+this.version+" to "+this.connection);
		}
	}
	
	private final Context context;
	
	private final Map<Hash, SubstateRequest> pendingRequests;
	private final Map<Hash, SubstateRequest> completedRequests;
	private final Multimap<AbstractConnection, Hash> requestAssignment;

	private final MonitoredReentrantLock lock;
	
	private final ScheduledExecutable timeoutExecutor = new ScheduledExecutable(0, Ledger.definitions().roundInterval(), TimeUnit.MILLISECONDS)
	{
		final List<SubstateRequest> timedoutSubstateRequests = new ArrayList<SubstateRequest>();
		@Override
		public void execute()
		{
			this.timedoutSubstateRequests.clear();
			
			SubstateRequestHandler.this.lock.lock();
			try
			{
				for (final SubstateRequest substateRequest : SubstateRequestHandler.this.pendingRequests.values())
				{
					if (substateRequest.isTimeout() == false)
						continue;
					
					this.timedoutSubstateRequests.add(substateRequest);
				}
				
				if (this.timedoutSubstateRequests.isEmpty() == false)
					SubstateRequestHandler.this.reattempt(this.timedoutSubstateRequests);
				
				final Iterator<SubstateRequest> completedRequestsIterator = SubstateRequestHandler.this.completedRequests.values().iterator();
				while(completedRequestsIterator.hasNext())
				{
					final SubstateRequest completedSubstateRequest = completedRequestsIterator.next();

					if (completedSubstateRequest.isStale() == false)
						continue;
					
					completedRequestsIterator.remove();
				}
			}
			finally
			{
				SubstateRequestHandler.this.lock.unlock();
			}
		}
	};
	private Future<?> timeoutExecutorTask;

	SubstateRequestHandler(final Context context)
	{
		this.context = Objects.requireNonNull(context, "Context is null");
		this.lock = new MonitoredReentrantLock(this.context.getName()+" Substate Request Handler Lock", true);

		this.pendingRequests = Collections.synchronizedMap(new HashMap<>());
		this.completedRequests = Collections.synchronizedMap(new HashMap<>());
		this.requestAssignment = HashMultimap.create();
	}

	@Override
	public void start() throws StartupException
	{
		// SUBSTATE //
		this.context.getNetwork().getMessaging().register(GetSubstateMessage.class, this.getClass(), new MessageProcessor<GetSubstateMessage>()
		{
			@Override
			public void process(final GetSubstateMessage getSubstateMessage, final AbstractConnection connection)
			{
				final int numShardGroups = SubstateRequestHandler.this.context.getLedger().numShardGroups();
				final ShardGroupID localShardGroupID = ShardMapper.toShardGroup(SubstateRequestHandler.this.context.getNode().getIdentity(), numShardGroups);
				final ShardGroupID substateShardGroupID = ShardMapper.toShardGroup(getSubstateMessage.getAddress(), numShardGroups);
				if (ShardMapper.equal(SubstateRequestHandler.this.context.getLedger().numShardGroups(), getSubstateMessage.getAddress(), SubstateRequestHandler.this.context.getNode().getIdentity()) == false)
				{
					gossipLog.error(SubstateRequestHandler.this.context.getName()+": Request from "+connection+" for substate in shard group "+substateShardGroupID+" but local is "+localShardGroupID);
					// Disconnect and ban?
					return;
				}
				
				if (SubstateRequestHandler.this.context.getConfiguration().get("gossip.faults.force.nondelivery.interval", 0l) > 0 && 
					ThreadLocalRandom.current().nextLong() % SubstateRequestHandler.this.context.getConfiguration().get("gossip.faults.force.nondelivery.interval", 0l) == 0)
				{
					gossipLog.warn(SubstateRequestHandler.this.context.getName()+": Not delivering substate "+getSubstateMessage.getAddress()+" version "+getSubstateMessage.getVersion()+" as per failure configuration");
					return;
				}
				
				try
				{
					Substate substate = null;
					// Check StateInput inventory, as they are versioned by default, faster
					if (getSubstateMessage.getVersion().equals(Hash.ZERO) == false)
					{
						final StateInput stateInput = SubstateRequestHandler.this.context.getLedger().getLedgerStore().get(Hash.hash(getSubstateMessage.getVersion(), getSubstateMessage.getAddress().getHash()), StateInput.class);
						if (stateInput != null)
							substate = stateInput.getSubstate();
					}

					// Read the current version
					if (substate == null)
					{
						// TODO write lock check
						substate = SubstateRequestHandler.this.context.getLedger().getLedgerStore().get(getSubstateMessage.getAddress());
					}
					
					if (gossipLog.hasLevel(Logging.INFO))
						gossipLog.info(SubstateRequestHandler.this.context.getName()+": Sending substate "+substate.getAddress()+" to "+connection);
					
					SubstateMessage substateMessage = new SubstateMessage(substate, getSubstateMessage.getVersion());
					SubstateRequestHandler.this.context.getNetwork().getMessaging().send(substateMessage, connection);
				}
				catch (Exception ex)
				{
					gossipLog.error(SubstateRequestHandler.this.context.getName()+": "+getSubstateMessage.MSN()+"  " + connection, ex);
				}
			}
		});
				
		this.context.getNetwork().getMessaging().register(SubstateMessage.class, this.getClass(), new MessageProcessor<SubstateMessage>()
		{
			@Override
			public void process(final SubstateMessage substateMessage, final AbstractConnection connection)
			{
				try
				{
					if (SubstateRequestHandler.this.context.getNode().isSynced() == false)
						return;

					if (gossipLog.hasLevel(Logging.INFO))
						gossipLog.info(SubstateRequestHandler.this.context.getName()+": Received substate "+substateMessage.getSubstate().getAddress()+" version "+substateMessage.getVersion()+" from "+connection);

					final Hash lookupHash = requestLookUpHash(substateMessage.getSubstate().getAddress(), substateMessage.getVersion());
					SubstateRequestHandler.this.lock.lock();
					try
					{
						if (SubstateRequestHandler.this.requestAssignment.remove(connection, lookupHash) == false)
						{
							gossipLog.warn(SubstateRequestHandler.this.context.getName()+": Received unrequested substate "+substateMessage.getSubstate().getAddress()+" version "+substateMessage.getVersion()+" from "+connection);
							// DISCONNECT / BAN
							return;
						}
						
						final SubstateRequest substateRequest = SubstateRequestHandler.this.pendingRequests.remove(lookupHash);
						if (substateRequest == null)
						{
							gossipLog.warn(SubstateRequestHandler.this.context.getName()+": Substate request not found for "+substateMessage.getSubstate().getAddress()+" version "+substateMessage.getVersion()+" from "+connection);
							return;
						}
						
						substateRequest.complete(substateMessage.getSubstate());
						if (substateRequest.reattempts() > 0)
							gossipLog.warn(SubstateRequestHandler.this.context.getName()+": Substate request fulfilled on attempt "+substateRequest.reattempts()+" for "+substateMessage.getSubstate().getAddress()+" version "+substateMessage.getVersion()+" from "+connection);
							
						SubstateRequestHandler.this.completedRequests.put(lookupHash, substateRequest);
					}
					finally
					{
						SubstateRequestHandler.this.lock.unlock();
					}
				}
				catch (Exception ex)
				{
					gossipLog.error(SubstateRequestHandler.this.context.getName()+": "+substateMessage.MSN()+" " + connection, ex);
				}
			}
		});
		
		this.context.getEvents().register(this.peerListener);
		
		this.timeoutExecutorTask = Executor.getInstance().schedule(this.timeoutExecutor);
	}

	@Override
	public void stop() throws TerminationException
	{
		this.timeoutExecutorTask.cancel(false);
		
		this.context.getEvents().unregister(this.peerListener);
		
		this.context.getNetwork().getMessaging().deregisterAll(getClass());
	}
	
	private Hash requestLookUpHash(final StateAddress address, final Hash version)
	{
		if (version.equals(Hash.ZERO))
			return address.getHash();
		else
			return PendingState.getHash(version, address);
	}
	
	SubstateRequest request(final StateAddress address) throws IOException
	{
		return request(address, Hash.ZERO);
	}
	
	SubstateRequest request(final StateAddress address, final Hash version) throws IOException
	{
		Objects.requireNonNull(address, "State address is null");
		Objects.requireNonNull(version, "Version is null");
		
		final Hash lookupHash = requestLookUpHash(address, version);

		this.lock.lock();
		try
		{
			// A request completed recent enough to use the result?
			final SubstateRequest completedRequestFuture = this.completedRequests.get(lookupHash);
			if (completedRequestFuture != null)
			{
				if (completedRequestFuture.isStale() == false)
					return completedRequestFuture;
				
				if (this.completedRequests.remove(lookupHash, completedRequestFuture) == false)
					gossipLog.warn(this.context.getName()+": Expired substate request not removed for "+completedRequestFuture.getAddress()+" with version hash "+completedRequestFuture.getVersion());
			}
			
			// Is there a current pending request?
			final SubstateRequest existingPendingRequestFuture = this.pendingRequests.get(lookupHash);
			if (existingPendingRequestFuture != null)
				return existingPendingRequestFuture;
			
        	// Make a new request
			final SubstateRequest pendingRequestFuture = new SubstateRequest(address, version);
			
			// Can complete immediately via a state input?
			final StateInput stateInput = this.context.getLedger().getLedgerStore().get(lookupHash, StateInput.class);
			if (stateInput != null)
			{
				pendingRequestFuture.complete(stateInput.getSubstate());
				this.completedRequests.put(lookupHash, pendingRequestFuture);
				return pendingRequestFuture;
			}

			// Nope, need to actually request it
			final int numShardGroups = this.context.getLedger().numShardGroups();
			final ShardGroupID localShardGroupID = ShardMapper.toShardGroup(this.context.getNode().getIdentity(), numShardGroups);
			final ShardGroupID substateShardGroupID = ShardMapper.toShardGroup(address, numShardGroups);
        	if (substateShardGroupID.equals(localShardGroupID))
        		throw new IllegalArgumentException("Shard group for substate request "+address+" is local");

        	// Discover shard connections
			final StandardConnectionFilter connectionFilter = StandardConnectionFilter.build(this.context).setStates(ConnectionState.SELECT_CONNECTED).setSynced(true).setStale(false).setShardGroupID(substateShardGroupID);
			final List<AbstractConnection> connections = this.context.getNetwork().get(connectionFilter);
			if (connections.isEmpty())
				throw new IOException("No remote connections available to request substate "+address+" version "+version);
			
			this.pendingRequests.put(lookupHash, pendingRequestFuture);

			final AbstractConnection requestConnection = connections.getFirst();
			try
			{
				this.requestAssignment.put(requestConnection, lookupHash);

				long timeout = Math.min(requestConnection.getNextTimeout(1, TimeUnit.MILLISECONDS), Constants.MAX_DIRECT_REQUEST_TIMEOUT_MILLISECONDS);
				pendingRequestFuture.request(requestConnection, timeout, TimeUnit.MILLISECONDS);	
			}
			catch(Exception ex)
			{
				// Clean up failed request
				this.requestAssignment.remove(requestConnection, lookupHash);
				this.pendingRequests.remove(lookupHash, pendingRequestFuture);
				throw ex;
			}
			
			return pendingRequestFuture;
		}
		finally
		{
			this.lock.unlock();
		}
	}
	
	private void reattempt(final List<SubstateRequest> requests)
	{
		final int numShardGroups = this.context.getLedger().numShardGroups();
		final ShardGroupID localShardGroupID = ShardMapper.toShardGroup(this.context.getNode().getIdentity(), numShardGroups);

		this.lock.lock();
		try
		{
			for (final SubstateRequest request : requests)
			{
				final Hash lookupHash = requestLookUpHash(request.getAddress(), request.getVersion());
				try
				{
					final AbstractConnection requestConnection = request.getConnection();
					this.requestAssignment.remove(requestConnection, lookupHash);
					
					try
					{
						if (requestConnection.getState().equals(ConnectionState.CONNECTED) || requestConnection.getState().equals(ConnectionState.CONNECTING))
							requestConnection.disconnect("Did not respond fully to substate request "+request.getAddress()+" version "+request.getVersion());
					}
					catch (Throwable t)
					{
						gossipLog.error(SubstateRequestHandler.this.context.getName()+": "+requestConnection.toString(), t);
					}
					
					if (request.reattempts() == Constants.MAX_DIRECT_ITEM_RETRIES)
					{
						request.completeExceptionally(new IllegalArgumentException("Re-attempts exceeded for substate request "+request.getAddress()+" version "+request.getVersion()));
						continue;
					}
					
					final ShardGroupID substateShardGroupID = ShardMapper.toShardGroup(request.getAddress(), numShardGroups);
					if (substateShardGroupID.equals(localShardGroupID))
					{
						request.completeExceptionally(new IllegalArgumentException("Shard group for substate request "+request.getAddress()+" is local"));
						continue;
					}
	
					// Discover shard connections
					final StandardConnectionFilter connectionFilter = StandardConnectionFilter.build(this.context).setStates(ConnectionState.SELECT_CONNECTED).setSynced(true).setStale(false).setShardGroupID(substateShardGroupID);
					final List<AbstractConnection> connections = this.context.getNetwork().get(connectionFilter);
					if (connections.isEmpty())
					{
						request.completeExceptionally(new IOException("No remote connections available to re-request substate "+request.getAddress()+" version "+request.getVersion()));
						continue;
					}
	
					final AbstractConnection reattemptConnection = connections.getFirst();
					try
					{
						long timeout = Math.min(reattemptConnection.getNextTimeout(1, TimeUnit.MILLISECONDS), Constants.MAX_DIRECT_REQUEST_TIMEOUT_MILLISECONDS);
						request.reattempt(reattemptConnection, timeout, TimeUnit.MILLISECONDS);
						this.requestAssignment.put(reattemptConnection, lookupHash);
					}
					catch(Exception ex)
					{
						this.requestAssignment.remove(reattemptConnection, lookupHash);
						request.completeExceptionally(new IOException("Failed to re-request substate "+request.getAddress()+" version "+request.getVersion(), ex));
					}
				}
				finally
				{
					if (request.isDone())
						this.pendingRequests.remove(lookupHash, request);
				}
			}
		}
		finally
		{
			this.lock.unlock();
		}
	}

	// SYNC CHANGE LISTENER //
	private SynchronousEventListener peerListener = new SynchronousEventListener()
	{
    	@Subscribe
		public void on(final DisconnectedEvent event)
		{
    		SubstateRequestHandler.this.lock.lock();
    		try
    		{
    			final Collection<Hash> requestAssignments = SubstateRequestHandler.this.requestAssignment.removeAll(event.getConnection());
    			final List<SubstateRequest> incompleteRequests = new ArrayList<SubstateRequest>();
    			for (Hash requestAssignmentLookupHash : requestAssignments)
    			{
    				final SubstateRequest substateRequest = SubstateRequestHandler.this.pendingRequests.get(requestAssignmentLookupHash);
    				if (substateRequest == null)
    				{
    					gossipLog.warn(SubstateRequestHandler.this.context.getName()+": Expected substate request for "+requestAssignmentLookupHash+" not found on disconnect "+event.getConnection());
    					continue;
    				}
    				
    				if (substateRequest.isDone() == true)
    				{
    					gossipLog.warn(SubstateRequestHandler.this.context.getName()+": Substate request is completed on disconnect for "+requestAssignmentLookupHash+" to "+event.getConnection());
    					continue;
    				}
    				
    				incompleteRequests.add(substateRequest);
    			}
    			
    			reattempt(incompleteRequests);
    		}
    		finally
    		{
    			SubstateRequestHandler.this.lock.unlock();
    		}
		}
	};
}
