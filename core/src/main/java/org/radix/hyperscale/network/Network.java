package org.radix.hyperscale.network;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.radix.hyperscale.Constants;
import org.radix.hyperscale.Context;
import org.radix.hyperscale.Service;
import org.radix.hyperscale.Universe;
import org.radix.hyperscale.WebSocketService;
import org.radix.hyperscale.common.Agent;
import org.radix.hyperscale.common.Direction;
import org.radix.hyperscale.crypto.CryptoException;
import org.radix.hyperscale.crypto.Identity;
import org.radix.hyperscale.events.SynchronousEventListener;
import org.radix.hyperscale.exceptions.StartupException;
import org.radix.hyperscale.exceptions.TerminationException;
import org.radix.hyperscale.executors.Executable;
import org.radix.hyperscale.executors.Executor;
import org.radix.hyperscale.executors.ScheduledExecutable;
import org.radix.hyperscale.ledger.Epoch;
import org.radix.hyperscale.ledger.ShardGroupID;
import org.radix.hyperscale.ledger.ShardMapper;
import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;
import org.radix.hyperscale.network.discovery.OutboundDiscoveryFilter;
import org.radix.hyperscale.network.events.ConnectedEvent;
import org.radix.hyperscale.network.events.ConnectingEvent;
import org.radix.hyperscale.network.events.DisconnectedEvent;
import org.radix.hyperscale.network.messages.NodeMessage;
import org.radix.hyperscale.network.peers.Peer;
import org.radix.hyperscale.network.peers.PeerHandler;
import org.radix.hyperscale.node.Node;
import org.radix.hyperscale.time.Time;
import org.radix.hyperscale.utils.UInt128;
import org.radix.hyperscale.utils.URIs;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.eventbus.Subscribe;

public final class Network implements Service
{
	private static final Logger networkLog = Logging.getLogger("network");
	
	static 
	{
		networkLog.setLevel(Logging.INFO);
	}
	
	private final Context		context;
	private final Messaging 	messaging;
	private final PeerHandler	peerHandler;
	private final GossipHandler	gossipHandler;
	private final WebSocketService websocketService;

    private final ReentrantLock connecting = new ReentrantLock();
    private final List<AbstractConnection>	connections = new ArrayList<AbstractConnection>();
    
    // Outbound selection
    private final class OutboundSlot
    {
    	final int index;        
    	final UInt128 rangeStart;
        final UInt128 rangeEnd;
        Identity identity;   
        Identity replacing;   
        
        OutboundSlot(final int index, final int numSlots)
        {
        	this.index = index;
            UInt128 stepSize = UInt128.MAX_VALUE.divide(UInt128.from(numSlots));
            this.rangeStart = stepSize.multiply(UInt128.from(index));
            this.rangeEnd = (index == numSlots - 1) ? UInt128.MAX_VALUE : stepSize.multiply(UInt128.from(index + 1));        
        }
    }
    private final ArrayListMultimap<ShardGroupID, OutboundSlot> outboundSlots;
    
    private ServerSocket 	TCPServerSocket = null;
    private Executable 		TCPListener = new Executable()
    {
        @Override
		public void onTerminated() 
        {
        	if (Network.this.TCPServerSocket == null || Network.this.TCPServerSocket.isClosed())
        		return;
        	
        	try
        	{
        		Network.this.TCPServerSocket.close();
        	}
        	catch (IOException ex)
        	{
        		networkLog.error(Network.this.context.getName()+": Server socket closure error", ex);
        	}
        	finally
        	{
        		Network.this.TCPServerSocket = null;
        	}
		}

		@Override
		public void execute()
		{
	    	while (Network.this.TCPServerSocket.isClosed() == false && isTerminated() == false)
	    	{
	    		try
	    		{
	    			Socket socket = null;

	    			try
	    			{
	    				socket = Network.this.TCPServerSocket.accept();
	    				networkLog.info(Network.this.context.getName()+": Socket request from "+socket.getInetAddress()+" -> "+socket.getLocalPort());
	    			}
	    			catch (SocketTimeoutException socktimeex)
	    			{
	    				continue;
	    			}

					Network.this.connecting.lock();
					try
					{
						new TCPConnection(Network.this.context, socket);
					}
					catch (Exception ex)
					{
						if (networkLog.hasLevel(Logging.DEBUG))
							networkLog.error(Network.this.context.getName()+": TCP "+socket.getInetAddress()+" error", ex);
						else
							networkLog.error(Network.this.context.getName()+": TCP "+socket.getInetAddress()+" error - "+ex.getMessage());
						
						continue;
		    		}
		    		finally
		    		{
		    			Network.this.connecting.unlock();
		    		}
	    		}
	    		catch (IOException ioex)
	    		{
	    			if (Network.this.TCPServerSocket.isClosed() && isTerminated() == false)
	    			{
	    				try
	    				{
	    					networkLog.error(Network.this.context.getName()+": TCPServerSocket died "+Network.this.TCPServerSocket);
		    				Network.this.TCPServerSocket = new ServerSocket(Network.this.TCPServerSocket.getLocalPort(), 32, Network.this.TCPServerSocket.getInetAddress());
		    				Network.this.TCPServerSocket.setReceiveBufferSize(65536);
		    				Network.this.TCPServerSocket.setSoTimeout(0);
		    				networkLog.info(Network.this.context.getName()+": Recreated TCPServerSocket on "+Network.this.TCPServerSocket);
	    				}
	    				catch (Exception ex)
	    				{
	    					networkLog.fatal(Network.this.context.getName()+": TCPServerSocket death is fatal", ex);
	    				}
	    			}
	    		}
	    		catch (Exception ex)
	    		{
	    			networkLog.fatal(Network.this.context.getName()+": TCPServerSocket exception ", ex);
	    		}
	    	}
		}
	};
	
	private Future<?> houseKeepingFuture = null;

    public Network(Context context)
    {
    	super();
    	
    	this.context = Objects.requireNonNull(context);
    	this.messaging = new Messaging(context);
    	this.peerHandler = new PeerHandler(context);
    	this.gossipHandler = new GossipHandler(context);
    	this.websocketService = new WebSocketService(context);
    	
    	this.outboundSlots = ArrayListMultimap.create();
    }
    
    private void resetOutboundSlots(final ShardGroupID localShardGroup, final int totalShardGroups) 
    {
    	this.outboundSlots.clear();
        
    	for(int s = 0 ; s < totalShardGroups ; s++)
    	{
    		final ShardGroupID shardGroupID = ShardGroupID.from(s);
    		int numConnections = localShardGroup.equals(shardGroupID) ? this.context.getConfiguration().get("network.connections.out.sync", Constants.DEFAULT_TCP_CONNECTIONS_OUT_SYNC) : 
    																	this.context.getConfiguration().get("network.connections.out.shard", Constants.DEFAULT_TCP_CONNECTIONS_OUT_SHARD); 

    		// Distribute remaining slots evenly
    		for (int i = 0 ; i < numConnections ; i++) 
    			this.outboundSlots.put(shardGroupID, new OutboundSlot(i, numConnections));
    	}
    }
    
    public Messaging getMessaging()
    {
    	return this.messaging;
    }

    public PeerHandler getPeerHandler()
    {
    	return this.peerHandler;
    }

    public GossipHandler getGossipHandler()
    {
    	return this.gossipHandler;
    }

    @Override
	public void start() throws StartupException
	{
    	try
    	{
   			this.TCPServerSocket = new ServerSocket();
   			this.TCPServerSocket.setSoTimeout(10000);
   			this.TCPServerSocket.setReuseAddress(true);
   			this.TCPServerSocket.setReceiveBufferSize(this.context.getConfiguration().get("network.tcp.buffer", Constants.DEFAULT_TCP_BUFFER));
   			
   			InetAddress inetAddress = this.context.getConfiguration().has("network.address") == false ? null : InetAddress.getByName(this.context.getConfiguration().get("network.address", "0.0.0.0"));
   			SocketAddress socketAddress = new InetSocketAddress(inetAddress, this.context.getConfiguration().get("network.port", Universe.get().getPort()));
   			this.TCPServerSocket.bind(socketAddress, 32);
   			
   			networkLog.info("TCP server socket "+this.TCPServerSocket.getLocalSocketAddress()+" RCV_BUF: "+this.TCPServerSocket.getReceiveBufferSize());

   			Thread TCPServerThread = new Thread(this.TCPListener); 		
   			TCPServerThread.setDaemon(false);
   			TCPServerThread.setName(this.context.getName()+" TCP Server");
   			TCPServerThread.start();
    		
			this.messaging.start();

			this.context.getEvents().register(this.peerListener);
			this.peerHandler.start();
			this.gossipHandler.start();
			
			if (this.websocketService != null)
				this.websocketService.start();
			
			// HOUSE KEEPING / DISCOVERY //
			this.houseKeepingFuture = Executor.getInstance().scheduleAtFixedRate(new ScheduledExecutable(10, 30, TimeUnit.SECONDS)
			{
				private final Map<ShardGroupID, Long> lastRotation = new HashMap<>(); 
				
				@Override
				public void execute()
				{
					// Discovery / Rotation //
					try
					{
						final int numShardGroups = Network.this.context.getLedger().numShardGroups();
						final ShardGroupID localShardGroupID = ShardMapper.toShardGroup(Network.this.context.getNode().getIdentity(), numShardGroups);
						if (numShardGroups != Network.this.outboundSlots.keySet().size())
							Network.this.resetOutboundSlots(localShardGroupID, numShardGroups);

						updateOutboundStatus();
						
						final Set<Peer> preferredPeers = new LinkedHashSet<Peer>();
						// Sync //
						{
							final ShardGroupID syncShardGroupID = localShardGroupID;

							// IMPORTANT OutboundDiscoveryFilter will filter out recently attempted connections that failed
							final Peer connectTo = updateOutboundSelection(syncShardGroupID);
							if (connectTo != null)
								preferredPeers.add(connectTo);
						}

						// Shard //
						{
							// TODO temporary connectivity over TCP for shard groups ... eventually should be connectionless
							// Round-robin shard connection attempts offset by local ShardGroupID to discourage "connection bashing" in small networks
							final List<ShardGroupID> shardGroupIDs = new ArrayList<>(numShardGroups);
							for (int sg = 0 ; sg < numShardGroups; sg++)
							{
								int shardGroupID = Math.abs((localShardGroupID.intValue()+sg) % numShardGroups);
								shardGroupIDs.add(ShardGroupID.from(shardGroupID));
							}
							
							for (ShardGroupID shardGroupID : shardGroupIDs)
							{
								if (shardGroupID.equals(localShardGroupID))
									continue;
	
								final Peer connectTo = updateOutboundSelection(shardGroupID);
								if (connectTo != null)
									preferredPeers.add(connectTo);
							}
						}
						
						if (preferredPeers.isEmpty() == false)
						{
							for (final Peer preferredPeer : preferredPeers)
							{
				            	final ShardGroupID preferredPeerShardGroupID = ShardMapper.toShardGroup(preferredPeer.getIdentity(), numShardGroups);
								this.lastRotation.put(preferredPeerShardGroupID, System.currentTimeMillis());
								
								Network.this.connecting.lock();
								try
								{
									// Perform this check within the lock for thread safety so that we catch any new INBOUND connections 
									// which match any of our preferred OUTBOUND connections
									if (Network.this.get(preferredPeer.getIdentity(), Protocol.TCP, ConnectionState.CONNECTING, ConnectionState.CONNECTED) != null ||
										Network.this.get(preferredPeer.getHostOnly(), Protocol.TCP, ConnectionState.CONNECTING) != null)
											continue;

									Network.this.connect(preferredPeer.getURI(), Direction.OUTBOUND, Protocol.TCP);
								}
								catch (Exception ex)
								{
									if (networkLog.hasLevel(Logging.DEBUG))
										networkLog.error(Network.this.context.getName()+": TCP "+preferredPeer.getURI().toString()+" connect error", ex);
									else if (ex.getMessage() != null)
										networkLog.error(Network.this.context.getName()+": TCP "+preferredPeer.getURI().toString()+" connect error - "+ex.getMessage());
									else
										networkLog.error(Network.this.context.getName()+": TCP "+preferredPeer.getURI().toString()+" connect error - "+ex.getClass());

									continue;
					    		}
					    		finally
					    		{
					    			Network.this.connecting.unlock();
					    		}
							}
						}
						else if (networkLog.hasLevel(Logging.DEBUG))
							networkLog.debug("Preferred outbound peers already connected");
					}
					catch (Exception ex)
					{
						networkLog.error(ex);
					}

					// System Heartbeat //
					// TODO still need this? or even node objects / messages?
					synchronized(Network.this.connections)
					{
						for (final AbstractConnection connection : Network.this.connections)
						{
							if(connection.getState().equals(ConnectionState.CONNECTED))
							{
								try
								{
									Network.this.getMessaging().send(new NodeMessage(Network.this.context.getNode()), connection);
								}
								catch (IOException ioex)
								{
									networkLog.error("Could not send System heartbeat to "+connection, ioex);
								}
							}
						}
	    			}
				}
				
				private Peer updateOutboundSelection(final ShardGroupID shardGroupID) throws IOException 
				{
					final Epoch epoch = Network.this.context.getLedger().getEpoch();
					final List<Peer> candidatePeers = Network.this.context.getNetwork().getPeerHandler().get(new OutboundDiscoveryFilter(Network.this.context, shardGroupID), new PeerHandler.PeerDistanceComparator(Network.this.context.getNode().getIdentity()));
				    if (candidatePeers.isEmpty() == false) 
				    {
				        // For each slot, find the ideal peer based on its target position
				        for (final OutboundSlot slot : Network.this.outboundSlots.get(shardGroupID)) 
				        {
				        	// Still a pending switch over to a better connection pending
				        	if (slot.replacing != null)
				        		continue;
				        	
			        		// Check the connection in the current slot is alive
			        		final AbstractConnection currentConnection;
			        		if (slot.identity != null)
			        			currentConnection = Network.this.get(slot.identity, Protocol.TCP);
			        		else 
			        			currentConnection = null;
			        		
			        		if (currentConnection == null || currentConnection.getState() == ConnectionState.DISCONNECTED)
			        			slot.identity = null;
			        		else
			        		{
			                    // If connection is recent enough, keep it
			                    if (System.currentTimeMillis() < currentConnection.getShuffleAt())
			                        continue;

			                    // Dont rotate connections too fast!
			        			if (TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - this.lastRotation.getOrDefault(shardGroupID, 0l)) < Constants.DEFAULT_CONNECTION_ROTATION_INTERVAL_SECONDS)
			        				continue;
			                }

			        		// Find best peer in this range
				            Peer bestPeer = null;
				            UInt128 bestDistance = null;
				            for (final Peer candidate : candidatePeers)
				        	{
			        	    	// Have an existing inbound connection?
			        			final AbstractConnection candidateConnection = Network.this.get(candidate.getIdentity(), Protocol.TCP, ConnectionState.CONNECTING, ConnectionState.CONNECTED);
			        			if (candidateConnection != null && candidateConnection.getDirection() == Direction.INBOUND)
			        				continue;

			        			if (isIdentityInSlot(shardGroupID, candidate.getIdentity()))
			        				continue;

			        			final UInt128 distance = Network.this.context.getNode().getIdentity().distance(candidate.getIdentity());
			        			
			        			// For slot 0, find closest other validator with vote power regardless of range
			        		    if (slot.index == 0) 
			        		    {
			        		    	// TODO This is the only place the networking module references anything to do with the ledger.  Not sure I like it!
			        		    	final long votePower = Network.this.context.getLedger().getValidatorHandler().getVotePower(epoch, candidate.getIdentity());
			        		    	if (votePower == 0)
			        		    		continue;
			        		    	
			        		        if (bestPeer == null || distance.compareTo(bestDistance) < 0) 
									{
			        		            bestPeer = candidate;
			        		            bestDistance = distance;
			        		        }
			        		        continue;  // Skip normal range check for slot 0
			        		    }
			        		    
				        		// Check if peer is in this slot's range
				        	    if (distance.compareTo(slot.rangeStart) >= 0 && distance.compareTo(slot.rangeEnd) < 0) 
				        	    {
				        	    	if (bestPeer == null || distance.compareTo(bestDistance) < 0) 
				        	    	{
				        	    		bestPeer = candidate;
					        	        bestDistance = distance;
				        	        }
				        	    }
				        	    // Might be that all of the possible connections are in the same "slot"
				        	    // We'd like more if that is the case, but need to set the distance such
				        	    // that peers legitimately within the slot can override an inital selection
				        	    else if (slot.identity == null && distance.compareTo(slot.rangeEnd) < 0) 
				        	    {
				        	        if (bestPeer == null || distance.compareTo(bestDistance) < 0) 
				        	        {
				        	            bestPeer = candidate;
				        	            bestDistance = slot.rangeEnd;
				        	        }
				        	    }
				        	}
				            
				            if (bestPeer == null)
				            	continue;
				        	    
				            // Update the slot if needed
				            if (slot.identity == null || slot.identity.equals(bestPeer.getIdentity()) == false) 
				            {
				                slot.replacing = slot.identity;
				                slot.identity = bestPeer.getIdentity();
				                return bestPeer;
				            }
				        }
				    }
				    
				    return null;
				}
				
				private boolean isIdentityInSlot(final ShardGroupID shardGroupID, final Identity identity)
				{
					for (OutboundSlot slot : Network.this.outboundSlots.get(shardGroupID))
					{
						if (identity.equals(slot.identity))
							return true;
					}
					
					return false;
				}
				
				private void updateOutboundStatus() throws IOException
				{
					boolean performedDisconnect = false;
					final int numShardGroups = Network.this.context.getLedger().numShardGroups();
					final ShardGroupID localShardGroupID = ShardMapper.toShardGroup(Network.this.context.getNode().getIdentity(), numShardGroups);

					// Disconnect any peers that were bring replaced if the replacement is now connected
					List<OutboundSlot> outboundSlots = new ArrayList<OutboundSlot>(Network.this.outboundSlots.values());
					Collections.shuffle(outboundSlots);
				    for (OutboundSlot slot : outboundSlots) 
				    {
				        if (slot.replacing != null) 
				        {
			            	final ShardGroupID preferredPeerShardGroupID = ShardMapper.toShardGroup(slot.identity, numShardGroups);

				        	// Was the connection to the new peer successful?
				        	final AbstractConnection preferredConnection = Network.this.get(slot.identity, Protocol.TCP, ConnectionState.CONNECTED);
				        	if (preferredConnection == null)
				        	{
				        		// Connection being replaced has gone too ... meh
					            final AbstractConnection staleConnection = Network.this.get(slot.replacing, Protocol.TCP);
					            if (staleConnection == null)
					            {
					        		slot.identity = null;
					        		slot.replacing = null;
					            }
					            else
					            {
					            	staleConnection.resetStale();
					        		slot.identity = slot.replacing;
					        		slot.replacing = null;
					            }
					            
					            // Allow a new rotation to happen immediately
					            this.lastRotation.put(preferredPeerShardGroupID, 0l);
				        		continue;
				        	}
				        	
				        	if (preferredConnection.getNode().isSynced() == false)
				        		continue;
				        	
				            // Find and disconnect the specific stale connection we're replacing
				            final AbstractConnection staleConnection = Network.this.get(slot.replacing, Protocol.TCP);
				            if (staleConnection != null)
				            {
				            	// Do not break many connections to quickly, do one per iteration to be safe
				            	if (staleConnection.isStale() && performedDisconnect == false)
					            {
					            	final ShardGroupID connectionShardGroupID = ShardMapper.toShardGroup(staleConnection.getNode().getIdentity(), numShardGroups);

					            	final String disconnectMessage;
					            	if (connectionShardGroupID.equals(localShardGroupID))
					            		disconnectMessage = "Discovered better sync peer";
					            	else
					            		disconnectMessage = "Discovered better shard peer for shard "+connectionShardGroupID;
					            	
					            	staleConnection.disconnect(disconnectMessage);
					            	slot.replacing = null;
					            	
					            	performedDisconnect = true;
					            }
				            	else
				            		staleConnection.setStale();
				            }
				            else
				            	slot.replacing = null;
				        }
				    }	
				}
			});
    	}
    	catch (Exception ex)
    	{
    		throw new StartupException(ex, this.getClass());
    	}
	}

    @Override
	public void stop() throws TerminationException
	{
    	try
		{
   			this.houseKeepingFuture.cancel(false);
    		
			this.context.getEvents().unregister(this.peerListener);
    		
    		synchronized(this.connections)
			{
				for (AbstractConnection connection : this.connections)
					connection.disconnect("Stopping network");
			}

			if (this.TCPServerSocket != null)
				this.TCPListener.terminate(false);
			
			if (this.websocketService != null)
				this.websocketService.stop();
			
			this.gossipHandler.stop();
			this.messaging.stop();
			this.peerHandler.stop();
			
			// Let the disconnections settle
			Thread.sleep(TimeUnit.SECONDS.toMillis(1));
		}
		catch (Exception ex)
		{
			throw new TerminationException(ex, this.getClass());
		}
	}
    
    @SuppressWarnings("unchecked")
	public <T extends AbstractConnection> T connect(URI uri, Direction direction, Protocol protocol) throws IOException, CryptoException
    {
		this.connecting.lock();
		try
		{
			AbstractConnection connectedPeer = get(uri, protocol);
			if (connectedPeer == null)
			{
		    	if (protocol.toString().equalsIgnoreCase(Protocol.TCP.toString()))
		    	{
		    		if (direction.equals(Direction.OUTBOUND) == false)
		    			throw new IllegalArgumentException("Can only specify OUTBOUND for TCP connections");
		    		
					connectedPeer = new TCPConnection(this.context, uri);
		    	}
		    	else
		    		throw new IllegalArgumentException("Unknown protocol "+protocol);
			}

			return (T) connectedPeer;
		}
		finally
		{
			this.connecting.unlock();
		}
    }
    
    synchronized boolean accept(final AbstractConnection connection) throws IOException
	{
		Objects.requireNonNull(connection, "Connection is null");
		
		if (connection.getState().equals(ConnectionState.CONNECTING) == false)
		{
			connection.disconnect("Not in CONNECTING state");
			return false;
		}
		
		final Node node = connection.getNode();
		if (node == null)
		{
			connection.disconnect("Does not have a node object");
			return false;
		}
		
		if (node.getIdentity() == null)
		{
			connection.disconnect("Gave null identity");
			return false;
		}

		if (node.getAgentVersion() <= Agent.REFUSE_AGENT_VERSION)
		{
			connection.disconnect("Old agent "+node.getAgent()+":"+node.getProtocolVersion());
			return false;
		}

		if (node.getIdentity().equals(this.context.getNode().getIdentity()))
		{
			connection.ban("Message from self");
			return false;
		}
		
		final Peer knownPeerByIdentity = this.context.getNetwork().getPeerHandler().get(node.getIdentity());
		if (knownPeerByIdentity != null)
		{
			if (knownPeerByIdentity.getBannedUntil() > Time.getSystemTime())
			{
				connection.disconnect("Peer is banned at "+connection);
				return false;
			}
			
			if (knownPeerByIdentity.getAcceptAt() > Time.getSystemTime())
			{
				connection.disconnect("Not accepting connections from "+node.getIdentity()+" until "+knownPeerByIdentity.getAcceptAt()+" at "+connection.toString());
				return false;
			}
		}
		
		// Check for multiple or simultaneous inbound / outboung
		final List<AbstractConnection> duplicates = this.context.getNetwork().get(StandardConnectionFilter.build(this.context).setStates(ConnectionState.CONNECTING, ConnectionState.CONNECTED).
																														  			 setIdentity(node.getIdentity()).
																														  			 setProtocol(connection.getProtocol()));
		boolean keepThis = true;
		for (int i = 0 ; i < duplicates.size() ; i++)
		{
			final AbstractConnection duplicate = duplicates.get(i);
			if (duplicate == connection)
				continue;
			
			if (duplicate.getEphemeralRemotePublicKey() == null || 
				connection.getEphemeralLocalKeyPair().getPublicKey().compareTo(duplicate.getEphemeralRemotePublicKey()) < 0)
			{
				duplicate.disconnect("Already connected at endpoint "+connection);
			}
			else
			{
				connection.disconnect("Already connected at endpoint "+duplicate);
				keepThis = false;
			}
		}

		return keepThis;
	}

    
	public List<AbstractConnection> get(final ConnectionFilter<AbstractConnection> filter)
	{
		return get(filter, Integer.MAX_VALUE, true);
	}

	public List<AbstractConnection> get(final ConnectionFilter<AbstractConnection> filter, final int limit)
	{
		return get(filter, limit, true);
	}

	public List<AbstractConnection> get(final ConnectionFilter<AbstractConnection> filter, final boolean shuffle)
	{
		return get(filter, Integer.MAX_VALUE, shuffle);
	}

	public List<AbstractConnection> get(final ConnectionFilter<AbstractConnection> filter, final int limit, final boolean shuffle)
	{
		final List<AbstractConnection> connections;
		synchronized(this.connections)
		{
			final int count = count(filter);
			connections = new ArrayList<AbstractConnection>(count);
			for (int i = 0 ; i < this.connections.size() ; i++)
			{
				final AbstractConnection connection = this.connections.get(i);
				if (filter.filter(connection))
					connections.add(connection);
			}
		}

		if (shuffle)
			Collections.shuffle(connections);

		if (connections.size() <= limit)
			return connections;
		else
			return connections.subList(0, limit);
	}

	public int count(final ConnectionFilter<AbstractConnection> filter)
	{
		synchronized(this.connections)
		{
			int count = 0;

			for (int i = 0 ; i < this.connections.size() ; i++)
			{
				final AbstractConnection connection = this.connections.get(i);
				if (filter.filter(connection))
					count++;
			}
			
			return count;
		}
	}

	public int count(ConnectionState ... states)
	{
		synchronized(this.connections)
		{
			int count = 0;

			for (int i = 0 ; i < this.connections.size() ; i++)
			{
				final AbstractConnection connection = this.connections.get(i);
				if (states == null || states.length == 0 || Arrays.stream(states).collect(Collectors.toSet()).contains(connection.getState()))
					count++;
			}
			
			return count;
		}
	}

	public int count(Protocol protocol, ConnectionState ... states)
	{
		synchronized(this.connections)
		{
			int count = 0;

			for (int i = 0 ; i < this.connections.size() ; i++)
			{
				final AbstractConnection connection = this.connections.get(i);
				if (connection.getProtocol().equals(protocol) &&
					(states == null || states.length == 0 || Arrays.stream(states).collect(Collectors.toSet()).contains(connection.getState())))
					count++;
			}
			
			return count;
		}
	}

	@SuppressWarnings("unchecked")
	public <T extends AbstractConnection> T get(URI host, Protocol protocol, ConnectionState ... states)
	{
		if (host.getPort() == -1)
			return getHostOnly(host, protocol, states);
		else
			return getHostWithPort(host, protocol, states);
	}

	private <T extends AbstractConnection> T getHostWithPort(URI host, Protocol protocol, ConnectionState ... states)
	{
		synchronized(this.connections)
		{
			for (int i = 0 ; i < this.connections.size() ; i++)
			{
				final AbstractConnection connection = this.connections.get(i);
				if (connection.getProtocol().equals(protocol) &&
					connection.getHost().equals(URIs.toHostAndPort(host)) &&
					(states == null || states.length == 0 || Arrays.stream(states).collect(Collectors.toSet()).contains(connection.getState())))
					return (T) connection;
			}

			return null;
		}
	}

	private <T extends AbstractConnection> T getHostOnly(URI host, Protocol protocol, ConnectionState ... states)
	{
		synchronized(this.connections)
		{
			for (int i = 0 ; i < this.connections.size() ; i++)
			{
				final AbstractConnection connection = this.connections.get(i);
				if (connection.getProtocol().equals(protocol) &&
					connection.getHost().getHost().equals(host.getHost()) &&
					(states == null || states.length == 0 || Arrays.stream(states).collect(Collectors.toSet()).contains(connection.getState())))
					return (T) connection;
			}

			return null;
		}
	}

	@SuppressWarnings("unchecked")
	public <T extends AbstractConnection> T get(Identity identity, Protocol protocol, ConnectionState ... states)
	{
		synchronized(this.connections)
		{
			for (int i = 0 ; i < this.connections.size() ; i++)
			{
				final AbstractConnection connection = this.connections.get(i);
				if (connection.getProtocol().equals(protocol) &&
					connection.getNode() != null && connection.getNode().getIdentity().getIdentity().equals(identity) &&
					(states == null || states.length == 0 || Arrays.stream(states).collect(Collectors.toSet()).contains(connection.getState())))
					return (T) connection;
			}
		}

		return null;
	}

	public boolean has(Identity identity, ConnectionState ... states)
	{
		synchronized(this.connections)
		{
			for (int i = 0 ; i < this.connections.size() ; i++)
			{
				final AbstractConnection connection = this.connections.get(i);
				if (connection.getNode() != null && connection.getNode().getIdentity().getIdentity().equals(identity) &&
					(states == null || states.length == 0 || Arrays.stream(states).collect(Collectors.toSet()).contains(connection.getState())))
					return true;
			}

			return false;
		}
	}

	public boolean has(Identity identity, Protocol protocol, ConnectionState ... states)
	{
		synchronized(this.connections)
		{
			for (int i = 0 ; i < this.connections.size() ; i++)
			{
				final AbstractConnection connection = this.connections.get(i);
				if (connection.getProtocol().equals(protocol) &&
					connection.getNode() != null && connection.getNode().getIdentity().getIdentity().equals(identity) &&
					(states == null || states.length == 0 || Arrays.stream(states).collect(Collectors.toSet()).contains(connection.getState())))
					return true;
			}

			return false;
		}
	}
	
	// PEER LISTENER //
    private SynchronousEventListener peerListener = new SynchronousEventListener()
    {
    	@Subscribe
		public void on(ConnectingEvent event)
		{
			synchronized(Network.this.connections)
			{
				Network.this.connections.add(event.getConnection());
				
				if (networkLog.hasLevel(Logging.DEBUG))
					networkLog.debug(Network.this.context.getName()+": CONNECTING/CONNECTED peer count is now "+Network.this.connections.size());
			}
		}

    	@Subscribe
		public void on(final ConnectedEvent event) throws IOException
		{
			event.getConnection().setConnectedAt(Time.getSystemTime());
		}

    	@Subscribe
		public void on(DisconnectedEvent event)
		{
			synchronized(Network.this.connections)
			{
				AbstractConnection removed = null;
				// Want to check on reference not equality as we can have multiple instances
				// that may satisfy the equality check that we want to keep track of.
				Iterator<AbstractConnection> connectionsIterator = Network.this.connections.iterator();
				while (connectionsIterator.hasNext())
				{
					AbstractConnection connection = connectionsIterator.next();
					if (connection == event.getConnection())
					{
						connectionsIterator.remove();
						removed = connection;
						networkLog.info(Network.this.context.getName()+": CONNECTING/CONNECTED peer count is now "+Network.this.connections.size());
						break;
					}
				}
				
				if (removed == null)
					networkLog.warn(Network.this.context.getName()+": Disconnecting peer not found "+event.getConnection());
			}
		}
    };
}
