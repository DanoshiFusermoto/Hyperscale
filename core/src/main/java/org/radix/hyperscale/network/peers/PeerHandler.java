package org.radix.hyperscale.network.peers;

import java.io.IOException;
import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.NoRouteToHostException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.radix.hyperscale.Configuration;
import org.radix.hyperscale.Constants;
import org.radix.hyperscale.Context;
import org.radix.hyperscale.Service;
import org.radix.hyperscale.Universe;
import org.radix.hyperscale.WebService;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.crypto.Identity;
import org.radix.hyperscale.events.EventListener;
import org.radix.hyperscale.exceptions.ServiceException;
import org.radix.hyperscale.exceptions.StartupException;
import org.radix.hyperscale.exceptions.TerminationException;
import org.radix.hyperscale.executors.Executor;
import org.radix.hyperscale.executors.ScheduledExecutable;
import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;
import org.radix.hyperscale.network.AbstractConnection;
import org.radix.hyperscale.network.ConnectionState;
import org.radix.hyperscale.network.MessageProcessor;
import org.radix.hyperscale.network.Protocol;
import org.radix.hyperscale.network.StandardConnectionFilter;
import org.radix.hyperscale.network.discovery.AllPeersFilter;
import org.radix.hyperscale.network.discovery.BootstrapService;
import org.radix.hyperscale.network.discovery.MultiPeerFilter;
import org.radix.hyperscale.network.discovery.NotLocalPeersFilter;
import org.radix.hyperscale.network.discovery.PeerFilter;
import org.radix.hyperscale.network.discovery.StandardDiscoveryFilter;
import org.radix.hyperscale.network.events.ConnectedEvent;
import org.radix.hyperscale.network.events.DisconnectedEvent;
import org.radix.hyperscale.network.messages.GetPeersMessage;
import org.radix.hyperscale.network.messages.PeerPingMessage;
import org.radix.hyperscale.network.messages.PeerPongMessage;
import org.radix.hyperscale.network.messages.PeersMessage;
import org.radix.hyperscale.node.Node;
import org.radix.hyperscale.node.Services;
import org.radix.hyperscale.time.Time;
import org.radix.hyperscale.utils.UInt128;

import com.google.common.eventbus.Subscribe;

public class PeerHandler implements Service
{
	private static final Logger networklog = Logging.getLogger("network");

	public static final class PeerDistanceComparator extends IdentityDistanceComparator<Peer>
	{
		public PeerDistanceComparator(final Identity origin)
		{
			super(origin);
		}

		@Override
		public int compare(final Peer p1, final Peer p2)
		{
			return compareXorDistances(p2.getIdentity(), p1.getIdentity());
		}
	}

	public static final class ConnectionDistanceComparator extends IdentityDistanceComparator<AbstractConnection>
	{
		public ConnectionDistanceComparator(final Identity origin)
		{
			super(origin);
		}

		@Override
		public int compare(final AbstractConnection c1, final AbstractConnection c2)
		{
			return compareXorDistances(c2.getNode().getIdentity(), c1.getNode().getIdentity());
		}
	}
	
	private abstract static class IdentityDistanceComparator<T> implements Comparator<T>
	{
		private final Identity origin;

		IdentityDistanceComparator(final Identity origin)
		{
			this.origin = Objects.requireNonNull(origin, "Origin is null");
		}

		public int compareXorDistances(final Identity id1, final Identity id2) 
		{
			Hash dh1 = Hash.hash(this.origin.getHash().toByteArray(), id1.getHash().toByteArray());
			UInt128 d1 = UInt128.from(dh1.toByteArray()).xor(UInt128.from(id1.getHash().toByteArray()));
			Hash dh2 = Hash.hash(this.origin.getHash().toByteArray(), id2.getHash().toByteArray());
			UInt128 d2 = UInt128.from(dh2.toByteArray()).xor(UInt128.from(id2.getHash().toByteArray()));

			return d1.compareTo(d2);
		}
	}

	private final Context	context;
	private final PeerStore peerStore;
	private final BootstrapService bootstrapper;
	private final Map<AbstractConnection, Long> pings = Collections.synchronizedMap(new HashMap<AbstractConnection, Long>());
	
	private Future<?> houseKeepingTaskFuture = null;

	public PeerHandler(final Context context)
	{
		super();
		
		this.context = Objects.requireNonNull(context, "Context is null");
    	this.peerStore = new PeerStore(context);
    	
    	if (Boolean.getBoolean("singleton") == false || Universe.get().shardGroupCount() > 1)
			this.bootstrapper = new BootstrapService(this.context);
		else
			this.bootstrapper = null;
	}

	@Override
	public void start() throws StartupException
	{
		try
		{
			this.context.getNetwork().getMessaging().register(PeersMessage.class, this.getClass(), new MessageProcessor<PeersMessage> ()
			{
				@Override
				public void process (final PeersMessage peersMessage, final AbstractConnection connection)
				{
					for (final URI uri : peersMessage.getPeers())
					{
						if (uri.getUserInfo() == null)
						{
							networklog.warn(PeerHandler.this.context.getName()+": Received peer does not have an identity");
							// Disconnect and ban
							continue;
						}
						
						try
						{
							Identity identity = Identity.from(uri.getUserInfo());
							if (PeerHandler.this.get(identity) != null)
								continue;

							final Peer peer = new Peer(uri);
							PeerHandler.this.update(peer);
						}
						catch(IOException ioex)
						{
							networklog.error(PeerHandler.this.context.getName()+": Failed to update peer "+uri, ioex);
						}
					}
				}
			});
	
			this.context.getNetwork().getMessaging().register(GetPeersMessage.class, this.getClass(), new MessageProcessor<GetPeersMessage> ()
			{
				@Override
				public void process(final GetPeersMessage getPeersMessage, final AbstractConnection connection)
				{
					try
					{
						// Deliver known Peers in its entirety, filtered on whitelist and activity
						// Chunk the sending of Peers so that UDP can handle it
						// TODO make this better!
						final NotLocalPeersFilter notLocalPeersFilter = new NotLocalPeersFilter(PeerHandler.this.context.getNode());
						final StandardDiscoveryFilter standardDiscoveryFilter = new StandardDiscoveryFilter(PeerHandler.this.context);
						final List<Peer> peersList = PeerHandler.this.getPeerStore().get(new MultiPeerFilter(notLocalPeersFilter, standardDiscoveryFilter));
						
						List<Peer> peersSublist = new ArrayList<Peer>();
						for (final Peer peer : peersList)
						{
							peersSublist.add(peer);
	
							if (peersSublist.size() == 64)
							{
								PeerHandler.this.context.getNetwork().getMessaging().send(new PeersMessage(peersSublist), connection);
								peersSublist = new ArrayList<Peer>();
							}
						}
	
						if (peersSublist.isEmpty() == false)
							PeerHandler.this.context.getNetwork().getMessaging().send(new PeersMessage(peersSublist), connection);
					}
					catch (Exception ex)
					{
						networklog.error(PeerHandler.this.context.getName()+": peers.get "+connection, ex);
					}
				}
			});
	
			this.context.getNetwork().getMessaging().register(PeerPingMessage.class, this.getClass(), new MessageProcessor<PeerPingMessage> ()
			{
				@Override
				public void process (final PeerPingMessage message, final AbstractConnection connection)
				{
					try
					{
						networklog.debug("peer.ping from "+connection+" with nonce "+message.getNonce());
						PeerHandler.this.context.getNetwork().getMessaging().send(new PeerPongMessage(message.getNonce()), connection);
					}
					catch (Exception ex)
					{
						networklog.error(PeerHandler.this.context.getName()+": peer.ping "+connection, ex);
					}
				}
			});
	
			this.context.getNetwork().getMessaging().register(PeerPongMessage.class, this.getClass(), new MessageProcessor<PeerPongMessage> ()
			{
				@Override
				public void process (final PeerPongMessage message, final AbstractConnection connection)
				{
					try
					{
						final Long nonce = PeerHandler.this.pings.remove(connection);
						if (nonce != message.getNonce())
							connection.disconnect("Got peer.pong with unexpected nonce "+message.getNonce());
						else if (networklog.hasLevel(Logging.DEBUG))
							networklog.debug("Got peer.pong from "+connection+" with nonce "+message.getNonce());
					}
					catch (Exception ex)
					{
						networklog.error(PeerHandler.this.context.getName()+": peer.pong "+connection, ex);
					}
				}
			});
			
	        // PEERS HOUSEKEEPING //
			long houseKeepingInterval = this.context.getConfiguration().get("network.peers.broadcast.interval", 60);
			this.houseKeepingTaskFuture = Executor.getInstance().scheduleWithFixedDelay(new ScheduledExecutable(houseKeepingInterval, houseKeepingInterval, TimeUnit.SECONDS)
			{
				@Override
				public void execute()
				{
 					try
					{
						// Clean out aged peers with no activity
						for (final Peer peer : PeerHandler.this.getPeerStore().get(new AllPeersFilter())) 
						{
							if (peer.getIdentity() != null && PeerHandler.this.context.getNetwork().has(peer.getIdentity(), ConnectionState.CONNECTING, ConnectionState.CONNECTED))
								continue;
							
							// Is aged?
							if (TimeUnit.MILLISECONDS.toSeconds(Time.getSystemTime() - peer.getKnownAt()) >= PeerHandler.this.context.getConfiguration().get("network.peers.aged", 3600))
							{
								if (peer.getAttempts() == 10 || 
									TimeUnit.MILLISECONDS.toSeconds(Time.getSystemTime() - peer.getConnectedAt()) >= PeerHandler.this.context.getConfiguration().get("network.peers.aged", 3600) ||
									TimeUnit.MILLISECONDS.toSeconds(Time.getSystemTime() - peer.getDisconnectedAt()) >= PeerHandler.this.context.getConfiguration().get("network.peers.aged", 3600))
								{
									if (peer.getIdentity() != null)
										PeerHandler.this.getPeerStore().delete(peer.getIdentity());
									else
										PeerHandler.this.getPeerStore().delete(peer.getURI());
								}
							}
						}
						
						PeerHandler.this.getPeerStore().flush();
	
						// Ping / pongs //
						for (final AbstractConnection connection : PeerHandler.this.context.getNetwork().get(StandardConnectionFilter.build(PeerHandler.this.context).setStates(ConnectionState.CONNECTED)))
						{
							synchronized(PeerHandler.this.pings)
							{
								Long nonce = PeerHandler.this.pings.remove(connection);
								if (nonce != null)
								{
									connection.disconnect("Did not respond to ping "+nonce);
									continue;
								}
								
								PeerPingMessage ping = new PeerPingMessage(ThreadLocalRandom.current().nextLong());
								PeerHandler.this.pings.put(connection, ping.getNonce());

								if (networklog.hasLevel(Logging.DEBUG))
									networklog.debug("Pinging "+connection+" with nonce "+ping.getNonce());

								PeerHandler.this.context.getNetwork().getMessaging().send(ping, connection);
							}		
						}

						// Peer refresh
						// Seems single connection refresh every 60s is sufficient on networks up to about 5k nodes
						final StandardConnectionFilter connectionFilter = StandardConnectionFilter.build(PeerHandler.this.context).setStates(ConnectionState.CONNECTED);
						final List<AbstractConnection> connections = PeerHandler.this.context.getNetwork().get(connectionFilter, true);
						if (connections.isEmpty() == false)
							PeerHandler.this.context.getNetwork().getMessaging().send(new GetPeersMessage(), connections.getFirst());
					}
					catch (Throwable t)
					{
						networklog.error("Peers update failed", t);
					}
				}
			});
	
			// Register listeners
			this.context.getEvents().register(this.peerListener);

			// Start services
			this.peerStore.start();
			if (this.bootstrapper != null)
			{
				Thread bootstrapperThread = new Thread(this.bootstrapper, this.context.getName()+" - Bootstrapper");
				bootstrapperThread.setDaemon(true);
				bootstrapperThread.start();
			}
		}
		catch (Exception ex)
		{
			throw new StartupException(ex);
		}
	}

	@Override
	public void stop() throws TerminationException
	{
		if (this.houseKeepingTaskFuture != null)
			this.houseKeepingTaskFuture.cancel(false);

		// Unregister listeners
		this.context.getEvents().unregister(this.peerListener);
		
		// Stpp services
		this.peerStore.stop();
	}
	
	@Override
	public void clean() throws ServiceException
	{
		try
		{
			this.peerStore.clean();
		}
		catch (Exception ex)
		{
			throw new ServiceException(ex, getClass());
		}
	}
	
	private PeerStore getPeerStore()
	{
		return this.peerStore;
	}

	public Peer get(final Identity identity)
	{
		Objects.requireNonNull(identity, "Identity is null");
		
		return getPeerStore().get(identity);
	}

	public Peer get(final URI uri)
	{
		Objects.requireNonNull(uri, "URI is null");
		
		return getPeerStore().get(uri);
	}

	public List<Peer> get(final PeerFilter filter) throws IOException
	{
		return get(filter, 0, Integer.MAX_VALUE, null);
	}

	public List<Peer> get(final PeerFilter filter, final int limit) throws IOException
	{
		return get(filter, 0, limit, null);
	}

	public List<Peer> get(final PeerFilter filter, final Comparator<Peer> sorter) throws IOException
	{
		return get(filter, 0, Integer.MAX_VALUE, sorter);
	}

	public List<Peer> get(final PeerFilter filter, final int limit, final Comparator<Peer> sorter) throws IOException
	{
		return get(filter, 0, limit, sorter);
	}

	public List<Peer> get(final PeerFilter filter, final int offset, final int limit) throws IOException
	{
		return get(filter, 0, Integer.MAX_VALUE, null);
	}
	
	public List<Peer> get(final PeerFilter filter, final int offset, final int limit, final Comparator<Peer> sorter) throws IOException
	{
		Objects.requireNonNull(filter, "Peer filter is null");
		
		final List<Peer> peers = getPeerStore().get(filter, offset, limit);
		if (sorter != null)
			peers.sort(sorter);

		return Collections.unmodifiableList(peers);
	}

	public List<Peer> get(final Collection<Identity> identities) throws IOException
	{
		return get(identities, null, null);
	}

	public List<Peer> get(final Collection<Identity> identities, final PeerFilter filter) throws IOException
	{
		return get(identities, filter, null);
	}

	public List<Peer> get(final Collection<Identity> identities, final PeerFilter filter, final Comparator<Peer> sorter) throws IOException
	{
		Objects.requireNonNull(identities, "Identities is null");
		
		List<Peer> peers = new ArrayList<Peer>();
		for (final Identity identity : identities)
		{
			final Peer peer = getPeerStore().get(identity);
			if (peer == null)
				continue;
			
			if (filter != null && filter.filter(peer) == false)
				continue;
			
			peers.add(peer);
		}

		if (sorter != null)
			peers.sort(sorter);

		return Collections.unmodifiableList(peers);
	}
	
	/**
	 * Checks if the node is accessible by probing the bootstrap endpoint. 
	 * If it is, then it is likely other ports are correctly routed and can be set as a GATEWAY.
	 * @param nodeURI The URI of the node to check
	 * @param node The node object containing meta data regarding ports and services
	 * @return true if the bootstrap endpoint is accessible, false otherwise
	 */
	public void checkAccessible(final URI nodeURI, final Node node) 
	{
	    try 
	    {
	        // Create URL for checking endpoint accessibility
	        URL url = new URL(WebService.DEFAULT_SCHEME + nodeURI.getHost() + ":" + node.getAPIPort() +
	                         Configuration.getDefault().get("api.url", WebService.DEFAULT_API_PATH) + "/ledger/head");
	        
	        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
	        conn.setRequestMethod("GET");
	        conn.setConnectTimeout(1000);
	        conn.setReadTimeout(1000);
	        
	        int responseCode = conn.getResponseCode();
	        if (responseCode >= 200 && responseCode < 300)
	        {
	        	// Create URI with gateway service
	            Set<Services> services = new HashSet<>(node.getServices());
	            services.add(Services.GATEWAY);
	            
	            // Build new URI with gateway service
	            String servicesQuery = services.stream().map(s -> s.name()).collect(Collectors.joining("&"));
	            
	            URI gatewayURI = new URI(nodeURI.getScheme(), 
	                                	 nodeURI.getUserInfo(),
	                                	 nodeURI.getHost(),
	                                	 nodeURI.getPort(),
	                                	 nodeURI.getPath(),
	                                	 servicesQuery,
	                                	 nodeURI.getFragment());
	            
	            final Peer gatewayPeer = new Peer(gatewayURI, Protocol.TCP);
	            
	            // If peer is not already known, store it as a confirmed gateway
	            if (context.getNetwork().getPeerHandler().get(gatewayPeer.getIdentity()) == null)
	            	context.getNetwork().getPeerHandler().update(gatewayPeer);

	            return;
	        }
	        
	    	if (networklog.hasLevel(Logging.DEBUG))
	    		networklog.error("Gateway check failure, returned response "+responseCode+" code "+nodeURI);
	    } 
	    catch (SocketTimeoutException | ConnectException | NoRouteToHostException nex)
	    {
	    	if (networklog.hasLevel(Logging.DEBUG))
		    	networklog.error("Gateway check timeout, ports likely not open, not a gateway "+nodeURI);
	    }
	    catch (Exception e) 
	    {
	    	networklog.error("Failed to complete gateway check "+nodeURI, e);
	    }

	    return;
	}
	
	public void update(final Peer peer) throws IOException
	{
		if (peer.hasService(Services.GATEWAY) == false)
		{
			networklog.warn(this.context.getName()+": Attempted to update none GATEWAY peer: "+peer);
			return;
		}
		
		if (peer.getIdentity() == null)
			throw new IllegalStateException("Peer does not have an idenity: "+peer);
		
		boolean isInvalid = false;
		final Peer knownPeerByIdentity = getPeerStore().get(peer.getIdentity());
		if (knownPeerByIdentity != null)
		{
			// Check for host switch for identities.  If within an hour of the last known IP for the identity, disconnect and ban. 
			if (knownPeerByIdentity.getIdentity().equals(peer.getIdentity()) && knownPeerByIdentity.getHost().equals(peer.getHost()) == false)
			{
				long lastKnownOrAttemptTimestamp = Math.max(knownPeerByIdentity.getKnownAt(), knownPeerByIdentity.getAttemptedAt());
				if (TimeUnit.MILLISECONDS.toHours(lastKnownOrAttemptTimestamp - Time.getSystemTime()) < 1)
					isInvalid = true;
				else
					getPeerStore().delete(knownPeerByIdentity.getIdentity());
			}
		}
		
		final Peer knownPeerByHost = getPeerStore().get(peer.getURI());
		if (knownPeerByHost != null)
		{
			// Check for host duplicates but different identities.  If within an hour of the last known IP for the identity, disconnect and ban. 
			if (knownPeerByHost.getIdentity().equals(peer.getIdentity()) == false)
			{
				long lastKnownOrAttemptTimestamp = Math.max(knownPeerByHost.getKnownAt(), knownPeerByHost.getAttemptedAt());
				if (TimeUnit.MILLISECONDS.toHours(lastKnownOrAttemptTimestamp - Time.getSystemTime()) < 1)
					isInvalid = true;
				else
					getPeerStore().delete(knownPeerByHost.getIdentity());
			}
		}
		
		if (isInvalid)
		{
			if (knownPeerByIdentity != null && knownPeerByIdentity.isInvalid() == false)
			{
				knownPeerByIdentity.setInvalid(isInvalid);
				getPeerStore().store(knownPeerByIdentity);

				networklog.warn(PeerHandler.this.context.getName()+": Updated "+knownPeerByIdentity.getHost()+" for "+knownPeerByIdentity.getIdentity()+" as invalid");
			}

			if (knownPeerByHost != null && knownPeerByHost.isInvalid() == false)
			{
				knownPeerByHost.setInvalid(isInvalid);
				getPeerStore().store(knownPeerByHost);

				networklog.warn(PeerHandler.this.context.getName()+": Updated "+knownPeerByIdentity.getHost()+" for "+knownPeerByIdentity.getIdentity()+" as invalid");
			}
		}
		else
		{
			getPeerStore().store(peer);

			if (networklog.hasLevel(Logging.DEBUG))
				networklog.debug(PeerHandler.this.context.getName()+": Updated "+peer.getHost()+" for "+peer.getIdentity()+" Attempts: "+peer.getAttempts()+" Attempted At: "+peer.getAttemptedAt()+" Next Attempt: "+peer.getAttemptAt());
		}
	}

	// PEER LISTENER //
	private EventListener peerListener = new EventListener()
	{
    	@Subscribe
		public void on(final ConnectedEvent event)
		{
			try
			{
				Peer peer = PeerHandler.this.get(event.getConnection().getNode().getIdentity());
				
				// New / unknown peer
				final boolean existingPeer = peer != null;
				if (existingPeer == false)
				{
					peer = Peer.from(event.getConnection());
				}
				// Existing peer to update
				else
				{
					peer.addProtocol(event.getConnection().getProtocol());
					peer.setConnectedAt(event.getConnection().getConnectedAt());
					peer.setConnectingAt(event.getConnection().getConnectingAt());
				}
				
				if (existingPeer == false)
				{
					final URI peerURI = peer.getURI();
					Executor.getInstance().schedule(() -> PeerHandler.this.checkAccessible(peerURI, event.getConnection().getNode()), 0, TimeUnit.SECONDS);
				}
				else
					PeerHandler.this.update(peer);
				
				if (event.getConnection().getProtocol().equals(Protocol.TCP)) // TODO what if the UDP connection is the ONLY connection we have to the node?
					PeerHandler.this.context.getNetwork().getMessaging().send(new GetPeersMessage(), event.getConnection());
			}
			catch (IOException ioex)
			{
				networklog.debug("Failed to request known peer information from "+event.getConnection(), ioex);
			}
		}

    	@Subscribe
		public void on(final DisconnectedEvent event)
		{
			PeerHandler.this.pings.remove(event.getConnection());
			
			try
    		{
				Peer peer = event.getConnection().getNode() != null ? PeerHandler.this.get(event.getConnection().getNode().getIdentity()) : 
																	  PeerHandler.this.get(event.getConnection().getHost());

				// New / unknown peer
				if (peer == null)
				{
					peer = Peer.from(event.getConnection());
				}
				// Existing peer to update
				else
				{
					peer.setAttemptedAt(Time.getSystemTime());
					peer.setDisconnectedAt(event.getConnection().getDisconnectedAt());
					peer.setLatency(event.getConnection().getLatency());

					if (event.getConnection().getBannedUntil() > 0)
						peer.setBanned(event.getConnection().getBanReason(), event.getConnection().getBannedUntil());
				
					// Update attempts if didn't connect, was a short lived connection
					if (event.getConnection().getConnectedAt() == 0 || 
						event.getConnection().getDisconnectedAt() - event.getConnection().getConnectedAt() < TimeUnit.SECONDS.toMillis(Constants.DEFAULT_MINIMUM_CONNECTION_DURATION))
					{
						peer.setAttempts(peer.getAttempts()+1);
						peer.setAttemptAt(Time.getSystemTime() + (TimeUnit.SECONDS.toMillis(PeerHandler.this.context.getConfiguration().get("network.peer.reattempt", 60)) * (peer.getAttempts()*peer.getAttempts())));
						peer.setAcceptAt(Time.getSystemTime() + TimeUnit.SECONDS.toMillis(PeerHandler.this.context.getConfiguration().get("network.peer.reattempt", 60)));
					}
					else
					{
						peer.setAttempts(0);
						
						// Disconnected with an exception so set an accept / attempt delay
						if (event.getException() != null)
						{
							peer.setAttemptAt(Time.getSystemTime() + (TimeUnit.SECONDS.toMillis(PeerHandler.this.context.getConfiguration().get("network.peer.reattempt", 60))));
							peer.setAcceptAt(Time.getSystemTime() + TimeUnit.SECONDS.toMillis(PeerHandler.this.context.getConfiguration().get("network.peer.reattempt", 60)));
						}
						else
						{
							peer.setAttemptAt(0);
							peer.setAcceptAt(0);
						}
					}
				}
				
				PeerHandler.this.update(peer);
			}
			catch (IOException ioex)
			{
				networklog.debug("Failed to store peer information for "+event.getConnection(), ioex);
			}
		}
	};
}
