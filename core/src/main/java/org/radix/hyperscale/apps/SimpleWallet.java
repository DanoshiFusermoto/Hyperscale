package org.radix.hyperscale.apps;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.json.JSONObject;
import org.radix.hyperscale.Context;
import org.radix.hyperscale.common.Order;
import org.radix.hyperscale.crypto.CryptoException;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.crypto.Identity;
import org.radix.hyperscale.crypto.KeyPair;
import org.radix.hyperscale.crypto.ed25519.EDKeyPair;
import org.radix.hyperscale.exceptions.ValidationException;
import org.radix.hyperscale.executors.Executable;
import org.radix.hyperscale.executors.Executor;
import org.radix.hyperscale.ledger.AssociationSearchQuery;
import org.radix.hyperscale.ledger.AssociationSearchResponse;
import org.radix.hyperscale.ledger.AtomFuture;
import org.radix.hyperscale.ledger.ShardGroupID;
import org.radix.hyperscale.ledger.ShardMapper;
import org.radix.hyperscale.ledger.StateAddress;
import org.radix.hyperscale.ledger.StateContext;
import org.radix.hyperscale.ledger.Substate;
import org.radix.hyperscale.ledger.SubstateCommit;
import org.radix.hyperscale.ledger.SubstateOutput;
import org.radix.hyperscale.ledger.SubstateSearchQuery;
import org.radix.hyperscale.ledger.SubstateSearchResponse;
import org.radix.hyperscale.ledger.exceptions.InsufficientBalanceException;
import org.radix.hyperscale.ledger.primitives.Atom;
import org.radix.hyperscale.ledger.primitives.AtomCertificate;
import org.radix.hyperscale.ledger.primitives.StateCertificate;
import org.radix.hyperscale.ledger.sme.SubstateTransitions;
import org.radix.hyperscale.ledger.sme.Vault;
import org.radix.hyperscale.ledger.sme.components.TokenComponent;
import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;
import org.radix.hyperscale.network.AbstractConnection;
import org.radix.hyperscale.network.ConnectionState;
import org.radix.hyperscale.network.Protocol;
import org.radix.hyperscale.network.StandardConnectionFilter;
import org.radix.hyperscale.serialization.Serialization;
import org.radix.hyperscale.serialization.SerializationException;
import org.radix.hyperscale.serialization.DsonOutput.Output;
import org.radix.hyperscale.serialization.mapper.JacksonCodecConstants;
import org.radix.hyperscale.utils.UInt256;

/** A simple wallet for interacting with the network.  Not very efficient and not very good, but adequate for most uses / testing.
 */
public class SimpleWallet implements AutoCloseable
{
	private static final Logger apiLog = Logging.getLogger("api");
	private static final Logger walletLog = Logging.getLogger("wallet");
	
	private final Context context;
	private final EDKeyPair key;
	private final Map<Hash, AtomFuture> futures = Collections.synchronizedMap(new HashMap<Hash, AtomFuture>()); 
	private final Map<URI, WebSocketClient> websockets = new HashMap<URI, WebSocketClient>();
	private final Map<StateAddress, Substate> substates = Collections.synchronizedMap(new LinkedHashMap<StateAddress, Substate>());
	
	private volatile boolean closed = false;
	
	private ReentrantLock lock = new ReentrantLock(true);
	
	public SimpleWallet(final Context context, final EDKeyPair key) throws InterruptedException, ExecutionException, URISyntaxException
	{
		this.context = Objects.requireNonNull(context);
		this.key = Objects.requireNonNull(key);
		
		StateAddress vaultStateAddress = StateAddress.from(Vault.class.getAnnotation(StateContext.class).value(), this.key.getIdentity());
		Future<SubstateSearchResponse> vaultSubstateSearchResponseFuture = context.getLedger().get(new SubstateSearchQuery(vaultStateAddress));
		SubstateSearchResponse vaultSubstateSearchResponse = vaultSubstateSearchResponseFuture.get();
		if (vaultSubstateSearchResponse.getResult() != null)
			this.substates.put(vaultSubstateSearchResponse.getResult().getSubstate().getAddress(), vaultSubstateSearchResponse.getResult().getSubstate());
		
		long associationSearchOffset = 0;
		AssociationSearchQuery associationsearchQuery = new AssociationSearchQuery(key.getIdentity().getHash(), "", Order.ASCENDING, 25);
		Future<AssociationSearchResponse> associationSearchResponseFuture;
		
		while((associationSearchResponseFuture = this.context.getLedger().get(associationsearchQuery)).get().isEmpty() == false)
		{
			for (SubstateCommit associationSubstateCommit : associationSearchResponseFuture.get().getResults())
			{
				this.substates.compute(associationSubstateCommit.getSubstate().getAddress(), (sa, st) -> {
					if (st == null)
						return associationSubstateCommit.getSubstate(); // TODO check for completeness & correctness
					
					return st.merge(associationSubstateCommit.getSubstate());
				});
			}
			
			associationSearchOffset = associationSearchResponseFuture.get().getNextOffset();
			if (associationSearchOffset == -1)
				break;
			
			associationsearchQuery = new AssociationSearchQuery(key.getPublicKey().getHash(), "", Order.ASCENDING, associationSearchOffset, 25);
		}
		
		openWebSockets();
	}
	
	public void close()
	{
		if (this.closed == true)
			return;
		
		this.lock.lock();
		try
		{
			this.closed = true;
			this.websockets.forEach((p, s) -> s.close());
			this.websockets.clear();
			this.substates.clear();
		}
		finally
		{
			this.lock.unlock();
		}
	}
	
	private void openWebSockets() throws URISyntaxException, InterruptedException
	{
		final int numShardGroups = this.context.getLedger().numShardGroups();
		ShardGroupID localShardGroupID = ShardMapper.toShardGroup(this.context.getNode().getIdentity(), numShardGroups);
		
		// Set up basic websocket clients
		for (int shardGroupID = 0 ; shardGroupID < numShardGroups ; shardGroupID++)
		{
			if (shardGroupID == localShardGroupID.longValue())
			{
				openWebsocket(new URI("ws://"+this.context.getNode().getIdentity().toString()+"@127.0.0.1:"+this.context.getNode().getWebsocketPort()+"#"+shardGroupID));
				continue;
			}
			
			List<AbstractConnection> shardConnected = this.context.getNetwork().get(StandardConnectionFilter.build(this.context).setProtocol(Protocol.TCP).setStates(ConnectionState.CONNECTED).setShardGroupID(ShardGroupID.from(shardGroupID)));
			for (AbstractConnection connection : shardConnected)
			{
				if (openWebsocket(new URI("ws://"+connection.getURI().getUserInfo()+"@"+connection.getURI().getHost()+":"+connection.getNode().getWebsocketPort()+"#"+shardGroupID)) != null)
					break;
			}
		}
	}
	
	private WebSocketClient openWebSocket(ShardGroupID shardGroupID) throws URISyntaxException, InterruptedException
	{
		List<AbstractConnection> shardConnected = this.context.getNetwork().get(StandardConnectionFilter.build(this.context).setProtocol(Protocol.TCP).setStates(ConnectionState.CONNECTED).setShardGroupID(shardGroupID));
		for (AbstractConnection connection : shardConnected)
		{
			WebSocketClient webSocketClient = openWebsocket(new URI("ws://"+connection.getURI().getUserInfo()+"@"+connection.getURI().getHost()+":"+connection.getNode().getWebsocketPort()+"#"+shardGroupID));
			if (webSocketClient != null)
				return webSocketClient;
		}
		
		return null;
	}

	private WebSocketClient openWebsocket(final URI URI) throws InterruptedException
	{
		WebSocketClient websocketConnection = new WebSocketClient(URI) 
		{
			@Override
			public void onOpen(ServerHandshake handshakedata)
			{
				SimpleWallet.this.websockets.put(getURI(), this);
			}

			@Override
			public void onMessage(String message)
			{
				JSONObject messageJSON = new JSONObject(message);
				
				Atom atom = Serialization.getInstance().fromJsonObject(messageJSON.getJSONObject("atom"), Atom.class);
				if (messageJSON.getJSONObject("atom").getString("hash").substring(JacksonCodecConstants.STR_VALUE_LEN).compareToIgnoreCase(atom.getHash().toString()) != 0)
				{
					apiLog.error(SimpleWallet.this.context.getName()+": Reconstructed atom hash is "+atom.getHash()+" but expected "+messageJSON.getJSONObject("atom").getString("hash"));
					return;
				}
				
				AtomCertificate certificate = null;
				if (messageJSON.has("certificate"))
					certificate = Serialization.getInstance().fromJsonObject(messageJSON.getJSONObject("certificate"), AtomCertificate.class);

				String type = messageJSON.getString("type");
				if (type.equalsIgnoreCase("committed"))
					SimpleWallet.this.committed(atom, certificate);
				else
					walletLog.error(SimpleWallet.this.context.getName()+": Unknown web socket message type "+type+" -> "+messageJSON.toString());
			}

			@Override
			public void onClose(int code, String reason, boolean remote)
			{
				if (remote)
					walletLog.info(SimpleWallet.this.context.getName()+": Wallet connection to "+this.uri+" closed remotely due to "+reason);
				else
					walletLog.info(SimpleWallet.this.context.getName()+": Wallet connection to "+this.uri+" closed");
				
				if (SimpleWallet.this.websockets.remove(getURI(), this) == false)
					walletLog.error(SimpleWallet.this.context.getName()+": Connection not found "+getURI());
					
				if (SimpleWallet.this.closed == true)
					return;
				
				// Open a new connection to a peer in the same shard group
				Executor.getInstance().submit(new Executable() 
				{
					@Override
					public void execute()
					{
						boolean reconnected = false;
						ShardGroupID shardGroupID = ShardGroupID.from(Integer.parseInt(getURI().getFragment()));
						try
						{
							if (openWebSocket(shardGroupID) != null)
								reconnected = true;

							if (reconnected == false)
							{
								walletLog.fatal(SimpleWallet.this.context.getName()+": Reconnection attempts shard group ID"+shardGroupID+" failed, closing wallet");
								close();
							}
						}
						catch (Exception ex)
						{
							walletLog.error(SimpleWallet.this.context.getName()+": Reconnection attempt to shard group ID "+shardGroupID+" threw exception", ex);
						}
					}
				});
			}

			@Override
			public void onError(Exception ex)
			{
				walletLog.error(SimpleWallet.this.context.getName()+": Reconnection attempt to "+getURI()+" failed", ex);
			}
		};
		
		if (websocketConnection.connectBlocking(5, TimeUnit.SECONDS) == false)
		{
			walletLog.error(this.context.getName()+": Failed to open web socket to "+websocketConnection.getURI());
			return null;
		}
		
		walletLog.info(this.context.getName()+": Connected to web socket at "+websocketConnection.getURI());
		return websocketConnection;
	}
	
	public Identity getIdentity()
	{
		return this.key.getIdentity();
	}

	public KeyPair<?,?,?> getKey() 
	{
		return this.key;
	}

	public UInt256 getBalance(final String symbol)
	{
		Objects.requireNonNull(symbol);
		
		this.lock.lock();
		try
		{
			StateAddress accountStateAddress = StateAddress.from(Vault.class.getAnnotation(StateContext.class).value(), this.key.getIdentity());
			Substate accountSubstate = this.substates.getOrDefault(accountStateAddress, new Substate(accountStateAddress));
			return accountSubstate.getOrDefault(symbol, UInt256.ZERO);
		}
		finally
		{
			this.lock.unlock();
		}
	}

	public Collection<Substate> get(final String ... contexts)
	{
		this.lock.lock();
		try
		{
			Set<String> toMatch = Arrays.stream(contexts).map(s -> s.toLowerCase()).collect(Collectors.toSet());
			List<Substate> substates = new ArrayList<Substate>();
			for (Substate substate : this.substates.values())
			{
				if (toMatch.contains(substate.getAddress().context()) == false)
					continue;
				
				substates.add(substate);
			}
			return substates;
		}
		finally
		{
			this.lock.unlock();
		}
	}
	
	public Atom spend(final String symbol, final UInt256 quantity, final Identity to) throws ValidationException, SerializationException, InsufficientBalanceException, InterruptedException, ExecutionException, TimeoutException
	{
		Atom atom = new Atom();
		spend(atom, symbol, quantity, to);
		return atom;
	}

	public Atom spend(final Atom atom, String symbol, final UInt256 quantity, final Identity to) throws ValidationException, InsufficientBalanceException, InterruptedException, ExecutionException, TimeoutException
	{
		if (atom.isSealed())
			throw new IllegalStateException("Atom "+atom.getHash()+" is sealed");

		symbol = symbol.toLowerCase();
		Future<SubstateSearchResponse> tokenSubstateSearchResponseFuture = context.getLedger().get(new SubstateSearchQuery(StateAddress.from(TokenComponent.class, Hash.valueOf(symbol.toLowerCase()))));
		SubstateSearchResponse tokenSubstateSearchResponse = tokenSubstateSearchResponseFuture.get(10, TimeUnit.SECONDS);
		if (tokenSubstateSearchResponse.getResult() == null)
			throw new ValidationException("Token "+symbol+" not found");
		
		Substate vault = this.substates.get(StateAddress.from(Vault.class.getAnnotation(StateContext.class).value(), this.key.getIdentity()));
		if (vault == null)
			throw new InsufficientBalanceException(this.key.getIdentity(), symbol, quantity);
		
		UInt256 balance = vault.getOrDefault(symbol, UInt256.ZERO);
		if (balance.compareTo(quantity) < 0)
			throw new InsufficientBalanceException(this.key.getIdentity(), symbol, quantity);

		atom.push(TokenComponent.class.getAnnotation(StateContext.class).value()+"::transfer(token('"+symbol+"'), uint256("+quantity+"), vault('"+this.key.getIdentity()+"'), vault('"+to+"'))");
		return atom;
	}
	
	public Future<AtomCertificate> submit(Atom atom) throws IOException, CryptoException
	{
		AtomFuture atomFuture;
		this.lock.lock();
		try
		{
			atom.sign(this.key);
			
			atomFuture = this.futures.get(atom.getHash());
			if (atomFuture == null)
			{
				atomFuture = new AtomFuture(atom);
				this.futures.put(atom.getHash(), atomFuture);
				
				if (SimpleWallet.this.context.getLedger().submit(atom) == false)
				{
					atomFuture.completeExceptionally(new RejectedExecutionException("Failed to submit atom "+atom.getHash()));
					this.futures.remove(atom.getHash());
					return atomFuture;
				}
				
				System.out.println(atom.getHash()+" -> "+Serialization.getInstance().toJson(atom, Output.API));
			}

			return atomFuture;
		}
		finally
		{
			this.lock.unlock();
		}
	}

	private void committed(final Atom atom, final AtomCertificate certificate)
	{
		this.lock.lock();
		try
		{
			AtomFuture future = this.futures.remove(atom.getHash());
			if (future != null)
				future.complete(certificate);
			
			Map<StateAddress, SubstateTransitions> stateTransitions = new HashMap<StateAddress, SubstateTransitions>();
			// Look for references of this wallet in the substate fields and authorities
			for (StateCertificate stateCertificate : certificate.getInventory(StateCertificate.class))
			{
				if (!stateCertificate.getStates().hasReads() && !stateCertificate.getStates().hasWrites())
					continue;
				
				if (this.substates.containsKey(stateCertificate.getStates().getAddress()))
				{
					stateTransitions.put(stateCertificate.getStates().getAddress(), stateCertificate.getStates());
					continue;
				}
				
				if (stateCertificate.getStates().getAuthority() != null && stateCertificate.getStates().getAuthority().equals(this.key.getIdentity()))
				{
					stateTransitions.put(stateCertificate.getStates().getAddress(), stateCertificate.getStates());
					continue;
				}
				
				if (stateCertificate.getStates().toSubstate(SubstateOutput.TRANSITIONS).references(this.key.getIdentity()))
				{
					stateTransitions.put(stateCertificate.getStates().getAddress(), stateCertificate.getStates());
					continue;
				}
			}
			
			for (SubstateTransitions stateTransition : stateTransitions.values())
			{
				this.substates.compute(stateTransition.getAddress(), (sa, st) -> {
					if (st == null)
						return stateTransition.toSubstate(SubstateOutput.TRANSITIONS); // TODO check for completeness & correctness
					
					return st.merge(stateTransition.toSubstate(SubstateOutput.TRANSITIONS));
				});
			}
		}
		finally
		{
			this.lock.unlock();
		}
	}
}
