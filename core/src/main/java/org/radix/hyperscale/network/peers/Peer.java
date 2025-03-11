package org.radix.hyperscale.network.peers;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

import org.radix.hyperscale.common.BasicObject;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.crypto.Identity;
import org.radix.hyperscale.network.AbstractConnection;
import org.radix.hyperscale.network.Protocol;
import org.radix.hyperscale.node.Services;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.SerializerId2;
import org.radix.hyperscale.serialization.DsonOutput.Output;
import org.radix.hyperscale.time.Time;
import org.radix.hyperscale.utils.Numbers;
import org.radix.hyperscale.utils.UInt128;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@SerializerId2("network.peer")
public final class Peer extends BasicObject
{
	public final static Peer from(final AbstractConnection connection)
	{
		Objects.requireNonNull(connection, "Connection is null");
		
		Peer peer = new Peer(connection.getURI());
		peer.setBanned(connection.getBanReason(), connection.getBannedUntil());
		peer.setConnectedAt(connection.getConnectedAt());
		peer.setConnectingAt(connection.getConnectingAt());
		peer.setDisconnectedAt(connection.getDisconnectedAt());
		peer.addProtocol(connection.getProtocol());
		peer.setLatency(connection.getLatency());
		return peer;
	}

	
	private URI	uri = null;

	@JsonProperty("protocols")
	@DsonOutput(Output.ALL)
	@JsonDeserialize(as=LinkedHashSet.class)
	private Set<Protocol> protocols;

	@JsonProperty("attempts")
	@DsonOutput(Output.PERSIST)
	private int attempts;

	@JsonProperty("accept_at")
	@DsonOutput(Output.PERSIST)
	private long acceptAt;

	@JsonProperty("attempt_at")
	@DsonOutput(Output.PERSIST)
	private long attemptAt;

	@JsonProperty("attempted_at")
	@DsonOutput(Output.PERSIST)
	private long attemptedAt;

	@JsonProperty("connected_at")
	@DsonOutput(Output.PERSIST)
	private long connectedAt;
	
	@JsonProperty("connecting_at")
	@DsonOutput(Output.PERSIST)
	private long connectingAt;

	@JsonProperty("disconnected_at")
	@DsonOutput(Output.PERSIST)
	private long disconnectedAt;

	@JsonProperty("banned_until")
	@DsonOutput(Output.PERSIST)
	private long bannedUntil;
	
	@JsonProperty("known_at")
	@DsonOutput(Output.PERSIST)
	private long knownAt;

	@JsonProperty("latency")
	@DsonOutput(Output.PERSIST)
	private int latency;

	@JsonProperty("invalid")
	@DsonOutput(Output.PERSIST)
	private boolean invalid;

	private String banReason = null;

	private transient Integer hashcode = 0;
	private transient Identity identity = null;
	
	protected Peer()
	{
		super();
		
		this.knownAt = Time.getSystemTime();
		this.latency = Integer.MAX_VALUE;
	}
	
	protected Peer(URI uri)
	{
		this();
		
		this.uri = Objects.requireNonNull(uri);
		this.protocols = new LinkedHashSet<Protocol>();
	}

	public Peer(URI uri, Protocol ... protocols)
	{
		this(uri);
		
		this.protocols = new LinkedHashSet<Protocol>();
		for (Protocol protocol : protocols)
			this.protocols.add(protocol);
	}

	@Override
	public boolean equals(Object object)
	{
		if (object == null) 
			return false;
		
		if (object == this) 
			return true;

		if (object instanceof Peer peer)
		{
			if (getURI().equals(peer.getURI()) == false)
				return false;

			return true;
		}

		return false;
	}

	@Override
	public int hashCode()
	{
		if (this.hashcode == null)
			this.hashcode = getURI().toString().toLowerCase().hashCode();
		
		return this.hashcode;
	}

	@Override
	public String toString()
	{
		return this.uri.toString()+(this.banReason == null ? "" : (" BANNED "+this.bannedUntil));
	}

	public URI getURI()
	{
		return this.uri;
	}

	public void setURI(URI uri)
	{
		this.uri = uri;
	}
	
	public int getLatency()
	{
		return this.latency;
	}

	public void setLatency(int latency)
	{
		Numbers.lessThan(latency, 0, "Latency is negative");
		this.latency = latency;
	}

	@JsonProperty("host")
	@DsonOutput(Output.API)
	public URI getHost()
	{
		try 
		{
			return new URI(null, null, this.uri.getHost(), this.uri.getPort(), null, null, null);
		} 
		catch (URISyntaxException usex) 
		{
			throw new IllegalArgumentException(usex);
		}
	}
	
	public URI getHostOnly()
	{
		try 
		{
			return new URI(null, null, this.uri.getHost(), -1, null, null, null);
		} 
		catch (URISyntaxException usex) 
		{
			throw new IllegalArgumentException(usex);
		}
	}

	@JsonProperty("identity")
	@DsonOutput(Output.API)
	public Identity getIdentity()
	{
		if (this.identity == null && this.uri.getUserInfo() != null)
			this.identity = Identity.from(this.uri.getUserInfo());
		
		return this.identity;
	}
	
	@JsonProperty("protocol_version")
	@DsonOutput(Output.API)
	public int getProtocolVersion()
	{
		String path = this.uri.getPath();
		String[] components = path.substring(1).split("/");
		if (components.length < 3)
			throw new IllegalStateException("Peer URI path does not contain protocol version");
		
		return Integer.parseInt(components[0]);
	}

	@JsonProperty("agent_version")
	@DsonOutput(Output.API)
	public int getAgentVersion()
	{
		String path = this.uri.getPath();
		String[] components = path.substring(1).split("/");
		if (components.length < 3)
			throw new IllegalStateException("Peer URI path does not contain agent version");
		
		return Integer.parseInt(components[1]);
	}

	@JsonProperty("agent")
	@DsonOutput(Output.API)
	public String getAgent()
	{
		String path = this.uri.getPath();
		String[] components = path.substring(1).split("/");
		if (components.length < 3)
			throw new IllegalStateException("Peer URI path does not contain agent info");
		
		return components[2];
	}

	@JsonProperty("services")
	@DsonOutput(Output.API)
	public Set<Services> getServices()
	{
		String query = this.uri.getQuery();
		if (query == null || query.isEmpty())
			return Collections.emptySet();
			
		Set<Services> supportedServices = new HashSet<Services>();
		String[] services = query.split("&");
		for (String service : services)
		{
			Services serviceEnum = Services.valueOf(service.toUpperCase());
			if (serviceEnum == null)
				throw new UnsupportedOperationException("Service "+service.toUpperCase()+" is unknown");
			
			supportedServices.add(serviceEnum);
		}
		
		return supportedServices;
	}
	
	public boolean isInvalid()
	{
		return this.invalid;
	}
	
	void setInvalid(boolean invalid)
	{
		this.invalid = invalid;
	}

	public boolean hasService(final Services service) 
	{
		Objects.requireNonNull(service, "Service is null");
		
		return getServices().contains(service);
	}

	public String getBanReason()
	{
		return this.banReason;
	}

	public boolean hasProtocol()
	{
		return this.protocols.isEmpty() == false;
	}

	public boolean hasProtocol(Protocol protocol)
	{
		return this.protocols.contains(protocol);
	}

	void addProtocol(Protocol protocol)
	{
		this.protocols.add(protocol);
	}

	public boolean isBanned()
	{
		return getBannedUntil() > Time.getSystemTime();
	}
	
	public long getBannedUntil()
	{
		return this.bannedUntil;
	}
	
	public void setBanned(String banReason, long bannedUntil)
	{
		this.bannedUntil = bannedUntil;
		this.banReason = banReason;
	}
	
	public int getAttempts()
	{
		return this.attempts;
	}

	void setAttempts(int attempts)
	{
		this.attempts = attempts;
	}

	public long getAcceptAt()
	{
		return this.acceptAt;
	}

	void setAcceptAt(long timestamp)
	{
		this.acceptAt = timestamp;
	}

	public long getAttemptAt()
	{
		return this.attemptAt;
	}

	void setAttemptAt(long timestamp)
	{
		this.attemptAt = timestamp;
	}

	public long getAttemptedAt()
	{
		return this.attemptedAt;
	}

	void setAttemptedAt(long timestamp)
	{
		this.attemptedAt = timestamp;
	}

	public long getConnectedAt()
	{
		return this.connectedAt;
	}

	void setConnectedAt(long timestamp)
	{
		this.connectedAt = timestamp;
	}

	public long getConnectingAt()
	{
		return this.connectingAt;
	}

	void setConnectingAt(long timestamp)
	{
		this.connectingAt = timestamp;
	}

	public long getDisconnectedAt()
	{
		return this.disconnectedAt;
	}

	void setDisconnectedAt(long timestamp)
	{
		this.disconnectedAt = timestamp;
	}

	public long getKnownAt()
	{
		return this.knownAt;
	}

	// Property "host" - 1 getter, 1 setter
	// Could potentially just serialize the URI as a string
	@JsonProperty("uri")
	@DsonOutput(Output.ALL)
	private String serializeHost() 
	{
		return this.uri.toString();
	}

	@JsonProperty("uri")
	private void deserializeHost(String uri) 
	{
		this.uri = java.net.URI.create(uri);
	}

	// Property "ban_reason" - 1 getter, 1 setter
	// FIXME: Should just serialize if non-null
	@JsonProperty("ban_reason")
	@DsonOutput(Output.PERSIST)
	private String serializeBanReason() 
	{
		return (getBannedUntil() > 0) ? banReason : null;
	}

	@JsonProperty("ban_reason")
	private void deserializeBanReason(String banReason) 
	{
		this.banReason = banReason;
	}

	public UInt128 distance(final Identity identity)
	{
		Hash dh = Hash.hash(identity.getHash().toByteArray(), getIdentity().getHash().toByteArray());
		return UInt128.from(dh.toByteArray()).xor(UInt128.from(getIdentity().getHash().toByteArray()));
	}
}
