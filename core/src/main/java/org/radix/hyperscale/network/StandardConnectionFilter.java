package org.radix.hyperscale.network;

import java.net.URI;

import java.util.Objects;
import java.util.function.Function;

import org.radix.hyperscale.Context;
import org.radix.hyperscale.common.Direction;
import org.radix.hyperscale.crypto.Identity;
import org.radix.hyperscale.ledger.ShardGroupID;
import org.radix.hyperscale.ledger.ShardMapper;
import org.radix.hyperscale.utils.Numbers;

public class StandardConnectionFilter implements ConnectionFilter<AbstractConnection>
{
	private final Context 	  context;
	private Direction		  direction;
	private ShardGroupID	  shardGroupID;
	private Protocol 		  protocol;
	private Identity		  identity;
	private URI				  uri;
	private Boolean			  synced;
	private Boolean 		  stale;
	private ConnectionState[] states;
	private Function<AbstractConnection, Boolean> with;
	
	public static final StandardConnectionFilter build(final Context context)
	{
		return new StandardConnectionFilter(context);
	}

	private StandardConnectionFilter(final Context context)
	{
		this.context = Objects.requireNonNull(context, "Context is null");
	}

	public StandardConnectionFilter setStale(final boolean stale)
	{
		this.stale = stale;
		return this;
	}

	public StandardConnectionFilter setSynced(final boolean synced)
	{
		this.synced = synced;
		return this;
	}

	public StandardConnectionFilter setURI(final URI uri)
	{
		this.uri = Objects.requireNonNull(uri, "URI is null");
		return this;
	}

	public StandardConnectionFilter setIdentity(final Identity identity)
	{
		this.identity = Objects.requireNonNull(identity, "Direction is null");
		return this;
	}

	public StandardConnectionFilter setDirection(final Direction direction)
	{
		this.direction = Objects.requireNonNull(direction, "Direction is null");
		return this;
	}

	public StandardConnectionFilter setShardGroupID(final ShardGroupID shardGroupID)
	{
		this.shardGroupID = Objects.requireNonNull(shardGroupID, "Shard group ID is null");
		return this;
	}

	public StandardConnectionFilter setProtocol(final Protocol protocol)
	{
		this.protocol = Objects.requireNonNull(protocol, "Protocol is null");
		return this;
	}

	public StandardConnectionFilter with(Function<AbstractConnection, Boolean> function) 
	{
		this.with = Objects.requireNonNull(function, "Function is null");
		return this;
	}

	public StandardConnectionFilter setStates(final ConnectionState ... states)
	{
		Objects.requireNonNull(states, "States is null");
		Numbers.isZero(states.length, "Peer states is empty");
		
		this.states = states;
		return this;
	}
	
	@Override
	public boolean filter(final AbstractConnection connection)
	{
		Objects.requireNonNull(connection, "Connection is null");
		
		if (this.synced != null && (connection.getNode() == null || this.synced != connection.getNode().isSynced()))
			return false;
		
		if (this.shardGroupID != null && (connection.getNode() == null || ShardMapper.toShardGroup(connection.getNode().getIdentity(), this.context.getLedger().numShardGroups()).equals(this.shardGroupID) == false))
			return false;

		if (this.protocol != null && connection.getProtocol().equals(this.protocol) == false)
			return false;
		
		if (this.identity != null && (connection.getNode() == null || connection.getNode().getIdentity().equals(this.identity) == false))
			return false;

		if (this.direction != null && connection.getDirection().equals(this.direction) == false)
			return false;

		if (this.uri != null)
		{
			if (this.uri.getScheme() != null && connection.getURI().getScheme().equalsIgnoreCase(this.uri.getScheme()) == false)
				return false;

			if (this.uri.getHost() != null && connection.getURI().getHost().equalsIgnoreCase(this.uri.getHost()) == false)
				return false;

			if (this.uri.getPort() > -1 && connection.getURI().getPort() != this.uri.getPort())
				return false;
		}

		if (this.states != null)
		{
			boolean match = false;
			for (int i = 0 ; i < this.states.length ; i++)
			{
				final ConnectionState state = this.states[i];
				if (state == connection.getState())
				{
					match = true;
					break;
				}
			}

			if (match == false)
				return false;
		}
		
		if (this.stale != null && connection.isStale() != this.stale)
			return false;
		
		if (this.with != null && this.with.apply(connection) == false)
			return false;

		return true;
	}
}
