package org.radix.hyperscale.network;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.radix.hyperscale.common.Primitive;
import org.radix.hyperscale.ledger.ShardGroupID;
import org.radix.hyperscale.utils.Numbers;

final class GossipBroadcast
{
	private final long				timestamp; 				
	private final Primitive 		primitive;
	private final List<ShardGroupID> shardGroupIDs;
	
	GossipBroadcast(final Primitive primitive)
	{
		this.primitive = Objects.requireNonNull(primitive, "Primitive is null");
		this.shardGroupIDs = new ArrayList<ShardGroupID>(4);
		this.timestamp = System.currentTimeMillis();
	}

	GossipBroadcast(final Primitive primitive, final ShardGroupID shardGroupID)
	{
		this.primitive = Objects.requireNonNull(primitive, "Primitive is null");
		this.shardGroupIDs = Collections.singletonList(shardGroupID);
		this.timestamp = System.currentTimeMillis();
	}

	GossipBroadcast(final Primitive primitive, final ShardGroupID ... shardGroupIDs)
	{
		Objects.requireNonNull(shardGroupIDs, "Shard group IDs is null");
		Numbers.isZero(shardGroupIDs.length, "Shard group IDs is empty");
		this.primitive = Objects.requireNonNull(primitive, "Primitive is null");
		this.timestamp = System.currentTimeMillis();
		
		if (shardGroupIDs.length == 1)
			this.shardGroupIDs = Collections.singletonList(shardGroupIDs[0]);
		else
			this.shardGroupIDs = Arrays.asList(shardGroupIDs);
	}

	GossipBroadcast(final Primitive primitive, final Collection<ShardGroupID> shardGroupIDs)
	{
		Objects.requireNonNull(shardGroupIDs, "Shard group IDs is null");
		Numbers.isZero(shardGroupIDs.size(), "Shard group IDs is empty");
		this.primitive = Objects.requireNonNull(primitive, "Primitive is null");
		this.shardGroupIDs = new ArrayList<ShardGroupID>(shardGroupIDs);
		this.timestamp = System.currentTimeMillis();
	}
	
	public boolean isUrgent()
	{
		final TransportParameters transportParameters = this.primitive.getClass().getAnnotation(TransportParameters.class);
		if (transportParameters == null)
			return false;
		
		return transportParameters.urgent();
	}
	
	public long getTimestamp()
	{
		return this.timestamp;
	}

	public Primitive getPrimitive()
	{
		return this.primitive;
	}

	public List<ShardGroupID> getShardGroups()
	{
		return this.shardGroupIDs;
	}
	
	public void setShardGroups(final Set<ShardGroupID> shardGroupIDs)
	{
		Objects.requireNonNull(shardGroupIDs, "Shard group IDs is null");
		this.shardGroupIDs.clear();
		this.shardGroupIDs.addAll(shardGroupIDs);
	}
	
	@Override
	public String toString()
	{
		return this.primitive.getClass().getCanonicalName()+" "+this.primitive.getHash()+" "+(this.shardGroupIDs == null ? "" : this.shardGroupIDs);
	}

}
