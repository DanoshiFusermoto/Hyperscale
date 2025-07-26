package org.radix.hyperscale.network;

import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

import org.radix.hyperscale.collections.AdaptiveArrayList;
import org.radix.hyperscale.collections.AdaptiveCollection;
import org.radix.hyperscale.common.Primitive;
import org.radix.hyperscale.ledger.ShardGroupID;
import org.radix.hyperscale.utils.Numbers;

final class GossipBroadcast
{
	private final long				timestamp; 				
	private final Primitive 		primitive;
	private final Collection<ShardGroupID> shardGroupIDs;
	
	GossipBroadcast(final Primitive primitive)
	{
		this.primitive = Objects.requireNonNull(primitive, "Primitive is null");
		this.shardGroupIDs = new AdaptiveArrayList<ShardGroupID>(4);
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
			this.shardGroupIDs = new AdaptiveArrayList<ShardGroupID>(shardGroupIDs).freeze();
	}

	GossipBroadcast(final Primitive primitive, final Collection<ShardGroupID> shardGroupIDs)
	{
		Objects.requireNonNull(shardGroupIDs, "Shard group IDs is null");
		Numbers.isZero(shardGroupIDs.size(), "Shard group IDs is empty");
		this.primitive = Objects.requireNonNull(primitive, "Primitive is null");
		this.timestamp = System.currentTimeMillis();
		
		if (shardGroupIDs instanceof AdaptiveCollection adaptiveCollection && adaptiveCollection.isFrozen())
			this.shardGroupIDs = shardGroupIDs;
		else 
			this.shardGroupIDs = new AdaptiveArrayList<ShardGroupID>(shardGroupIDs).freeze();
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

	public Collection<ShardGroupID> getShardGroups()
	{
		return this.shardGroupIDs;
	}
	
	public void setShardGroups(final Set<ShardGroupID> shardGroupIDs)
	{
		Objects.requireNonNull(shardGroupIDs, "Shard group IDs is null");
		Numbers.isZero(shardGroupIDs.size(), "Shard groups IDs is empty");
		
		if (this.shardGroupIDs instanceof AdaptiveCollection freezable)
		{
			if (freezable.isFrozen())
				throw new IllegalStateException("Shard group IDs for broadcast as already set and are immutable");
		}
		
		if (this.shardGroupIDs.isEmpty() == false)
			throw new IllegalStateException("Shard group IDs for broadcast as already set");
		
		this.shardGroupIDs.addAll(shardGroupIDs);
	}
	
	@Override
	public String toString()
	{
		return this.primitive.getClass().getCanonicalName()+" "+this.primitive.getHash()+" "+(this.shardGroupIDs == null ? "" : this.shardGroupIDs);
	}
}
