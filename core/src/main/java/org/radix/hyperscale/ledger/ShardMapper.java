package org.radix.hyperscale.ledger;

import java.util.Collection;
import java.util.Objects;
import java.util.Set;

import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.set.MutableSet;
import org.radix.hyperscale.crypto.Identity;
import org.radix.hyperscale.utils.Numbers;

public final class ShardMapper 
{
	public static ShardGroupID toShardGroup(final Identity identity, final int numShardGroups)
	{
		return toShardGroup(Objects.requireNonNull(identity, "Identity is null").getHash().asLong(), numShardGroups);
	}

	public static ShardGroupID toShardGroup(final StateAddress address, final int numShardGroups)
	{
		return toShardGroup(Objects.requireNonNull(address, "State address is null").getShardID(), numShardGroups);
	}

	public static boolean equal(final int numShardGroups, final Identity primary, final Identity secondary)
	{
		return toShardGroup(primary, numShardGroups).equals(toShardGroup(secondary, numShardGroups));
	}

	public static boolean equal(final int numShardGroups, final StateAddress stateAddress, final Identity identity)
	{
		return toShardGroup(stateAddress, numShardGroups).equals(toShardGroup(identity, numShardGroups));
	}

	public static Set<ShardGroupID> toShardGroups(final Collection<ShardID> shards, final int numShardGroups)
	{
		final MutableSet<ShardGroupID> shardGroups = Sets.mutable.ofInitialCapacity(shards.size());
		for (ShardID shard : Objects.requireNonNull(shards, "Shards is null"))
			shardGroups.add(toShardGroup(shard, numShardGroups));
		
		return shardGroups.asUnmodifiable();
	}

	public static Set<ShardGroupID> toShardGroups(final Collection<ShardID> shards, ShardGroupID exclude, final int numShardGroups)
	{
		final MutableSet<ShardGroupID> shardGroups = Sets.mutable.ofInitialCapacity(shards.size());
		for (ShardID shard : Objects.requireNonNull(shards, "Shards is null"))
		{
			ShardGroupID shardGroupID = toShardGroup(shard, numShardGroups);
			if (shardGroupID.equals(exclude))
				continue;

			shardGroups.add(shardGroupID);
		}
		
		return shardGroups.asUnmodifiable();
	}

	public static Set<ShardGroupID> toShardGroups(final PendingAtom atom, final StateLockMode lockMode, final int numShardGroups)
	{
		if (atom.isGlobal())
			return toAllShardGroups(numShardGroups);

		final MutableSet<ShardGroupID> shardGroups = Sets.mutable.ofInitialCapacity(atom.numStateAddresses(lockMode));
		atom.forShards(lockMode, (stateAddress, shardID) -> {
			ShardGroupID shardGroupID = toShardGroup(shardID, numShardGroups);
			shardGroups.add(shardGroupID);
		});

		return shardGroups.asUnmodifiable();
	}

	public static Set<ShardGroupID> toShardGroups(final PendingAtom atom, final StateLockMode lockMode, final ShardGroupID exclude, final int numShardGroups)
	{
		if (atom.isGlobal())
			return toAllShardGroups(exclude, numShardGroups);

		final MutableSet<ShardGroupID> shardGroups = Sets.mutable.ofInitialCapacity(atom.numStateAddresses(lockMode));
		atom.forShards(lockMode, (stateAddress, shardID) -> {
			ShardGroupID shardGroupID = toShardGroup(shardID, numShardGroups);
			if (shardGroupID.equals(exclude))
				return;

			shardGroups.add(shardGroupID);
		});
		
		return shardGroups.asUnmodifiable();
	}

	// TODO test this heavily ... UInts may have low value ranges and so all end up in similar shards ... don't want that
	public static ShardGroupID toShardGroup(final ShardID shard, final int numShardGroups)
	{
		Objects.requireNonNull(shard, "Shard is null");
		if (shard.equals(ShardID.ZERO))
			throw new IllegalArgumentException("Shard is ZERO");
		
		return toShardGroup(shard.asLong(), numShardGroups);
	}
	
	private static ShardGroupID toShardGroup(final long truncatedShard, final int numShardGroups)
	{
		Numbers.isNegative(numShardGroups, "Num shard groups is negative");
		return ShardGroupID.from((int) Math.abs(truncatedShard % numShardGroups));
	}
	
	public static Set<ShardGroupID> toAllShardGroups(final int numShardGroups)
	{
		return toAllShardGroups(null, numShardGroups);
	}
	
	public static Set<ShardGroupID> toAllShardGroups(final ShardGroupID exclude, final int numShardGroups)
	{
		final MutableSet<ShardGroupID> shardGroups = Sets.mutable.ofInitialCapacity(numShardGroups);
		for (int s = 0 ; s < numShardGroups ; s++)
		{
			if (exclude != null && s == exclude.intValue())
				continue;
			
			shardGroups.add(ShardGroupID.from(s));
		}
		return shardGroups;
	}

	private ShardMapper() {}
}
