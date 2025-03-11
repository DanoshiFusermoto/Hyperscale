package org.radix.hyperscale.network.discovery;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.radix.hyperscale.Context;
import org.radix.hyperscale.ledger.ShardGroupID;
import org.radix.hyperscale.ledger.ShardMapper;
import org.radix.hyperscale.network.peers.Peer;
import org.radix.hyperscale.node.Services;
import org.radix.hyperscale.time.Time;

public final class OutboundDiscoveryFilter extends StandardDiscoveryFilter
{
	private final ShardGroupID shardGroupID;
	
	public OutboundDiscoveryFilter(final Context context, final ShardGroupID shardGroupID)
	{
		super(context);

		this.shardGroupID = Objects.requireNonNull(shardGroupID, "Shard group ID is null");
	}

	@Override
	public boolean filter(final Peer peer)
	{
		Objects.requireNonNull(peer, "Peer to filter is null");
		
		if (peer.getIdentity() == null)
			return false;
		
		if (peer.getServices().contains(Services.GATEWAY) == false && peer.getServices().contains(Services.BOOTSTRAP) == false)
			return false;
		
		if (ShardMapper.toShardGroup(peer.getIdentity(), getContext().getLedger().numShardGroups()).equals(this.shardGroupID) == false)
			return false;
		
		if (peer.getAttemptedAt() > 0 && peer.getDisconnectedAt() > 0 && 
			Time.getSystemTime() - peer.getDisconnectedAt() < TimeUnit.SECONDS.toMillis(getContext().getConfiguration().get("network.peer.inactivity", 30)))
			return false;
		
		if (peer.getAttemptAt() > 0 && Time.getSystemTime() < peer.getAttemptAt()+TimeUnit.SECONDS.toMillis(10))
			return false;

		return super.filter(peer);
	}
}
