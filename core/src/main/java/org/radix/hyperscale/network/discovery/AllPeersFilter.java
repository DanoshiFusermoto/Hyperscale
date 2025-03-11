package org.radix.hyperscale.network.discovery;

import org.radix.hyperscale.network.peers.Peer;

public final class AllPeersFilter implements PeerFilter
{
	@Override
	public boolean filter(final Peer peer)
	{
		return true;
	}
}
