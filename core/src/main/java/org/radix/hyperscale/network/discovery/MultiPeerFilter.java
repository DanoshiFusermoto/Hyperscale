package org.radix.hyperscale.network.discovery;

import java.util.ArrayList;
import java.util.List;

import org.radix.hyperscale.network.peers.Peer;

public class MultiPeerFilter implements PeerFilter
{
	private final List<PeerFilter> peerFilters;
	
	public MultiPeerFilter(final PeerFilter ... peerFilters)
	{
		this.peerFilters = new ArrayList<PeerFilter>(peerFilters.length);
		for(final PeerFilter peerFilter : peerFilters)
			this.peerFilters.add(peerFilter);
	}

	public MultiPeerFilter(final List<PeerFilter> peerFilters)
	{
		this.peerFilters = new ArrayList<PeerFilter>(peerFilters);
	}

	@Override
	public boolean filter(final Peer peer) 
	{
		for (final PeerFilter peerFilter : this.peerFilters)
		{
			if (peerFilter.filter(peer) == false)
				return false;
		}
		
		return true;
	}
}
