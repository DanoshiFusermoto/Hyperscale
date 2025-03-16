package org.radix.hyperscale.network.discovery;

import org.radix.hyperscale.network.peers.Peer;

@FunctionalInterface
public interface PeerFilter {
  boolean filter(final Peer peer);
}
