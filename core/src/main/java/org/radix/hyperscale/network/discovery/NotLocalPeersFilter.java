package org.radix.hyperscale.network.discovery;

import java.net.InetAddress;
import java.util.Objects;
import org.radix.hyperscale.network.peers.Peer;
import org.radix.hyperscale.node.Node;

public class NotLocalPeersFilter implements PeerFilter {
  private final Node node;
  private boolean banned;
  private boolean invalid;

  public NotLocalPeersFilter(final Node node) {
    Objects.requireNonNull(node, "Node is null");
    this.node = node;
    this.banned = false;
  }

  public NotLocalPeersFilter setBanned(boolean banned) {
    this.banned = banned;
    return this;
  }

  public NotLocalPeersFilter setInvalid(boolean invalid) {
    this.invalid = invalid;
    return this;
  }

  @Override
  public boolean filter(final Peer peer) {
    if (peer.getIdentity() == null) return false;

    if (peer.getIdentity().equals(this.node.getIdentity())) return false;

    if (this.banned == false && peer.isBanned()) return false;

    if (this.invalid == false && peer.isInvalid()) return false;

    final InetAddress inetAddress;
    try {
      inetAddress = InetAddress.getByName(peer.getURI().getHost());
    } catch (Exception ex) {
      return false;
    }

    if (inetAddress.isAnyLocalAddress()
        || inetAddress.isLoopbackAddress()
        || inetAddress.isLinkLocalAddress()
        || inetAddress.isSiteLocalAddress()) return false;

    return true;
  }
}
