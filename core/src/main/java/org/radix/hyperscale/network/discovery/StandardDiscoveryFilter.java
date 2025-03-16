package org.radix.hyperscale.network.discovery;

import java.net.URI;
import java.util.Objects;
import java.util.function.Function;
import org.radix.hyperscale.Context;
import org.radix.hyperscale.common.Agent;
import org.radix.hyperscale.crypto.Identity;
import org.radix.hyperscale.ledger.ShardGroupID;
import org.radix.hyperscale.ledger.ShardMapper;
import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;
import org.radix.hyperscale.network.Protocol;
import org.radix.hyperscale.network.peers.Peer;

public class StandardDiscoveryFilter implements DiscoveryFilter {
  private static final Logger log = Logging.getLogger();

  private final Context context;

  private ShardGroupID shardGroupID;
  private Protocol protocol;
  private Identity identity;
  private URI uri;
  private Function<Peer, Boolean> with;

  // TODO remove context and use dependency injection instead
  public StandardDiscoveryFilter(final Context context) {
    this.context = Objects.requireNonNull(context);
  }

  protected Context getContext() {
    return this.context;
  }

  public StandardDiscoveryFilter setURI(final URI uri) {
    this.uri = Objects.requireNonNull(uri, "URI is null");
    return this;
  }

  public StandardDiscoveryFilter setIdentity(final Identity identity) {
    this.identity = Objects.requireNonNull(identity, "Direction is null");
    return this;
  }

  public StandardDiscoveryFilter setShardGroupID(final ShardGroupID shardGroupID) {
    this.shardGroupID = Objects.requireNonNull(shardGroupID, "Shard group ID is null");
    return this;
  }

  public StandardDiscoveryFilter setProtocol(final Protocol protocol) {
    this.protocol = Objects.requireNonNull(protocol, "Protocol is null");
    return this;
  }

  public StandardDiscoveryFilter with(Function<Peer, Boolean> function) {
    this.with = Objects.requireNonNull(function, "Function is null");
    return this;
  }

  public boolean filter(final Peer peer) {
    Objects.requireNonNull(peer, "Peer is null");

    try {
      if (peer.getIdentity() == null) return false;

      if (peer.getIdentity().equals(this.context.getNode().getIdentity())) return false;

      if (peer.getProtocolVersion() != 0 && peer.getProtocolVersion() < Agent.PROTOCOL_VERSION)
        return false;

      if (peer.getAgentVersion() != 0 && peer.getAgentVersion() <= Agent.REFUSE_AGENT_VERSION)
        return false;

      if (peer.isBanned()) return false;

      if (peer.isInvalid()) return false;

      if (this.shardGroupID != null
          && ShardMapper.toShardGroup(peer.getIdentity(), this.context.getLedger().numShardGroups())
                  .equals(this.shardGroupID)
              == false) return false;

      if (this.protocol != null && peer.hasProtocol(this.protocol) == false) return false;

      if (this.identity != null && peer.getIdentity().equals(this.identity) == false) return false;

      if (this.uri != null && peer.getURI().equals(this.uri) == false) return false;

      if (this.with != null && this.with.apply(peer) == false) return false;

      return true;
    } catch (Exception ex) {
      log.error("Could not process filter on PeerFilter for Peer:" + peer.toString(), ex);
      return false;
    }
  }
}
