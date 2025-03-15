package org.radix.hyperscale.network.messages;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.radix.hyperscale.network.peers.Peer;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.DsonOutput.Output;
import org.radix.hyperscale.serialization.SerializerId2;

@SerializerId2("network.message.peers")
public final class PeersMessage extends Message {
  @JsonProperty("peers")
  @DsonOutput(Output.ALL)
  private List<String> peers;

  @SuppressWarnings("unused")
  private PeersMessage() {
    super();
  }

  public PeersMessage(final Collection<Peer> peers) {
    Objects.requireNonNull(peers, "Peers is null");
    if (peers.isEmpty()) throw new IllegalArgumentException("Peer is empty");

    this.peers = peers.stream().map(peer -> peer.getURI().toString()).collect(Collectors.toList());
  }

  public List<URI> getPeers() {
    return this.peers.stream().map(URI::create).collect(Collectors.toList());
  }
}
