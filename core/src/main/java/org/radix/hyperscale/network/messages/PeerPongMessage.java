package org.radix.hyperscale.network.messages;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.radix.hyperscale.node.Node;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.DsonOutput.Output;
import org.radix.hyperscale.serialization.SerializerId2;

@SerializerId2("network.message.pong")
public final class PeerPongMessage extends NodeMessage {
  @JsonProperty("nonce")
  @DsonOutput(Output.ALL)
  private long nonce;

  PeerPongMessage() {
    super();

    this.nonce = 0l;
  }

  public PeerPongMessage(final Node node, final long nonce) {
    super(node);

    this.nonce = nonce;
  }

  public long getNonce() {
    return this.nonce;
  }
}
