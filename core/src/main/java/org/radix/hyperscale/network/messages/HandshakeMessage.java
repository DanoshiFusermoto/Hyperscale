package org.radix.hyperscale.network.messages;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;
import org.radix.hyperscale.crypto.bls12381.BLSSignature;
import org.radix.hyperscale.crypto.ed25519.EDPublicKey;
import org.radix.hyperscale.network.TransportParameters;
import org.radix.hyperscale.node.Node;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.DsonOutput.Output;
import org.radix.hyperscale.serialization.SerializerId2;

@SerializerId2("message.handshake")
@TransportParameters(priority = Integer.MAX_VALUE)
public class HandshakeMessage extends Message {
  @JsonProperty("node")
  @DsonOutput(Output.ALL)
  private Node node;

  @JsonProperty("ephemeral_key")
  @DsonOutput(Output.ALL)
  private EDPublicKey ephemeralKey;

  @JsonProperty("binding")
  @DsonOutput(Output.ALL)
  private BLSSignature binding;

  protected HandshakeMessage() {
    super();
  }

  public HandshakeMessage(
      final Node node, final EDPublicKey ephemeralKey, final BLSSignature binding) {
    super();

    this.node = new Node(Objects.requireNonNull(node, "Node is null"));
    this.ephemeralKey = Objects.requireNonNull(ephemeralKey, "Ephemeral key is null");
    this.binding = Objects.requireNonNull(binding, "Key binding signature is null");
  }

  public Node getNode() {
    return this.node;
  }

  public EDPublicKey getEphemeralKey() {
    return this.ephemeralKey;
  }

  public BLSSignature getBinding() {
    return this.binding;
  }
}
