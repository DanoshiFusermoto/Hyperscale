package org.radix.hyperscale.network.messages;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;
import org.radix.hyperscale.node.Node;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.DsonOutput.Output;
import org.radix.hyperscale.serialization.SerializerId2;

/**
 * TODO need a secure way to sign the Node object so that it may be passed around the network in
 * peer updates and used in discovery processes
 */
@SerializerId2("message.node")
public class NodeMessage extends Message {
  @JsonProperty("node")
  @DsonOutput(Output.ALL)
  private Node node;

  protected NodeMessage() {
    super();
  }

  public NodeMessage(final Node node) {
    super();

    this.node = new Node(Objects.requireNonNull(node, "Node is null"));
  }

  public Node getNode() {
    return this.node;
  }
}
