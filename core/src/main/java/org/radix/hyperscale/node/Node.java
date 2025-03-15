package org.radix.hyperscale.node;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.radix.hyperscale.Constants;
import org.radix.hyperscale.common.BasicObject;
import org.radix.hyperscale.crypto.Identity;
import org.radix.hyperscale.ledger.BlockHeader;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.DsonOutput.Output;
import org.radix.hyperscale.serialization.SerializerId2;
import org.radix.hyperscale.utils.Numbers;

@SerializerId2("node")
public class Node extends BasicObject {
  private String agent;

  private int agentVersion;

  private int protocolVersion;

  @JsonProperty("services")
  @DsonOutput(Output.ALL)
  @JsonDeserialize(as = LinkedHashSet.class)
  private Set<Services> services;

  @JsonProperty("network_port")
  @DsonOutput(Output.ALL)
  private int networkPort;

  @JsonProperty("api_port")
  @DsonOutput(Output.ALL)
  private int apiPort;

  @JsonProperty("websocket_port")
  @DsonOutput(Output.ALL)
  private int websocketPort;

  @JsonProperty("head")
  @DsonOutput(Output.ALL)
  private volatile BlockHeader head;

  /** Whether the node is signalling it is synced with respect to its shard group */
  @JsonProperty("synced")
  @DsonOutput(value = {Output.API, Output.WIRE})
  private volatile boolean synced;

  @JsonProperty("identity")
  @DsonOutput(Output.ALL)
  private Identity identity;

  public Node() {
    super();

    this.agent = "unknown";
    this.agentVersion = 0;
    this.head = null;
    this.protocolVersion = 0;
    this.networkPort = 0;
    this.websocketPort = 0;
    this.apiPort = 0;
    this.identity = null;
    this.synced = false;
    this.services = Collections.emptySet();
  }

  public Node(final Node node) {
    super();

    Objects.requireNonNull(node, "Node is null");

    this.agent = node.getAgent();
    this.agentVersion = node.getAgentVersion();
    this.head = node.getHead();
    this.protocolVersion = node.getProtocolVersion();
    this.networkPort = node.getNetworkPort();
    this.apiPort = node.getAPIPort();
    this.websocketPort = node.getWebsocketPort();
    this.identity = node.getIdentity();
    this.synced = node.isSynced();
    this.services = Collections.unmodifiableSet(new LinkedHashSet<Services>(node.getServices()));
  }

  public Node(
      final Identity identity,
      final BlockHeader head,
      final String agent,
      final int agentVersion,
      final int protocolVersion,
      final int networkPort,
      final int apiPort,
      final int websocketPort,
      final Set<Services> services,
      final boolean synced) {
    this();

    this.identity = Objects.requireNonNull(identity, "Identity is null");
    this.agent = Objects.requireNonNull(agent, "Agent is null");
    this.head = Objects.requireNonNull(head, "BlockHeader is null");
    this.services =
        Collections.unmodifiableSet(
            new LinkedHashSet<Services>(Objects.requireNonNull(services, "Services is null")));

    Numbers.isNegative(agentVersion, "Agent version is negative");
    Numbers.isNegative(protocolVersion, "Protocol version is negative");

    Numbers.inRange(networkPort, 1, 65535, "Network port is invalid");
    Numbers.inRange(apiPort, 1, 65535, "API port is invalid");
    Numbers.inRange(websocketPort, 1, 65535, "Websocket port is invalid");

    this.agentVersion = agentVersion;
    this.protocolVersion = protocolVersion;
    this.networkPort = networkPort;
    this.apiPort = apiPort;
    this.websocketPort = websocketPort;
    this.synced = synced;
  }

  public String getAgent() {
    return this.agent;
  }

  public int getAgentVersion() {
    return this.agentVersion;
  }

  public int getProtocolVersion() {
    return this.protocolVersion;
  }

  public int getNetworkPort() {
    return this.networkPort;
  }

  void setNetworkPort(final int port) {
    Numbers.inRange(port, 1, 65535, "Network port is invalid");
    this.networkPort = port;
  }

  public int getAPIPort() {
    return this.apiPort;
  }

  void setAPIPort(final int port) {
    this.apiPort = Numbers.inRange(port, 1, 65535, "API port is invalid");
  }

  public int getWebsocketPort() {
    return this.websocketPort;
  }

  void setWebsocketPort(final int port) {
    this.websocketPort = Numbers.inRange(port, 1, 65535, "Websocket port is invalid");
  }

  public Set<Services> getServices() {
    return this.services;
  }

  public BlockHeader getHead() {
    return this.head;
  }

  public void setHead(final BlockHeader head) {
    this.head = Objects.requireNonNull(head, "Block header is null");
  }

  // SYNC //
  public final boolean isSynced() {
    return this.synced;
  }

  public final void setSynced(boolean synced) {
    this.synced = synced;
  }

  public Identity getIdentity() {
    return this.identity;
  }

  // Property "agent" - 1 getter, 1 setter
  // FIXME: Should be included in a serializable class
  @JsonProperty("agent")
  @DsonOutput(Output.ALL)
  Map<String, Object> getJsonAgent() {
    return ImmutableMap.of(
        "name", this.agent,
        "version", this.agentVersion,
        "protocol", this.protocolVersion);
  }

  @JsonProperty("agent")
  void setJsonAgent(Map<String, Object> props) {
    this.agent = (String) props.get("name");
    this.agentVersion = ((Number) props.get("version")).intValue();
    this.protocolVersion = ((Number) props.get("protocol")).intValue();
  }

  @Override
  public String toString() {
    return this.getIdentity().toString(Constants.TRUNCATED_IDENTITY_LENGTH)
        + " @ "
        + this.head.toString();
  }
}
