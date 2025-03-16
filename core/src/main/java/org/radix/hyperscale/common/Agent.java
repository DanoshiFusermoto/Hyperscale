package org.radix.hyperscale.common;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.util.Objects;
import java.util.StringJoiner;
import org.radix.hyperscale.Universe;
import org.radix.hyperscale.node.Node;
import org.radix.hyperscale.node.Services;
import org.radix.hyperscale.utils.Numbers;

public final class Agent {
  public static final int PROTOCOL_VERSION = 100;
  public static final int AGENT_VERSION = 100;
  public static final int REFUSE_AGENT_VERSION = 99;
  public static final String URI_PREFIX = "CASSIE://";
  public static final String AGENT = "CASSIE-REF-IMP";

  @Deprecated
  public static final URI getURI(String host) {
    Objects.requireNonNull(host, "Host is null");
    if (host.isEmpty()) throw new IllegalArgumentException("Host length is zero");

    if (host.toUpperCase().startsWith(Agent.URI_PREFIX) == false) host = Agent.URI_PREFIX + host;

    URI uri = URI.create(host);

    if ((uri.getPath() != null && uri.getPath().length() > 0)
        || (uri.getQuery() != null && uri.getQuery().length() > 0))
      throw new IllegalArgumentException(uri + ": Paths or queries are not permitted in agent URI");

    if (uri.getPort() == -1) {
      host += ":" + Universe.getDefault().getPort();
      uri = URI.create(host);
    }

    return uri;
  }

  public static final URI getURI(final String host, final int port) {
    Objects.requireNonNull(host, "Host is null");
    Numbers.isZero(host.length(), "Host is empty");
    Numbers.isNegative(port, "Port is negative");

    if (port > 0) return URI.create(Agent.URI_PREFIX + host + ":" + port);
    else return URI.create(Agent.URI_PREFIX + host);
  }

  public static final URI getURI(SocketAddress socketAddress) {
    if (socketAddress instanceof InetSocketAddress inetSocketAddress)
      return getURI(inetSocketAddress.getHostString(), 0);

    throw new IllegalArgumentException("Unknown socket address type " + socketAddress.getClass());
  }

  public static final URI getURI(final String host, final Node node) {
    StringBuilder agentURIBuilder = new StringBuilder();
    agentURIBuilder.append(Agent.URI_PREFIX);
    agentURIBuilder.append(node.getIdentity().toString());
    agentURIBuilder.append("@");
    agentURIBuilder.append(host);
    agentURIBuilder.append(":");
    agentURIBuilder.append(node.getNetworkPort());
    agentURIBuilder.append("/");
    agentURIBuilder.append(node.getProtocolVersion());
    agentURIBuilder.append("/");
    agentURIBuilder.append(node.getAgentVersion());
    agentURIBuilder.append("/");
    agentURIBuilder.append(node.getAgent());
    agentURIBuilder.append("?");

    if (node.getServices().isEmpty() == false) {
      StringJoiner servicesJoiner = new StringJoiner("&");
      for (Services service : node.getServices()) servicesJoiner.add(service.name());

      agentURIBuilder.append(servicesJoiner.toString());
    }

    return URI.create(agentURIBuilder.toString());
  }

  private Agent() {}
}
