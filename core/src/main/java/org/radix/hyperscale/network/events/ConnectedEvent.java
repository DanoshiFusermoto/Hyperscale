package org.radix.hyperscale.network.events;

import org.radix.hyperscale.network.AbstractConnection;

public final class ConnectedEvent extends ConnectionEvent {
  public ConnectedEvent(final AbstractConnection connection) {
    super(connection);
  }
}
