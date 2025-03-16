package org.radix.hyperscale.network.events;

import org.radix.hyperscale.network.AbstractConnection;

public final class BannedEvent extends ConnectionEvent {
  public BannedEvent(final AbstractConnection connection) {
    super(connection);
  }
}
