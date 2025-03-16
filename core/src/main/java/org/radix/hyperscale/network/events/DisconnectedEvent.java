package org.radix.hyperscale.network.events;

import org.radix.hyperscale.network.AbstractConnection;

public final class DisconnectedEvent extends ConnectionEvent {
  private Throwable exception;

  public DisconnectedEvent(final AbstractConnection connection, Throwable exception) {
    super(connection);

    this.exception = exception;
  }

  public Throwable getException() {
    return this.exception;
  }
}
