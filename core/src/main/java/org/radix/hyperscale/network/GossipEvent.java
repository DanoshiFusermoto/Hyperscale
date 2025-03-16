package org.radix.hyperscale.network;

import java.util.Objects;
import org.radix.hyperscale.network.messages.Message;

final class GossipEvent {
  private final boolean urgent;
  private final Message message;
  private final AbstractConnection connection;

  GossipEvent(final Message message, final AbstractConnection connection) {
    this.connection = Objects.requireNonNull(connection, "Primitive is null");
    this.message = Objects.requireNonNull(message, "Message is null");
    this.urgent = message.isUrgent();
  }

  public boolean isUrgent() {
    return this.urgent;
  }

  @SuppressWarnings("unchecked")
  public <T extends Message> T getMessage() {
    return (T) this.message;
  }

  public AbstractConnection getConnection() {
    return this.connection;
  }
}
