package org.radix.hyperscale.network;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.radix.hyperscale.executors.ScheduledExecutable;

public abstract class ConnectionTask extends ScheduledExecutable {
  private final AbstractConnection connection;

  protected ConnectionTask(
      final AbstractConnection connection, final long delay, final TimeUnit unit) {
    super(delay, 0, unit);

    this.connection = Objects.requireNonNull(connection, "Connection is null");
  }

  public AbstractConnection getConnection() {
    return this.connection;
  }
}
