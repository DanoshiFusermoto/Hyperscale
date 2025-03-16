package org.radix.hyperscale.database;

import java.io.IOException;
import java.util.Objects;
import org.radix.hyperscale.Service;
import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;

public abstract class DatabaseStore implements Service {
  protected static final Logger log = Logging.getLogger();

  private final DatabaseEnvironment environment;

  protected DatabaseStore(final DatabaseEnvironment environment) {
    super();

    this.environment = Objects.requireNonNull(environment, "Database environment is null");

    if (environment.isRegistered(this) == false) environment.register(this);
  }

  public void close() throws IOException {
    flush();

    if (this.environment.isRegistered(this) == true) this.environment.deregister(this);
  }

  protected DatabaseEnvironment getEnvironment() {
    return this.environment;
  }

  public abstract void flush() throws IOException;
}
