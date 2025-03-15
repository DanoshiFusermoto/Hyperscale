package org.radix.hyperscale.network;

import java.util.concurrent.TimeUnit;
import org.radix.hyperscale.executors.ScheduledExecutable;

public abstract class NetworkTask extends ScheduledExecutable {
  protected NetworkTask(final long delay, final TimeUnit unit) {
    super(delay, 0, unit);
  }
}
