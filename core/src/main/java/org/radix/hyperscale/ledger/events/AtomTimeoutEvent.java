package org.radix.hyperscale.ledger.events;

import java.util.Objects;
import org.radix.hyperscale.ledger.PendingAtom;
import org.radix.hyperscale.ledger.timeouts.AtomTimeout;

public final class AtomTimeoutEvent extends AtomEvent {
  private AtomTimeout timeout;

  public AtomTimeoutEvent(final PendingAtom pendingAtom, final AtomTimeout timeout) {
    super(pendingAtom);

    this.timeout = Objects.requireNonNull(timeout, "Atom timeout is null");

    if (pendingAtom.getTimeout() != this.timeout)
      throw new IllegalStateException(
          "Timeout "
              + pendingAtom.getTimeout()
              + " in pending atom "
              + pendingAtom.getHash()
              + " does not match expected timeout "
              + timeout);
  }

  public AtomTimeout getTimeout() {
    return this.timeout;
  }
}
