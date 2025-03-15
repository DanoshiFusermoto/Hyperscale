package org.radix.hyperscale.ledger.events;

import java.util.Objects;
import org.radix.hyperscale.ledger.PendingAtom;

public final class AtomExceptionEvent extends AtomEvent {
  private final Exception exception;

  public AtomExceptionEvent(final PendingAtom pendingAtom, final Exception exception) {
    super(pendingAtom);

    this.exception = Objects.requireNonNull(exception, "Exception is null");
  }

  public Exception getException() {
    return this.exception;
  }
}
