package org.radix.hyperscale.ledger.events;

import java.util.Objects;
import org.radix.hyperscale.exceptions.ValidationException;
import org.radix.hyperscale.ledger.PendingAtom;

public class AtomExecutedEvent extends AtomEvent {
  private final ValidationException thrown;

  public AtomExecutedEvent(final PendingAtom pendingAtom) {
    super(pendingAtom);
    this.thrown = null;
  }

  public AtomExecutedEvent(final PendingAtom pendingAtom, final ValidationException thrown) {
    super(pendingAtom);
    this.thrown = Objects.requireNonNull(thrown, "ValidationException is null");
  }

  public ValidationException thrown() {
    return this.thrown;
  }
}
