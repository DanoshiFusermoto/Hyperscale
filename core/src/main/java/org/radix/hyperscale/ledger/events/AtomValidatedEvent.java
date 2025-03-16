package org.radix.hyperscale.ledger.events;

import org.radix.hyperscale.ledger.PendingAtom;

public final class AtomValidatedEvent extends AtomEvent {
  public AtomValidatedEvent(final PendingAtom pendingAtom) {
    super(pendingAtom);
  }
}
