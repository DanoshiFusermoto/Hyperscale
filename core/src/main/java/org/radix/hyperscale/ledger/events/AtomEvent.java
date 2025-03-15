package org.radix.hyperscale.ledger.events;

import java.util.Objects;
import org.radix.hyperscale.events.Event;
import org.radix.hyperscale.ledger.PendingAtom;
import org.radix.hyperscale.ledger.primitives.Atom;

abstract class AtomEvent implements Event {
  private final PendingAtom pendingAtom;

  AtomEvent(final PendingAtom pendingAtom) {
    this.pendingAtom = Objects.requireNonNull(pendingAtom, "Pending atom is null");
  }

  public final PendingAtom getPendingAtom() {
    return this.pendingAtom;
  }

  public final Atom getAtom() {
    return this.pendingAtom.getAtom();
  }
}
