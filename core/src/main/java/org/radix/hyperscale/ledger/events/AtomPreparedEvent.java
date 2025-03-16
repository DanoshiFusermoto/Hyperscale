package org.radix.hyperscale.ledger.events;

import org.radix.hyperscale.ledger.PendingAtom;

public final class AtomPreparedEvent extends AtomEvent {
  public AtomPreparedEvent(final PendingAtom pendingAtom) {
    super(pendingAtom);
  }
}
