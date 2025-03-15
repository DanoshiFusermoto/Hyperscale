package org.radix.hyperscale.ledger.events;

import org.radix.hyperscale.ledger.BlockHeader;
import org.radix.hyperscale.ledger.PendingAtom;

public class AtomExecutableEvent extends AtomEventWithProposalHeader {
  public AtomExecutableEvent(final BlockHeader header, final PendingAtom pendingAtom) {
    super(header, pendingAtom);
  }
}
