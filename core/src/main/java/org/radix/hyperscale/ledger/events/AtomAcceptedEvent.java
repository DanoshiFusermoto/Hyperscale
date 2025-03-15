package org.radix.hyperscale.ledger.events;

import org.radix.hyperscale.ledger.BlockHeader;
import org.radix.hyperscale.ledger.PendingAtom;

public class AtomAcceptedEvent extends AtomEventWithProposalHeader {
  public AtomAcceptedEvent(final BlockHeader header, final PendingAtom pendingAtom) {
    super(header, pendingAtom);
  }
}
