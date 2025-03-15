package org.radix.hyperscale.ledger.events;

import java.util.Objects;
import org.radix.hyperscale.ledger.BlockHeader;
import org.radix.hyperscale.ledger.PendingAtom;

abstract class AtomEventWithProposalHeader extends AtomEvent {
  final BlockHeader header;

  protected AtomEventWithProposalHeader(final BlockHeader header, final PendingAtom pendingAtom) {
    super(pendingAtom);

    this.header = Objects.requireNonNull(header, "Proposal header is null");
  }

  public final BlockHeader getProposalHeader() {
    return this.header;
  }
}
