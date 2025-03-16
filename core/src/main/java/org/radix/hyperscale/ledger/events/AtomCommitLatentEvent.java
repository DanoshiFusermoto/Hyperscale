package org.radix.hyperscale.ledger.events;

import org.radix.hyperscale.ledger.PendingAtom;

public final class AtomCommitLatentEvent extends AtomEvent {
  public AtomCommitLatentEvent(final PendingAtom pendingAtom) {
    super(pendingAtom);

    if (pendingAtom.getBlockHeader() == null)
      throw new IllegalStateException(
          "Can not signal latency on "
              + pendingAtom.getHash()
              + " which has not been accepted in a proposal");
  }
}
