package org.radix.hyperscale.ledger.events;

import org.radix.hyperscale.ledger.BlockHeader;
import org.radix.hyperscale.ledger.PendingAtom;
import org.radix.hyperscale.ledger.primitives.StateCertificate;
import org.radix.hyperscale.ledger.timeouts.ExecutionTimeout;

public final class AtomExecutionTimeoutEvent extends AtomEventWithProposalHeader {
  public AtomExecutionTimeoutEvent(final BlockHeader header, final PendingAtom pendingAtom) {
    super(header, pendingAtom);

    if (pendingAtom.getTimeout() == null)
      throw new IllegalStateException(
          "No execution timeout present in pending atom " + pendingAtom.getHash());

    if (ExecutionTimeout.class.isAssignableFrom(pendingAtom.getTimeout().getClass()) == false)
      throw new IllegalStateException(
          "Timeout is not an execution timeout in pending atom " + pendingAtom.getHash());

    if (pendingAtom.getBlockHeader() == null
        && pendingAtom.getOutputs(StateCertificate.class).isEmpty() == false)
      throw new IllegalStateException(
          "Can not timeout "
              + pendingAtom.getHash()
              + " which has state certificates before being included in a block");
  }
}
