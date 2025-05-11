package org.radix.hyperscale.ledger.events;

import org.radix.hyperscale.ledger.BlockHeader;
import org.radix.hyperscale.ledger.PendingAtom;
import org.radix.hyperscale.ledger.timeouts.AcceptTimeout;

public final class AtomAcceptedTimeoutEvent extends AtomEventWithProposalHeader
{
	public AtomAcceptedTimeoutEvent(final BlockHeader header, final PendingAtom pendingAtom) 
	{
		super(header, pendingAtom);

		if (pendingAtom.getTimeout(AcceptTimeout.class) == null)
			throw new IllegalStateException("No accept timeout present in pending atom "+pendingAtom.getHash());
	}
}
