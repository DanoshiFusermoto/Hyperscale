package org.radix.hyperscale.ledger.events;

import org.radix.hyperscale.ledger.PendingAtom;
import org.radix.hyperscale.ledger.timeouts.PrepareTimeout;

public class AtomPrepareTimeoutEvent extends AtomEvent
{
	public AtomPrepareTimeoutEvent(final PendingAtom pendingAtom)
	{
		super(pendingAtom);

		if (pendingAtom.getTimeout(PrepareTimeout.class) == null)
			throw new IllegalStateException("No prepare timeout present in pending atom "+pendingAtom.getHash());
	}
}
