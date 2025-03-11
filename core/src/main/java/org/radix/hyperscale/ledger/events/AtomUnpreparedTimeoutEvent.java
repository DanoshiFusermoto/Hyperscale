package org.radix.hyperscale.ledger.events;

import org.radix.hyperscale.ledger.PendingAtom;
import org.radix.hyperscale.ledger.timeouts.PrepareTimeout;

public class AtomUnpreparedTimeoutEvent extends AtomEvent
{
	public AtomUnpreparedTimeoutEvent(final PendingAtom pendingAtom)
	{
		super(pendingAtom);

		if (pendingAtom.getTimeout() == null)
			throw new IllegalStateException("No prepare timeout present in pending atom "+pendingAtom.getHash());
		
		if (PrepareTimeout.class.isAssignableFrom(pendingAtom.getTimeout().getClass()) == false)
			throw new IllegalStateException("Timeout is not a prepare timeout in pending atom "+pendingAtom.getHash());
	}
}
