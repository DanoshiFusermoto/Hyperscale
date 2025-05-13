package org.radix.hyperscale.ledger.events;

import org.radix.hyperscale.ledger.PendingAtom;
import org.radix.hyperscale.ledger.timeouts.PrepareTimeout;

public class AtomPrepareTimeoutEvent extends AtomEvent
{
	public AtomPrepareTimeoutEvent(final PendingAtom pendingAtom)
	{
		super(pendingAtom);
		
		final PrepareTimeout timeout = pendingAtom.getTimeout(PrepareTimeout.class); 
		if (timeout == null)
			throw new IllegalStateException(PrepareTimeout.class.getSimpleName()+" is not triggered for pending atom "+pendingAtom.getHash());
		
		pendingAtom.setTimeout(timeout.getClass());
	}
}
