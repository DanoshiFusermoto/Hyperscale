package org.radix.hyperscale.ledger.events;

import org.radix.hyperscale.ledger.PendingAtom;

public final class AtomAcceptableEvent extends AtomEvent 
{
	public AtomAcceptableEvent(final PendingAtom pendingAtom)
	{
		super(pendingAtom);
	}
}
