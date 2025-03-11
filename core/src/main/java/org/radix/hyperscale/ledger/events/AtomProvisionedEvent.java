package org.radix.hyperscale.ledger.events;

import org.radix.hyperscale.ledger.PendingAtom;

public final class AtomProvisionedEvent extends AtomEvent
{
	public AtomProvisionedEvent(final PendingAtom pendingAtom)
	{
		super(pendingAtom);
	}
}