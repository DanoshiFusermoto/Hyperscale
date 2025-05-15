package org.radix.hyperscale.ledger.events;

import org.radix.hyperscale.ledger.BlockHeader;
import org.radix.hyperscale.ledger.PendingAtom;
import org.radix.hyperscale.ledger.BlockHeader.InventoryType;
import org.radix.hyperscale.ledger.timeouts.AcceptTimeout;

public final class AtomAcceptedTimeoutEvent extends AtomEventWithProposalHeader
{
	public AtomAcceptedTimeoutEvent(final BlockHeader header, final PendingAtom pendingAtom) 
	{
		super(header, pendingAtom);
		
		final AcceptTimeout timeout = pendingAtom.getTimeout(AcceptTimeout.class); 
		if (timeout == null)
			throw new IllegalStateException(AcceptTimeout.class.getSimpleName()+" is not triggered for pending atom "+pendingAtom.getHash());
		
		if (header.contains(timeout.getAtom(), InventoryType.UNACCEPTED) == false)
			throw new IllegalStateException(AcceptTimeout.class.getSimpleName()+" for pending atom "+pendingAtom.getHash()+" is not referenced in block "+header);
		
		pendingAtom.setTimeout(timeout.getClass());
	}
}
