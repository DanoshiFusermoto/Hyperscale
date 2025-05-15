package org.radix.hyperscale.ledger.events;

import org.radix.hyperscale.ledger.BlockHeader;
import org.radix.hyperscale.ledger.BlockHeader.InventoryType;
import org.radix.hyperscale.ledger.PendingAtom;
import org.radix.hyperscale.ledger.timeouts.ExecutionTimeout;

public final class AtomExecutionTimeoutEvent extends AtomEventWithProposalHeader
{
	public AtomExecutionTimeoutEvent(final BlockHeader header, final PendingAtom pendingAtom)
	{
		super(header, pendingAtom);
		
		if (pendingAtom.getBlockHeader() == null)
			throw new IllegalStateException("Can not "+ExecutionTimeout.class.getSimpleName()+" "+pendingAtom.getHash()+" which has not been included in a block");

		final ExecutionTimeout timeout = pendingAtom.getTimeout(ExecutionTimeout.class); 
		if (timeout == null)
			throw new IllegalStateException(ExecutionTimeout.class.getSimpleName()+" is not triggered for pending atom "+pendingAtom.getHash());
		
		if (header.contains(timeout.getHash(), InventoryType.UNEXECUTED) == false)
			throw new IllegalStateException(ExecutionTimeout.class.getSimpleName()+" for pending atom "+pendingAtom.getHash()+" is not referenced in block "+header);
		
		pendingAtom.setTimeout(timeout.getClass());
	}
}

