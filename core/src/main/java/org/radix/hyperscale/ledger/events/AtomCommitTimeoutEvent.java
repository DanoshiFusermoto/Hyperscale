package org.radix.hyperscale.ledger.events;

import org.radix.hyperscale.ledger.BlockHeader;
import org.radix.hyperscale.ledger.PendingAtom;
import org.radix.hyperscale.ledger.BlockHeader.InventoryType;
import org.radix.hyperscale.ledger.timeouts.CommitTimeout;

public final class AtomCommitTimeoutEvent extends AtomEventWithProposalHeader
{
	public AtomCommitTimeoutEvent(final BlockHeader header, final PendingAtom pendingAtom)
	{
		super(header, pendingAtom);
		
		if (pendingAtom.getBlockHeader() == null)
			throw new IllegalStateException("Can not "+CommitTimeout.class.getSimpleName()+" "+pendingAtom.getHash()+" which has not been included in a block");

		final CommitTimeout timeout = pendingAtom.getTimeout(CommitTimeout.class); 
		if (timeout == null)
			throw new IllegalStateException(CommitTimeout.class.getSimpleName()+" is not triggered for pending atom "+pendingAtom.getHash());
		
		if (header.contains(timeout.getHash(), InventoryType.UNCOMMITTED) == false)
			throw new IllegalStateException(CommitTimeout.class.getSimpleName()+" for pending atom "+pendingAtom.getHash()+" is not referenced in block "+header);
		
		pendingAtom.setTimeout(timeout.getClass());
	}
}
