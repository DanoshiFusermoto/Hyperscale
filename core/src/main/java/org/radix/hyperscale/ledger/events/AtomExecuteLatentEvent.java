package org.radix.hyperscale.ledger.events;

import org.radix.hyperscale.ledger.BlockHeader;
import org.radix.hyperscale.ledger.PendingAtom;

public final class AtomExecuteLatentEvent extends AtomEventWithProposalHeader
{
	public AtomExecuteLatentEvent(final BlockHeader header, final PendingAtom pendingAtom)
	{
		super(header, pendingAtom);
		
		if (pendingAtom.getBlockHeader() == null)
			throw new IllegalStateException("Can not signal latency on "+pendingAtom.getHash()+" which has not been accepted in a proposal");
	}
}
