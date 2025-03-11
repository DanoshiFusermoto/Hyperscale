package org.radix.hyperscale.ledger.events;

import org.radix.hyperscale.ledger.PendingBlock;

public final class BlockCommittedEvent extends PendingBlockEvent
{
	public BlockCommittedEvent(final PendingBlock pendingBlock)
	{
		super(pendingBlock);
	}
}
