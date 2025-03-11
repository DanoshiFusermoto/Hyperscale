package org.radix.hyperscale.ledger.events;

import org.radix.hyperscale.ledger.PendingBlock;

public class BlockCommitEvent extends PendingBlockEvent
{
	public BlockCommitEvent(final PendingBlock pendingBlock)
	{
		super(pendingBlock);
	}
}
