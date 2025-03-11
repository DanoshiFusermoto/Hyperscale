package org.radix.hyperscale.ledger.events;

import java.util.Objects;

import org.radix.hyperscale.events.Event;
import org.radix.hyperscale.ledger.PendingBlock;

abstract class PendingBlockEvent implements Event 
{
	private final PendingBlock pendingBlock;
	
	PendingBlockEvent(final PendingBlock pendingBlock)
	{
		this.pendingBlock = Objects.requireNonNull(pendingBlock, "Pending block is null");
	}
	
	public final PendingBlock getPendingBlock()
	{
		return this.pendingBlock;
	}
}