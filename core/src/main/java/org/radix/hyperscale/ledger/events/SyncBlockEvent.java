package org.radix.hyperscale.ledger.events;

import org.radix.hyperscale.ledger.Block;

public final class SyncBlockEvent extends BlockEvent
{
	public SyncBlockEvent(final Block block)
	{
		super(block);
	}
}