package org.radix.hyperscale.ledger.events;

import java.util.Objects;

import org.radix.hyperscale.events.Event;
import org.radix.hyperscale.ledger.Block;

abstract class BlockEvent implements Event 
{
	private final Block block;
	
	BlockEvent(final Block block)
	{
		this.block = Objects.requireNonNull(block, "Block is null");
	}
	
	public final Block getBlock()
	{
		return this.block;
	}
}
