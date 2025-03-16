package org.radix.hyperscale.ledger.events;

import org.radix.hyperscale.ledger.PendingBlock;

public class BlockConstructedEvent extends PendingBlockEvent {
  public BlockConstructedEvent(final PendingBlock pendingBlock) {
    super(pendingBlock);
  }
}
