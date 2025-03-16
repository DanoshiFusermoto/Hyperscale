package org.radix.hyperscale.ledger.events;

import org.radix.hyperscale.ledger.PendingBlock;

public class BlockAppliedEvent extends PendingBlockEvent {
  public BlockAppliedEvent(final PendingBlock pendingBlock) {
    super(pendingBlock);
  }
}
