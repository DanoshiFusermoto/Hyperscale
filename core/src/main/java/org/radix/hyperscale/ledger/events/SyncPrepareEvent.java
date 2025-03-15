package org.radix.hyperscale.ledger.events;

import java.util.Objects;
import org.radix.hyperscale.events.Event;
import org.radix.hyperscale.ledger.BlockHeader;

public class SyncPrepareEvent implements Event {
  private final BlockHeader head;

  public SyncPrepareEvent(final BlockHeader head) {
    super();

    this.head = Objects.requireNonNull(head, "Sync header is null");
  }

  public BlockHeader getHead() {
    return this.head;
  }
}
