package org.radix.hyperscale.ledger.events;

import java.util.Objects;
import org.radix.hyperscale.ledger.BlockVote;
import org.radix.hyperscale.ledger.PendingBlock;

public class BlockVoteEvent extends PendingBlockEvent {
  private final BlockVote vote;

  public BlockVoteEvent(final PendingBlock pendingBlock, final BlockVote vote) {
    super(pendingBlock);

    this.vote = Objects.requireNonNull(vote, "Block vote is null");
  }

  public BlockVote getBlockVote() {
    return this.vote;
  }
}
