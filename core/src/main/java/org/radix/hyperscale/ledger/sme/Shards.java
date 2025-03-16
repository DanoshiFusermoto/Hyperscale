package org.radix.hyperscale.ledger.sme;

import org.radix.hyperscale.ledger.sme.exceptions.StateMachineExecutionException;
import org.radix.hyperscale.utils.Numbers;

public class Shards implements GlobalInstruction {
  private final long epoch;

  // Current and Proposed could be UInt256, but using ints is easier for now and allows for 2B
  // validator sets!
  private final int current;
  private final int proposed;

  public Shards(final long epoch, final int current, final int proposed) {
    Numbers.isNegative(epoch, "Clock is negative");
    Numbers.isNegative(current, "Current shard groups is negative");
    Numbers.isNegative(proposed, "Proposed shard groups is negative");

    this.epoch = epoch;
    this.current = current;
    this.proposed = proposed;
  }

  public long getEpoch() {
    return this.epoch;
  }

  public int numCurrent() {
    return this.current;
  }

  public int numProposed() {
    return this.proposed;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + (int) (this.epoch & 0xFFFFFFFF);
    result = prime * result + (int) (this.current & 0xFFFFFFFF);
    result = prime * result + (int) (this.proposed & 0xFFFFFFFF);
    return result;
  }

  @Override
  public boolean equals(Object object) {
    if (object == null) return false;

    if (this == object) return true;

    if (object instanceof Shards shards) {
      if (this.epoch != shards.epoch) return false;

      if (this.current != shards.current) return false;

      if (this.proposed != shards.proposed) return false;

      return true;
    }

    return false;
  }

  void lock(final StateMachine stateMachine) {}

  void execute(final StateMachine stateMachine) throws StateMachineExecutionException {}
}
