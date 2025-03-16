package org.radix.hyperscale.ledger;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;
import org.radix.hyperscale.common.BasicObject;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.DsonOutput.Output;
import org.radix.hyperscale.serialization.SerializerId2;
import org.radix.hyperscale.utils.Numbers;

@SerializerId2("ledger.substate.commit")
public final class SubstateCommit extends BasicObject {
  @JsonProperty("id")
  @DsonOutput(Output.ALL)
  private long id;

  @JsonProperty("index")
  @DsonOutput(Output.ALL)
  private long index;

  @JsonProperty("timestamp")
  @DsonOutput(Output.ALL)
  private long timestamp;

  @JsonProperty("mutator")
  @DsonOutput(Output.ALL)
  private Hash mutator;

  @JsonProperty("substate")
  @DsonOutput(Output.ALL)
  private Substate substate;

  @JsonProperty("revision")
  @DsonOutput(Output.ALL)
  private long revision;

  @SuppressWarnings("unused")
  private SubstateCommit() {
    // FOR SERIALIZER
  }

  SubstateCommit(final Substate substate, final long timestamp) {
    Objects.requireNonNull(substate, "Substate is null");
    Numbers.isNegative(timestamp, "Timestamp is negative");

    this.id = -1;
    this.index = -1;
    this.timestamp = timestamp;
    this.revision = 0;
    this.substate = substate;
  }

  SubstateCommit(final Substate substate, final long index, final long timestamp) {
    Objects.requireNonNull(substate, "Substate is null");
    Numbers.isNegative(index, "Index is negative");
    Numbers.isNegative(timestamp, "Timestamp is negative");

    this.id = index;
    this.index = index;
    this.timestamp = timestamp;
    this.revision = 0;
    this.substate = substate;
  }

  SubstateCommit(
      final Substate substate, final Hash mutator, final long index, final long timestamp) {
    Objects.requireNonNull(substate, "Substate is null");
    Objects.requireNonNull(mutator, "Mutator hash is null");
    Hash.notZero(mutator, "Mutator hash is ZERO");
    Numbers.isNegative(index, "Index is negative");
    Numbers.isNegative(timestamp, "Timestamp is negative");

    this.id = index;
    this.index = index;
    this.timestamp = timestamp;
    this.revision = 0;
    this.mutator = mutator;
    this.substate = substate;
  }

  SubstateCommit(
      final Substate substate,
      final Hash mutator,
      final long index,
      final long timestamp,
      final SubstateCommit previous) {
    Objects.requireNonNull(substate, "Substate is null");
    Objects.requireNonNull(mutator, "Mutator hash is null");
    Hash.notZero(mutator, "Mutator hash is ZERO");
    Objects.requireNonNull(previous, "Previous commit is null");
    Numbers.isNegative(index, "Index is negative");
    Numbers.isNegative(timestamp, "Timestamp is negative");

    this.id = previous.id;
    this.index = index;
    this.timestamp = timestamp;
    this.revision = previous.revision + 1;
    this.mutator = mutator;
    this.substate = substate;
  }

  public long getID() {
    return this.id;
  }

  public long getIndex() {
    return this.index;
  }

  public long getRevision() {
    return this.revision;
  }

  public Hash getMutator() {
    return this.mutator;
  }

  public Substate getSubstate() {
    return this.substate;
  }

  public long getTimestamp() {
    return this.timestamp;
  }

  @Override
  public String toString() {
    return this.substate
        + " "
        + (this.mutator == null ? Hash.ZERO : this.mutator)
        + " "
        + this.id
        + " "
        + this.index
        + " "
        + this.timestamp;
  }
}
