package org.radix.hyperscale.ledger;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;
import org.radix.hyperscale.common.BasicObject;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.DsonOutput.Output;
import org.radix.hyperscale.serialization.SerializerId2;
import org.radix.hyperscale.utils.Numbers;

@SerializerId2("ledger.association.commit")
public final class AssociationCommit extends BasicObject {
  @JsonProperty("referrer")
  @DsonOutput(Output.ALL)
  private Hash referrer;

  @JsonProperty("address")
  @DsonOutput(Output.ALL)
  private StateAddress address;

  @JsonProperty("index")
  @DsonOutput(Output.ALL)
  private long index;

  @JsonProperty("timestamp")
  @DsonOutput(Output.ALL)
  private long timestamp;

  @SuppressWarnings("unused")
  private AssociationCommit() {
    // FOR SERIALIZER
  }

  AssociationCommit(
      final Hash referrer, final StateAddress address, final long index, final long timestamp) {
    Objects.requireNonNull(referrer, "Referrer hash is null");
    Hash.notZero(referrer, "Referrer hash is null");
    Objects.requireNonNull(address, "Key hash is null");
    Numbers.isNegative(index, "Index is negative");
    Numbers.isNegative(timestamp, "Timestamp is negative");

    this.index = index;
    this.timestamp = timestamp;
    this.referrer = referrer;
    this.address = address;
  }

  public long getIndex() {
    return this.index;
  }

  public Hash getReferrer() {
    return this.referrer;
  }

  public StateAddress getAddress() {
    return this.address;
  }

  public long getTimestamp() {
    return this.timestamp;
  }

  @Override
  public String toString() {
    return this.referrer + " " + this.address + " " + this.index + " " + this.timestamp;
  }
}
