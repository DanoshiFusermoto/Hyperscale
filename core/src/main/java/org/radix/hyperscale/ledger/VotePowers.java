package org.radix.hyperscale.ledger;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import org.radix.hyperscale.crypto.Identity;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.DsonOutput.Output;
import org.radix.hyperscale.serialization.Serializable;
import org.radix.hyperscale.serialization.SerializerId2;

@SerializerId2("ledger.vote.powers")
public final class VotePowers extends Serializable {
  @JsonProperty("epoch")
  @DsonOutput(Output.ALL)
  private long epoch;

  @JsonProperty("power")
  @DsonOutput(Output.ALL)
  @JsonDeserialize(as = TreeMap.class)
  private TreeMap<Identity, Long> powers;

  private VotePowers() {
    // FOR SERIALIZER
  }

  public VotePowers(final long epoch, final Map<Identity, Long> powers) {
    this();

    this.epoch = epoch;
    this.powers = new TreeMap<>(powers);
  }

  public long getEpoch() {
    return this.epoch;
  }

  public long get(final Identity identity) {
    return this.powers.get(identity);
  }

  public Map<Identity, Long> getAll() {
    return Collections.unmodifiableMap(this.powers);
  }

  public int size() {
    return this.powers.size();
  }

  public VotePowers merge(final VotePowers other) {
    if (this.epoch != other.epoch)
      throw new IllegalArgumentException("Can not merge vote power with different epochs");

    // Merge is based on highest vote power
    final Map<Identity, Long> mergedPowers = new HashMap<>(this.powers);
    for (final Identity identity : other.powers.keySet()) {
      long thisPower = this.powers.getOrDefault(identity, -1l);
      long thatPower = other.powers.getOrDefault(identity, -1l);
      if (thisPower > -1 && thatPower > -1) {
        if (thatPower > thisPower) mergedPowers.put(identity, thatPower);
      } else if (thatPower > -1) mergedPowers.put(identity, thatPower);
      else if (thisPower > -1) mergedPowers.put(identity, thisPower);
    }

    return new VotePowers(this.epoch, mergedPowers);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (int) (this.epoch ^ (this.epoch >>> 32));
    result = prime * result + ((this.powers == null) ? 0 : this.powers.hashCode());
    return result;
  }

  @Override
  public boolean equals(final Object object) {
    if (this == object) return true;

    if (object == null) return false;

    if (object instanceof VotePowers other) {
      if (this.epoch != other.epoch) return false;

      if (this.powers == null) {
        if (other.powers != null) return false;
      } else if (this.powers.equals(other.powers) == false) return false;

      return true;
    }

    return false;
  }
}
