package org.radix.hyperscale.ledger.sme;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.TreeMap;
import org.radix.hyperscale.common.BasicObject;
import org.radix.hyperscale.crypto.Identity;
import org.radix.hyperscale.ledger.StateAddress;
import org.radix.hyperscale.ledger.StateAddressable;
import org.radix.hyperscale.ledger.Substate;
import org.radix.hyperscale.ledger.Substate.NativeField;
import org.radix.hyperscale.ledger.SubstateOutput;
import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;
import org.radix.hyperscale.serialization.DsonCached;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.DsonOutput.Output;
import org.radix.hyperscale.serialization.SerializerId2;
import org.radix.hyperscale.utils.Strings;

@SerializerId2("ledger.substate.transitions")
@DsonCached
public final class SubstateTransitions extends BasicObject implements StateAddressable {
  private static final Logger serializerlog = Logging.getLogger("serializer");

  @JsonProperty("address")
  @DsonOutput(Output.ALL)
  private StateAddress address;

  @JsonProperty("reads")
  @DsonOutput(Output.ALL)
  @JsonDeserialize(as = TreeMap.class)
  private Map<String, Object> reads;

  @JsonProperty("writes")
  @DsonOutput(Output.ALL)
  @JsonDeserialize(as = TreeMap.class)
  private Map<String, Object> writes;

  @SuppressWarnings("unused")
  private SubstateTransitions() {
    // FOR SERIALIZER
  }

  SubstateTransitions(final StateAddress address, final boolean mutable) {
    Objects.requireNonNull(address, "State address is null");

    this.address = address;

    if (mutable) {
      this.reads = new TreeMap<String, Object>();
      this.writes = new TreeMap<String, Object>();
    } else {
      this.reads = Collections.emptyMap();
      this.writes = Collections.emptyMap();
    }
  }

  @Override
  public StateAddress getAddress() {
    return this.address;
  }

  public Identity getAuthority() {
    return get(NativeField.AUTHORITY.lower());
  }

  public boolean isVoid() {
    return (hasReads() == false && hasWrites() == false);
  }

  protected <T> T get(String field) {
    return getOrDefault(field, null);
  }

  @SuppressWarnings("unchecked")
  protected <T> T getOrDefault(String field, T def) {
    field = Strings.toLowerCase(field);

    Object value = null;
    if (this.writes != null) value = (T) this.writes.get(field);
    if (value == null && this.reads != null) value = (T) this.reads.get(field);
    if (value == null) value = def;

    return (T) value;
  }

  public boolean hasReads() {
    return this.reads != null && this.reads.isEmpty() == false ? true : false;
  }

  protected <T> T read(String field, final T value) {
    field = Strings.toLowerCase(field);
    Object existing = this.reads.putIfAbsent(field, value);
    return (T) existing;
  }

  public boolean hasWrites() {
    return this.writes != null && this.writes.isEmpty() == false ? true : false;
  }

  protected <T> T write(String field, final T value) {
    field = Strings.toLowerCase(field);
    Object existing = this.writes.put(field, value);
    return (T) existing;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = this.address.hashCode();
    result = prime * result + (this.reads == null ? 0 : this.reads.hashCode());
    result = prime * result + (this.writes == null ? 0 : this.writes.hashCode());
    return result;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) return true;

    if (obj instanceof SubstateTransitions other) {
      if (this.address.equals(other.address) == false) return false;

      if (this.reads != null && this.reads.equals(other.reads) == false) return false;

      if (this.writes != null && this.writes.equals(other.writes) == false) return false;

      return true;
    }

    return false;
  }

  public Map<String, Object> getReads() {
    return Collections.unmodifiableMap(this.reads);
  }

  public Map<String, Object> getWrites() {
    return Collections.unmodifiableMap(this.writes);
  }

  public Substate toSubstate(final SubstateOutput output) {
    Objects.requireNonNull(output, "Output type is null");

    if (output.equals(SubstateOutput.READS) == false
        && output.equals(SubstateOutput.WRITES) == false
        && output.equals(SubstateOutput.TRANSITIONS) == false)
      throw new IllegalArgumentException("Unsupported output type " + output);

    Substate substate = new Substate(this.address);

    if (this.reads != null) {
      if (output.equals(SubstateOutput.READS) || output.equals(SubstateOutput.TRANSITIONS)) {
        for (Entry<String, Object> read : this.reads.entrySet())
          substate.set(read.getKey(), read.getValue());
      }
    }

    if (this.writes != null) {
      if (output.equals(SubstateOutput.WRITES) || output.equals(SubstateOutput.TRANSITIONS)) {
        for (Entry<String, Object> write : this.writes.entrySet())
          substate.set(write.getKey(), write.getValue());
      }
    }

    return substate;
  }

  @Override
  public String toString() {
    return super.toString()
        + ", address="
        + this.address
        + ", reads="
        + this.reads
        + ", writes="
        + this.writes;
  }
}
