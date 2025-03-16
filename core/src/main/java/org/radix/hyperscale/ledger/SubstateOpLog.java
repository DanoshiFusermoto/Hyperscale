package org.radix.hyperscale.ledger;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.ledger.sme.SubstateTransitions;
import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.DsonOutput.Output;
import org.radix.hyperscale.serialization.Serializable;
import org.radix.hyperscale.serialization.Serialization;
import org.radix.hyperscale.serialization.SerializationException;
import org.radix.hyperscale.serialization.SerializerId2;

@SerializerId2("ledger.substate.oplog")
public final class SubstateOpLog extends Serializable {
  private static final Logger serializerlog = Logging.getLogger("serializer");

  @JsonProperty("atom")
  @DsonOutput(Output.ALL)
  private Hash atom;

  @JsonProperty("substates")
  @DsonOutput(Output.ALL)
  @JsonDeserialize(as = LinkedHashMultimap.class)
  private Multimap<Integer, SubstateTransitions> substates;

  @SuppressWarnings("unused")
  private SubstateOpLog() {
    // FOR SERIALIZER
  }

  public SubstateOpLog(final Hash atom) {
    this.atom = atom;
    this.substates = LinkedHashMultimap.create(4, 2);
  }

  public SubstateOpLog(final Hash atom, final Multimap<Integer, SubstateTransitions> substates) {
    this.atom = atom;
    this.substates = LinkedHashMultimap.create(substates);

    // Ensure the DSON output caching is triggered.
    try {
      Serialization.getInstance().toDson(this, Output.PERSIST);
    } catch (SerializationException ex) {
      serializerlog.error("DSON cache priming failed", ex);
    }
  }

  public Hash getAtom() {
    return this.atom;
  }

  public boolean isEmpty() {
    return this.substates.isEmpty();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null) return false;
    if (other == this) return true;

    if (other instanceof SubstateOpLog substateOpLog) {
      if (substateOpLog.atom.equals(this.atom) == false) return false;

      if (substateOpLog.substates.equals(this.substates) == false) return false;

      return true;
    }

    return false;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 17;
    result = prime * result + this.atom.hashCode();
    result = prime * result + this.substates.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return this.atom + " = " + this.substates;
  }
}
