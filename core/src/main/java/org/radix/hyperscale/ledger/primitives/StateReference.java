package org.radix.hyperscale.ledger.primitives;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;
import org.radix.hyperscale.common.ExtendedObject;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.ledger.StateAddress;
import org.radix.hyperscale.ledger.sme.SubstateTransitions;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.DsonOutput.Output;
import org.radix.hyperscale.serialization.SerializerId2;

@SerializerId2("ledger.state.reference")
public final class StateReference extends ExtendedObject implements StateOutput {
  @JsonProperty("atom")
  @DsonOutput(Output.ALL)
  private Hash atom;

  @JsonProperty("states")
  @DsonOutput(Output.ALL)
  private SubstateTransitions states;

  @JsonProperty("execution")
  @DsonOutput(Output.ALL)
  private Hash execution;

  @SuppressWarnings("unused")
  private StateReference() {
    super();

    // FOR SERIALIZER //
  }

  public StateReference(final Hash atom, final SubstateTransitions states, final Hash execution) {
    Objects.requireNonNull(atom, "Atom is null");
    Hash.notZero(atom, "Atom is ZERO");

    Objects.requireNonNull(states, "State inventory is null");

    this.atom = atom;
    this.states = states;
    this.execution = execution;
  }

  public Hash getAtom() {
    return this.atom;
  }

  @Override
  public StateAddress getAddress() {
    return this.states.getAddress();
  }

  public SubstateTransitions getStates() {
    return this.states;
  }

  public Hash getExecution() {
    return this.execution;
  }

  @Override
  public String toString() {
    return getHash() + " A: " + this.atom;
  }
}
