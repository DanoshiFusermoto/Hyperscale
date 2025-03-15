package org.radix.hyperscale.ledger.primitives;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.list.MutableList;
import org.radix.hyperscale.crypto.Certificate;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.exceptions.ValidationException;
import org.radix.hyperscale.ledger.CommitDecision;
import org.radix.hyperscale.ledger.StateContext;
import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;
import org.radix.hyperscale.serialization.DsonCached;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.DsonOutput.Output;
import org.radix.hyperscale.serialization.Serialization;
import org.radix.hyperscale.serialization.SerializationException;
import org.radix.hyperscale.serialization.SerializerId2;

@SerializerId2("ledger.atom.certificate")
@StateContext("atom.certificate")
@DsonCached
public final class AtomCertificate extends Certificate {
  private static final Logger serializerlog = Logging.getLogger("serializer");

  @JsonProperty("atom")
  @DsonOutput(Output.ALL)
  private Hash atom;

  @JsonProperty("inventory")
  @DsonOutput(value = {Output.API, Output.WIRE, Output.PERSIST})
  private List<StateOutput> inventory;

  @SuppressWarnings("unused")
  private AtomCertificate() {
    super();

    // FOR SERIALIZER
  }

  public AtomCertificate(
      final Hash atom,
      final CommitDecision decision,
      final Collection<? extends StateOutput> inventory)
      throws ValidationException {
    super(decision);

    Objects.requireNonNull(inventory, "State output inventory is null");
    Objects.requireNonNull(atom, "Atom is null");
    Hash.notZero(atom, "Atom is ZERO");
    this.atom = atom;

    if (inventory.isEmpty()) throw new IllegalArgumentException("State output inventory is empty");

    this.inventory = Lists.mutable.ofAll(inventory);
    for (int i = 0; i < this.inventory.size(); i++) {
      StateOutput stateOutput = this.inventory.get(i);
      if (stateOutput.getAtom().equals(this.atom) == false)
        throw new ValidationException(
            stateOutput,
            "State certificate for "
                + stateOutput.getAddress()
                + " does not reference atom "
                + this.atom);
    }

    this.inventory.sort((so1, so2) -> so1.getAddress().compareTo(so2.getAddress()));

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

  @SuppressWarnings("unchecked")
  @Override
  public <T> T getObject() {
    return (T) this.atom;
  }

  public int getInventorySize() {
    return this.inventory.size();
  }

  public <T extends StateOutput> List<T> getInventory(Class<T> clazz) {
    MutableList<T> inventory = Lists.mutable.ofInitialCapacity(this.inventory.size());
    for (int i = 0; i < this.inventory.size(); i++) {
      StateOutput output = this.inventory.get(i);
      if (clazz.isInstance(output) == false) continue;

      inventory.add(clazz.cast(output));
    }

    return inventory.asUnmodifiable();
  }
}
