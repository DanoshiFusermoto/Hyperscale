package org.radix.hyperscale.ledger.exceptions;

import java.util.Objects;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.ledger.StateAddress;

public final class StateUnlockException extends LockException {
  /** */
  private static final long serialVersionUID = 110356083142939115L;

  private final StateAddress address;
  private final Hash dependent;

  private StateUnlockException(String message, StateAddress address, Hash dependent) {
    super(message);

    Objects.requireNonNull(message, "Message is null");
    Objects.requireNonNull(address, "State address is null");
    Objects.requireNonNull(dependent, "Dependent is null");
    Hash.notZero(dependent, "Dependent hash is ZERO");

    if (dependent.equals(address.getHash()))
      throw new IllegalArgumentException("State and dependent are the same");

    this.address = address;
    this.dependent = dependent;
  }

  public StateUnlockException(StateAddress address, Hash dependent) {
    this(
        "State " + address + " is NOT locked by " + Objects.requireNonNull(dependent),
        address,
        dependent);
  }

  public StateAddress getAddress() {
    return this.address;
  }

  public Hash getDependent() {
    return this.dependent;
  }
}
