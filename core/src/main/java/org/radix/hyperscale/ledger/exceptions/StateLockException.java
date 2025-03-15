package org.radix.hyperscale.ledger.exceptions;

import java.util.Objects;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.ledger.StateAddress;

public final class StateLockException extends LockException {
  /** */
  private static final long serialVersionUID = 110356083142939115L;

  private final StateAddress address;
  private final Hash dependent;
  private final Hash holder;

  private StateLockException(String message, StateAddress address, Hash dependent, Hash holder) {
    super(message);

    Objects.requireNonNull(message, "Message is null");
    Objects.requireNonNull(address, "State address is null");
    Objects.requireNonNull(dependent, "Dependent is null");
    Hash.notZero(dependent, "Dependent hash is ZERO");

    if (dependent.equals(address.getHash()))
      throw new IllegalArgumentException("State and dependent are the same");

    this.address = address;
    this.dependent = dependent;
    this.holder = holder;
  }

  public StateLockException(StateAddress address, Hash dependent, Hash holder) {
    this(
        "State "
            + address
            + " required by "
            + Objects.requireNonNull(dependent)
            + " is locked by "
            + Objects.requireNonNull(holder),
        address,
        dependent,
        holder);
  }

  public StateAddress getAddress() {
    return this.address;
  }

  public Hash getDependent() {
    return this.dependent;
  }

  public Hash getHolder() {
    return this.holder;
  }
}
