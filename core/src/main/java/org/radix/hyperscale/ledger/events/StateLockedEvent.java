package org.radix.hyperscale.ledger.events;

import java.util.Objects;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.events.Event;
import org.radix.hyperscale.ledger.StateAddress;

/**
 * Invoked when a {@link StateAddress} lock is attempted but it already locked by a {@link
 * StateAccumulator}
 */
public final class StateLockedEvent implements Event {
  private final Hash atom;
  private final StateAddress address;

  public StateLockedEvent(final Hash atom, final StateAddress address) {
    this.atom = Objects.requireNonNull(atom, "Atom is null");
    Hash.notZero(this.atom, "Atom is ZERO");
    this.address = Objects.requireNonNull(address, "State address is null");
  }

  public Hash getAtom() {
    return this.atom;
  }

  public StateAddress getAddress() {
    return this.address;
  }
}
