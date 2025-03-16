package org.radix.hyperscale.ledger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.eclipse.collections.api.factory.Maps;
import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.set.MutableSet;
import org.radix.hyperscale.Context;
import org.radix.hyperscale.concurrency.MonitoredReadWriteLock;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.ledger.exceptions.LockException;
import org.radix.hyperscale.ledger.exceptions.PrimitiveLockException;
import org.radix.hyperscale.ledger.exceptions.PrimitiveUnlockException;
import org.radix.hyperscale.ledger.exceptions.StateLockException;
import org.radix.hyperscale.ledger.exceptions.StateUnlockException;
import org.radix.hyperscale.ledger.primitives.Atom;
import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;
import org.radix.hyperscale.utils.Numbers;

public final class StateAccumulator implements LedgerProvider {
  private static final Logger stateAccumulatorLog = Logging.getLogger("stateaccumulator");

  private final Context context;

  private final String name;
  private final LedgerProvider provider;
  private final Map<StateAddress, PendingAtom> locked;
  private final ReentrantReadWriteLock lock;

  StateAccumulator(final String name, final StateAccumulator accumulator) {
    accumulator.lock.readLock().lock();
    try {
      this.name = name;
      this.context = accumulator.context;
      this.provider = accumulator.provider;
      this.locked = Maps.mutable.ofMap(accumulator.locked);
      this.lock =
          new MonitoredReadWriteLock(
              this.context.getName() + " State Accumulator '" + name + "' Lock", true);
    } finally {
      accumulator.lock.readLock().unlock();
    }
  }

  StateAccumulator(final Context context, final String name, final LedgerProvider provider) {
    Objects.requireNonNull(context, "Context is null");
    Objects.requireNonNull(provider, "Ledger provider is null");
    Objects.requireNonNull(name, "Name is null");
    Numbers.isZero(name.length(), "Name is empty");

    this.name = name;
    this.context = context;
    this.provider = provider;
    this.locked = Maps.mutable.ofInitialCapacity(1 << 12);
    this.lock =
        new MonitoredReadWriteLock(
            this.context.getName() + " State Accumulator '" + name + "' Lock", true);
  }

  public void reset() {
    this.lock.writeLock().lock();
    try {
      this.locked.clear();
    } finally {
      this.lock.writeLock().unlock();
    }
  }

  @Override
  public boolean has(final StateAddress address) throws IOException {
    Objects.requireNonNull(address, "State address is null");

    this.lock.readLock().lock();
    try {
      return this.provider.has(address);
    } finally {
      this.lock.readLock().unlock();
    }
  }

  @Override
  public Substate get(final StateAddress address) throws IOException {
    Objects.requireNonNull(address, "State address is null");

    // TODO returns committed values only, perhaps want this to return pending stuff?
    return this.provider.get(address);
  }

  public List<StateAddress> locked() {
    this.lock.readLock().lock();
    try {
      return new ArrayList<StateAddress>(this.locked.keySet());
    } finally {
      this.lock.readLock().unlock();
    }
  }

  void lock(final StateAccumulator other) {
    Objects.requireNonNull(other, "State accumulator is null");

    this.lock.writeLock().lock();
    try {
      other.lock.readLock().lock();
      try {
        this.locked.putAll(other.locked);
      } finally {
        other.lock.readLock().unlock();
      }
    } finally {
      this.lock.writeLock().unlock();
    }
  }

  void lock(final Collection<PendingAtom> pendingAtoms) throws LockException {
    Objects.requireNonNull(pendingAtoms, "Pending atoms is null");

    if (pendingAtoms.isEmpty()) return;

    this.lock.writeLock().lock();
    try {
      for (PendingAtom pendingAtom : pendingAtoms) lock(pendingAtom);
    } finally {
      this.lock.writeLock().unlock();
    }
  }

  void lock(final PendingAtom pendingAtom) throws LockException {
    Objects.requireNonNull(pendingAtom, "Pending atom is null");

    final List<StateAddress> stateAddresses = pendingAtom.getStateAddresses(StateLockMode.WRITE);
    if (stateAccumulatorLog.hasLevel(Logging.DEBUG))
      stateAccumulatorLog.debug(
          this.context.getName() + ": " + this.name + " -> Locking state in " + pendingAtom);

    this.lock.writeLock().lock();
    try {
      final PendingAtom locked = this.locked.get(pendingAtom.getAddress());
      if (locked != null) throw new PrimitiveLockException(Atom.class, pendingAtom.getHash());

      lockable(stateAddresses, pendingAtom);

      for (int i = 0; i < stateAddresses.size(); i++) {
        final StateAddress stateAddress = stateAddresses.get(i);
        this.locked.put(stateAddress, pendingAtom);

        if (stateAccumulatorLog.hasLevel(Logging.DEBUG))
          stateAccumulatorLog.debug(
              this.context.getName()
                  + ": "
                  + this.name
                  + " -> Locked state "
                  + stateAddress
                  + " "
                  + " via "
                  + pendingAtom);
      }
    } finally {
      this.lock.writeLock().unlock();
    }
  }

  void unlock(final Collection<PendingAtom> pendingAtoms) throws LockException {
    Objects.requireNonNull(pendingAtoms, "Pending atoms is null");

    if (pendingAtoms.isEmpty()) return;

    this.lock.writeLock().lock();
    try {
      for (PendingAtom pendingAtom : pendingAtoms) unlock(pendingAtom);
    } finally {
      this.lock.writeLock().unlock();
    }
  }

  void unlock(final PendingAtom pendingAtom) throws LockException {
    Objects.requireNonNull(pendingAtom, "Pending atom is null");

    final List<StateAddress> stateAddresses = pendingAtom.getStateAddresses(StateLockMode.WRITE);
    this.lock.writeLock().lock();
    try {
      if (stateAccumulatorLog.hasLevel(Logging.DEBUG))
        stateAccumulatorLog.debug(
            this.context.getName() + ": " + this.name + " -> Unlocking state in " + pendingAtom);

      unlockable(stateAddresses, pendingAtom);

      if (this.locked.containsKey(pendingAtom.getAddress()) == false)
        throw new PrimitiveUnlockException(Atom.class, pendingAtom.getHash());

      for (int i = 0; i < stateAddresses.size(); i++) {
        final StateAddress stateAddress = stateAddresses.get(i);
        if (this.locked.remove(stateAddress, pendingAtom) == false)
          throw new StateUnlockException(stateAddress, pendingAtom.getHash());

        if (stateAccumulatorLog.hasLevel(Logging.DEBUG))
          stateAccumulatorLog.debug(
              this.context.getName()
                  + ": "
                  + this.name
                  + " Unlocked state "
                  + stateAddress
                  + " via "
                  + pendingAtom);
      }
    } finally {
      this.lock.writeLock().unlock();
    }
  }

  boolean lockable(final StateAddress address, final PendingAtom pendingAtom) {
    Objects.requireNonNull(address, "State address is null");

    this.lock.readLock().lock();
    try {
      if (this.locked.containsKey(pendingAtom.getAddress())) return false;

      if (this.locked.containsKey(address)) return false;

      return true;
    } finally {
      this.lock.readLock().unlock();
    }
  }

  void lockable(final List<StateAddress> stateAddresses, final PendingAtom pendingAtom)
      throws StateLockException {
    Objects.requireNonNull(stateAddresses, "State addresses is null");

    this.lock.readLock().lock();
    try {
      for (int i = 0; i < stateAddresses.size(); i++) {
        final StateAddress stateAddress = stateAddresses.get(i);
        final PendingAtom lockedBy = this.locked.get(stateAddress);
        if (lockedBy != null)
          throw new StateLockException(stateAddress, pendingAtom.getHash(), lockedBy.getHash());
      }
    } finally {
      this.lock.readLock().unlock();
    }
  }

  void unlockable(final List<StateAddress> stateAddresses, final PendingAtom pendingAtom)
      throws StateUnlockException {
    Objects.requireNonNull(stateAddresses, "State addresses is null");

    this.lock.readLock().lock();
    try {
      for (int i = 0; i < stateAddresses.size(); i++) {
        final StateAddress stateAddress = stateAddresses.get(i);
        final PendingAtom lockedBy = this.locked.get(stateAddress);
        if (pendingAtom.equals(lockedBy) == false)
          throw new StateUnlockException(stateAddress, pendingAtom.getHash());
      }
    } finally {
      this.lock.readLock().unlock();
    }
  }

  boolean anyLocked(final PendingAtom pendingAtom) {
    Objects.requireNonNull(pendingAtom, "Pending atom is null");
    return anyLocked(pendingAtom.getStateAddresses(null));
  }

  boolean anyLocked(final List<StateAddress> stateAddresses) {
    Objects.requireNonNull(stateAddresses, "State addresses is null");

    this.lock.readLock().lock();
    try {
      for (int i = 0; i < stateAddresses.size(); i++) {
        final StateAddress stateAddress = stateAddresses.get(i);
        if (this.locked.containsKey(stateAddress)) return true;
      }

      return false;
    } finally {
      this.lock.readLock().unlock();
    }
  }

  boolean isLocked(final PendingAtom pendingAtom) {
    Objects.requireNonNull(pendingAtom, "Pending atom is null");

    this.lock.readLock().lock();
    try {
      if (this.locked.containsKey(pendingAtom.getAddress()) == false) return false;

      final List<StateAddress> stateAddresses = pendingAtom.getStateAddresses(null);
      for (int i = 0; i < stateAddresses.size(); i++) {
        final StateAddress stateAddress = stateAddresses.get(i);
        if (isLockedBy(stateAddress, pendingAtom)) return true;
      }

      return false;
    } finally {
      this.lock.readLock().unlock();
    }
  }

  boolean isLockedBy(final StateAddress address, final PendingAtom pendingAtom) {
    Objects.requireNonNull(pendingAtom, "Pending atom is null");
    Objects.requireNonNull(address, "State address is null");

    this.lock.readLock().lock();
    try {
      if (pendingAtom.equals(this.locked.get(address))) return true;

      return false;
    } finally {
      this.lock.readLock().unlock();
    }
  }

  boolean isLocked(final StateAddress address) {
    Objects.requireNonNull(address, "State address is null");

    this.lock.readLock().lock();
    try {
      if (this.locked.containsKey(address)) return true;

      return false;
    } finally {
      this.lock.readLock().unlock();
    }
  }

  public PendingAtom getLockedBy(StateAddress address) {
    Objects.requireNonNull(address, "State address is null");

    this.lock.readLock().lock();
    try {
      return this.locked.get(address);
    } finally {
      this.lock.readLock().unlock();
    }
  }

  Set<PendingAtom> getPendingAtoms() {
    this.lock.readLock().lock();
    try {
      final MutableSet<PendingAtom> pendingAtoms =
          Sets.mutable.ofInitialCapacity(this.locked.size());
      this.locked.forEach(
          (sa, pa) -> {
            if (pa.getAddress().equals(sa)) pendingAtoms.add(pa);
          });
      return pendingAtoms.asUnmodifiable();
    } finally {
      this.lock.readLock().unlock();
    }
  }

  public int numPendingAtoms() {
    return getPendingAtoms().size();
  }

  public int numLocked(boolean exclusive) {
    return this.locked.size();
  }

  LedgerProvider getProvider() {
    return this.provider;
  }

  public Hash checksum() {
    this.lock.readLock().lock();
    try {
      if (this.locked.isEmpty()) return Hash.ZERO;

      List<StateAddress> locked = new ArrayList<StateAddress>(this.locked.keySet());
      Collections.sort(locked);
      return locked.stream().map(StateAddress::getHash).reduce((a, b) -> Hash.hash(a, b)).get();
    } finally {
      this.lock.readLock().unlock();
    }
  }

  public String name() {
    return this.name;
  }
}
