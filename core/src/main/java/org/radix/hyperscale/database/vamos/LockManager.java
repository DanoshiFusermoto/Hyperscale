package org.radix.hyperscale.database.vamos;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;

public class LockManager {
  private static final Logger vamosLog = Logging.getLogger("vamos");
  private static final int NUM_LOCK_STRIPES = 256;

  private class LockPool<T> {
    private final Queue<Lock<T>> pool;

    LockPool(final int maxSize) {
      this.pool = new ArrayBlockingQueue<>(maxSize, false);
    }

    Lock<T> acquire(final T key) {
      Lock<T> lock = this.pool.poll();
      if (lock == null) return new Lock<>(key);

      lock.reset(key);
      return lock;
    }

    void release(final Lock<T> lock) {
      // will return false if full, which is fine
      this.pool.offer(lock);
    }
  }

  private final Environment environment;

  private final LockPool<InternalKey> keyLockPools[];
  private final Map<InternalKey, Lock<InternalKey>> keyLocks[];
  private final LockPool<IndexNodeID> indexNodeLockPools[];
  private final Map<IndexNodeID, Lock<IndexNodeID>> indexNodeLocks[];

  // Tracking metrics
  private final AtomicLong keyLockCount = new AtomicLong(0);
  private final AtomicLong nodeLockCount = new AtomicLong(0);
  private final AtomicLong totalKeyLockWaitTime = new AtomicLong(0);
  private final AtomicLong totalNodeLockWaitTime = new AtomicLong(0);

  @SuppressWarnings("unchecked")
  LockManager(final Environment environment) {
    this.environment = environment;

    this.keyLockPools = new LockPool[NUM_LOCK_STRIPES];
    this.indexNodeLockPools = new LockPool[NUM_LOCK_STRIPES];

    this.keyLocks = new HashMap[NUM_LOCK_STRIPES];
    this.indexNodeLocks = new HashMap[NUM_LOCK_STRIPES];
    for (int i = 0; i < NUM_LOCK_STRIPES; i++) {
      this.keyLocks[i] = new HashMap<>(1 << 8);
      this.keyLockPools[i] = new LockPool<>(1 << 8);
      this.indexNodeLocks[i] = new HashMap<>(1 << 8);
      this.indexNodeLockPools[i] = new LockPool<>(1 << 8);
    }
  }

  // Metrics retrieval methods
  public long getTotalKeyLockWaitTime() {
    return this.totalKeyLockWaitTime.get() / 1_000_000;
  }

  public long getKeyLockCount() {
    return this.keyLockCount.get();
  }

  public double getAverageKeyLockWaitTime() {
    long count = this.keyLockCount.get();
    return count > 0 ? (double) (this.totalKeyLockWaitTime.get() / 1_000_000) / count : 0;
  }

  public long getTotalNodeLockWaitTime() {
    return this.totalNodeLockWaitTime.get() / 1_000_000;
  }

  public long getNodeLockCount() {
    return this.nodeLockCount.get();
  }

  public double getAverageNodeLockWaitTime() {
    long count = this.nodeLockCount.get();
    return count > 0 ? (double) (this.totalNodeLockWaitTime.get() / 1_000_000) / count : 0;
  }

  private int stripe(final InternalKey key) {
    return (key.hashCode() & Integer.MAX_VALUE) % NUM_LOCK_STRIPES;
  }

  private int stripe(final IndexNodeID indexNodeID) {
    return (indexNodeID.hashCode() & Integer.MAX_VALUE) % NUM_LOCK_STRIPES;
  }

  boolean isLocked(final InternalKey key) {
    Map<InternalKey, Lock<InternalKey>> stripe = this.keyLocks[stripe(key)];
    synchronized (stripe) {
      Lock<InternalKey> lock = stripe.get(key);
      return lock != null;
    }
  }

  boolean isLockedBy(final InternalKey key, final Transaction transaction) {
    Map<InternalKey, Lock<InternalKey>> stripe = this.keyLocks[stripe(key)];
    synchronized (stripe) {
      Lock<InternalKey> lock = stripe.get(key);
      if (lock == null) return false;

      return lock.hasLock(transaction);
    }
  }

  void lock(
      final InternalKey key, final Transaction transaction, final long time, final TimeUnit unit)
      throws InterruptedException, LockInternalKeyTimeoutException {
    Lock<InternalKey> lock;
    int stripeIndex = stripe(key);
    Map<InternalKey, Lock<InternalKey>> stripe = this.keyLocks[stripeIndex];

    final long lockStartTime = System.nanoTime();
    try {
      synchronized (stripe) {
        lock = stripe.get(key);
        if (lock == null || lock.isStale()) {
          if (vamosLog.hasLevel(Logging.DEBUG)) {
            if (lock == null)
              vamosLog.debug(
                  "Created new lock for "
                      + key
                      + " on database '"
                      + this.environment.getDatabase(key.getDatabaseID()).getName()
                      + "'");
            else if (lock.isStale())
              vamosLog.debug(
                  "Replacing stale lock for "
                      + key
                      + " on database '"
                      + this.environment.getDatabase(key.getDatabaseID()).getName()
                      + "'");
          }

          lock = LockManager.this.keyLockPools[stripeIndex].acquire(key);
          stripe.put(key, lock);
        }

        lock.signal(transaction);
      }

      if (lock.tryLock(transaction, time, unit) == false)
        throw new LockInternalKeyTimeoutException(
            key, this.environment.getDatabase(key.getDatabaseID()), transaction);

      if (vamosLog.hasLevel(Logging.DEBUG))
        vamosLog.debug(
            "Locked index key "
                + key
                + " on database '"
                + this.environment.getDatabase(key.getDatabaseID()).getName()
                + "' to transaction "
                + transaction);
    } finally {
      this.keyLockCount.incrementAndGet();
      this.totalKeyLockWaitTime.addAndGet(System.nanoTime() - lockStartTime);
    }
  }

  void unlock(final InternalKey key, final Transaction transaction) {
    int stripeIndex = stripe(key);
    Map<InternalKey, Lock<InternalKey>> stripe = this.keyLocks[stripeIndex];
    synchronized (stripe) {
      Lock<InternalKey> lock = stripe.get(key);
      if (lock == null)
        throw new IllegalStateException(
            "Lock for key "
                + key
                + " on database '"
                + this.environment.getDatabase(key.getDatabaseID()).getName()
                + "' is null");

      lock.unlock(transaction);

      if (lock.isStale()) {
        if (vamosLog.hasLevel(Logging.DEBUG))
          vamosLog.debug(
              "Removed stale lock "
                  + key
                  + " on database '"
                  + this.environment.getDatabase(key.getDatabaseID()).getName()
                  + "' in transaction "
                  + transaction);

        LockManager.this.keyLockPools[stripeIndex].release(lock);
        stripe.remove(key);
      }
    }

    if (vamosLog.hasLevel(Logging.DEBUG))
      vamosLog.debug(
          "Unlocked key "
              + key
              + " on database '"
              + this.environment.getDatabase(key.getDatabaseID()).getName()
              + "' in transaction "
              + transaction);
  }

  boolean isLocked(final IndexNodeID indexNodeID) {
    Map<IndexNodeID, Lock<IndexNodeID>> stripe = this.indexNodeLocks[stripe(indexNodeID)];
    synchronized (stripe) {
      Lock<IndexNodeID> lock = stripe.get(indexNodeID);
      return lock != null;
    }
  }

  boolean isLockedBy(final IndexNodeID indexNodeID, final Transaction transaction) {
    Map<IndexNodeID, Lock<IndexNodeID>> stripe = this.indexNodeLocks[stripe(indexNodeID)];
    synchronized (stripe) {
      Lock<IndexNodeID> lock = stripe.get(indexNodeID);
      if (lock == null) return false;

      return lock.hasLock(transaction);
    }
  }

  boolean lock(
      final IndexNodeID indexNodeID,
      final Transaction transaction,
      final long time,
      final TimeUnit unit)
      throws InterruptedException, LockIndexNodeTimeoutException {
    Lock<IndexNodeID> lock;
    int stripeIndex = stripe(indexNodeID);
    Map<IndexNodeID, Lock<IndexNodeID>> stripe = this.indexNodeLocks[stripeIndex];

    final long lockStartTime = System.nanoTime();
    try {
      synchronized (stripe) {
        lock = stripe.get(indexNodeID);
        if (lock == null || lock.isStale()) {
          if (vamosLog.hasLevel(Logging.DEBUG)) {
            if (lock == null) vamosLog.debug("Created new lock for index node " + indexNodeID);
            else if (lock.isStale())
              vamosLog.debug("Replacing stale lock for index node " + indexNodeID);
          }

          lock = LockManager.this.indexNodeLockPools[stripeIndex].acquire(indexNodeID);
          stripe.put(indexNodeID, lock);
        }

        lock.signal(transaction);
      }

      if (lock.tryLock(transaction, time, unit) == false) {
        if (time != 0) throw new LockIndexNodeTimeoutException(indexNodeID, transaction);

        return false;
      }

      if (vamosLog.hasLevel(Logging.DEBUG))
        vamosLog.debug("Locked index node " + indexNodeID + " in transaction " + transaction);

      return true;
    } finally {
      this.nodeLockCount.incrementAndGet();
      this.totalNodeLockWaitTime.addAndGet(System.nanoTime() - lockStartTime);
    }
  }

  void unlock(final IndexNodeID indexNodeID, final Transaction transaction) {
    int stripeIndex = stripe(indexNodeID);
    Map<IndexNodeID, Lock<IndexNodeID>> stripe = this.indexNodeLocks[stripeIndex];
    synchronized (stripe) {
      Lock<IndexNodeID> lock = stripe.get(indexNodeID);
      if (lock == null)
        throw new IllegalStateException("Lock for index node " + indexNodeID + " is null");

      lock.unlock(transaction);

      if (lock.isStale()) {
        if (vamosLog.hasLevel(Logging.DEBUG))
          vamosLog.debug(
              "Removed stale lock for index node "
                  + indexNodeID
                  + " in transaction "
                  + transaction);

        LockManager.this.indexNodeLockPools[stripeIndex].release(lock);
        stripe.remove(indexNodeID);
      }
    }

    if (vamosLog.hasLevel(Logging.DEBUG))
      vamosLog.debug("Unlocked index node " + indexNodeID + " in transaction " + transaction);
  }
}
