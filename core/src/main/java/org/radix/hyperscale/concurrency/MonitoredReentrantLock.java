package org.radix.hyperscale.concurrency;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;

@SuppressWarnings("serial")
public final class MonitoredReentrantLock extends ReentrantLock implements MonitoredLock {
  private static final Logger locksLog = Logging.getLogger("locks");
  private static final long DEFAULT_LOCK_WARN_THRESHOLD_MS = 10;

  // Tracking metrics
  private final AtomicLong locksCount = new AtomicLong(0);
  private final AtomicLong totalLockTime = new AtomicLong(0);
  private final AtomicLong totalLockWaitTime = new AtomicLong(0);

  private final String label;
  private final long warningThresholdMS;

  public MonitoredReentrantLock(String label) {
    super();

    this.label = label;
    this.warningThresholdMS = DEFAULT_LOCK_WARN_THRESHOLD_MS;

    MonitoredLock.add(this);
  }

  public MonitoredReentrantLock(String label, boolean fair) {
    super(fair);

    this.label = label;
    this.warningThresholdMS = DEFAULT_LOCK_WARN_THRESHOLD_MS;

    MonitoredLock.add(this);
  }

  // Metrics retrieval methods
  public long getTotalLockWaitTime() {
    return this.totalLockWaitTime.get() / 1_000_000;
  }

  public long getLocksCount() {
    return this.locksCount.get();
  }

  public double getAverageLockWaitTime() {
    long count = this.locksCount.get();
    return count > 0 ? (double) (this.totalLockTime.get() / 1_000_000) / count : 0;
  }

  @Override
  public void lock() {
    // Check if the current thread already holds the lock
    boolean isFirstAcquisition = getHoldCount() == 0;

    long startWaitTime = System.nanoTime();
    long startLockTime = startWaitTime;

    try {
      super.lock();

      // Only track metrics for the first lock acquisition by this thread
      if (isFirstAcquisition) {
        // Record wait time
        long waitTime = (System.nanoTime() - startWaitTime);
        this.totalLockWaitTime.addAndGet(waitTime);

        // Increment distinct lock count
        this.locksCount.incrementAndGet();
      }

      // Track total lock time
      startLockTime = System.nanoTime();
    } finally {
      // Ensure lock time is tracked even if an exception occurs
      long lockTime = (System.nanoTime() - startLockTime);
      this.totalLockTime.addAndGet(lockTime);

      // Log warning if lock acquisition took too long
      long lockTimeMS = lockTime / 1_000_000;
      if (lockTimeMS > this.warningThresholdMS)
        locksLog.warn(
            "Lock acquisition took "
                + lockTime
                + "ms ( "
                + getLocksCount()
                + " / "
                + getAverageLockWaitTime()
                + "ms / "
                + getTotalLockWaitTime()
                + "ms )",
            new Exception("Lock acquisition stack trace"));
    }
  }

  @Override
  public void lockInterruptibly() throws InterruptedException {
    // Check if the current thread already holds the lock
    boolean isFirstAcquisition = getHoldCount() == 0;

    long startWaitTime = System.nanoTime();
    long startLockTime = startWaitTime;

    try {
      super.lockInterruptibly();

      // Only track metrics for the first lock acquisition by this thread
      if (isFirstAcquisition) {
        // Record wait time
        long waitTime = (System.nanoTime() - startWaitTime);
        this.totalLockWaitTime.addAndGet(waitTime);

        // Increment distinct lock count
        this.locksCount.incrementAndGet();
      }

      // Track total lock time
      startLockTime = System.nanoTime();
    } finally {
      // Ensure lock time is tracked even if an exception occurs
      long lockTime = (System.nanoTime() - startLockTime);
      this.totalLockTime.addAndGet(lockTime);

      // Log warning if lock acquisition took too long
      if ((lockTime / 1_000_000) > this.warningThresholdMS)
        locksLog.warn(
            "Lock acquisition took "
                + lockTime
                + "ms ("
                + getLocksCount()
                + " / "
                + getAverageLockWaitTime()
                + "ms / "
                + getTotalLockWaitTime()
                + ")",
            new Exception("Lock acquisition stack trace"));
    }
  }

  @Override
  public boolean tryLock(long timeout, TimeUnit unit) throws InterruptedException {
    return super.tryLock(timeout, unit);
  }

  @Override
  public String toString() {
    return this.label
        + " "
        + getClass().getSimpleName()
        + " "
        + getLocksCount()
        + " / "
        + getAverageLockWaitTime()
        + "ms | "
        + getTotalLockWaitTime()
        + "ms";
  }
}
