package org.radix.hyperscale.time;

public interface TimeProvider {
  /**
   * Returns a corrected System time
   *
   * @return
   */
  long getSystemTime();

  /**
   * Returns a corrected ledger UTC time in seconds
   *
   * @return
   */
  int getLedgerTimeSeconds();

  /**
   * Returns a corrected ledger UTC time in milliseconds
   *
   * @return
   */
  long getLedgerTimeMS();

  /**
   * Returns true if time provider uses a synchronized time
   *
   * @return
   */
  boolean isSynchronized();
}
