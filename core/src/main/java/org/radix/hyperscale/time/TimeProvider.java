package org.radix.hyperscale.time;

public interface TimeProvider
{
	/**
	 * Returns a corrected System time
	 *
	 * @return
	 */
	public long getSystemTime();

	/**
	 * Returns a corrected ledger UTC time in seconds
	 *
	 * @return
	 */
	public int getLedgerTimeSeconds();

	/**
	 * Returns a corrected ledger UTC time in milliseconds
	 *
	 * @return
	 */
	public long getLedgerTimeMS();
	
	/**
	 * Returns true if time provider uses a synchronized time
	 *
	 * @return
	 */
	public boolean isSynchronized();
}
