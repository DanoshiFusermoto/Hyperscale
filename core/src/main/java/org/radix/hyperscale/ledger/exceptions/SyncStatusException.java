package org.radix.hyperscale.ledger.exceptions;

// TODO this could be temporary if pending atoms/state is managed accordingly 
public final class SyncStatusException extends IllegalStateException
{
	/**
	 * 
	 */
	private static final long serialVersionUID = -2478911398982015885L;

	public SyncStatusException(String message)
	{
		super(message);
	}

	public SyncStatusException(String message, Throwable t)
	{
		super(message, t);
	}
}
