package org.radix.hyperscale.ledger.exceptions;

public abstract class LockException extends Exception 
{
	/**
	 * 
	 */
	private static final long serialVersionUID = -3504710223452288089L;

	LockException(String message)
	{
		super(message);
	}

	LockException(String message, Throwable ex)
	{
		super(message, ex);
	}

	LockException(Throwable ex)
	{
		super(ex);
	}
}
