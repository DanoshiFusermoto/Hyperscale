package org.radix.hyperscale.ledger.sme.exceptions;

public abstract class StateMachineException extends Exception
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1438632770245460661L;
	
	StateMachineException(final String message)
	{
		super(message);
	}

	StateMachineException(final Exception exception)
	{
		super(exception);
	}

	StateMachineException(final String message, final Exception exception)
	{
		super(message, exception);
	}
}
