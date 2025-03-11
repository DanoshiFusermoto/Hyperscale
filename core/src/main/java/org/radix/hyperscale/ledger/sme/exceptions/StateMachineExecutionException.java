package org.radix.hyperscale.ledger.sme.exceptions;

public final class StateMachineExecutionException extends StateMachineException
{
	/**
	 * 
	 */
	private static final long serialVersionUID = -5735995224308181473L;

	public StateMachineExecutionException(String message)
	{
		super(message);
	}

	public StateMachineExecutionException(Exception exception)
	{
		super(exception);
	}

	public StateMachineExecutionException(String message, Exception exception)
	{
		super(message, exception);
	}
}
