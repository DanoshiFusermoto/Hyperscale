package org.radix.hyperscale.ledger.sme.exceptions;

public final class StateMachinePreparationException extends StateMachineException
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 4176416467490930209L;

	public StateMachinePreparationException(String message)
	{
		super(message);
	}

	public StateMachinePreparationException(Exception exception)
	{
		super(exception);
	}

	public StateMachinePreparationException(String message, Exception exception)
	{
		super(message, exception);
	}
}
