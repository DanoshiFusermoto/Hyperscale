package org.radix.hyperscale.exceptions;

@SuppressWarnings("serial")
public class QueueFullException extends IllegalStateException
{
	public QueueFullException() 
	{
		super();
	}

	public QueueFullException(final String message)
	{
		super(message);
	}

	public QueueFullException(final Throwable throwable)
	{
		super(throwable);
	}

	public QueueFullException(final String message, final Throwable throwable)
	{
		super(message, throwable);
	}
}
