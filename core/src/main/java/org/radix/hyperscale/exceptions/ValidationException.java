package org.radix.hyperscale.exceptions;

@SuppressWarnings("serial")
public class ValidationException extends Exception
{
	private final Object object;
	
	public ValidationException(final Throwable cause)
	{
		this(null, cause);
	}

	public ValidationException(final String message, final Throwable cause)
	{
		this(null, message, cause);
	}

	public ValidationException(final String message)
	{
		this(null, message);
	}
	
	public ValidationException(final Object object, final Throwable cause)
	{
		super(cause);
		
		this.object = object;
	}

	public ValidationException(final Object object, final String message, final Throwable cause)
	{
		super(message, cause);

		this.object = object;
	}

	public ValidationException(final Object object, final String message)
	{
		super(message);

		this.object = object;
	}
	
	@SuppressWarnings("unchecked")
	public <T> T getObject()
	{
		return (T) this.object;
	}

}
