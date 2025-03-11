package org.radix.hyperscale.exceptions;

import java.util.Objects;

public class TerminationException extends Exception 
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 7203560549694305527L;
	
	private final Class<?> clazz;
	
	public TerminationException(final String message, final Throwable throwable)
	{
		super (message, throwable);
		
		this.clazz = null;
	}

	public TerminationException(final String message)
	{
		super(message);
		
		this.clazz = null;
	}

	public TerminationException(final Throwable throwable)
	{
		super (throwable);
		
		this.clazz = null;
	}

	public TerminationException(final Class<?> clazz)
	{
		super ();
		
		this.clazz = Objects.requireNonNull(clazz, "Termination class is null");
	}

	public TerminationException(final String message, final Throwable throwable, final Class<?> clazz)
	{
		super (message, throwable);
		
		this.clazz = Objects.requireNonNull(clazz, "Termination class is null");
	}

	public TerminationException(final String message, final Class<?> clazz)
	{
		super (message);
		
		this.clazz = Objects.requireNonNull(clazz, "Termination class is null");
	}

	public TerminationException(final Throwable throwable, final Class<?> clazz)
	{
		super (throwable);
		
		this.clazz = Objects.requireNonNull(clazz, "Termination class is null");
	}
	
	public Class<?> getClazz() 
	{ 
		return this.clazz; 
	}
}