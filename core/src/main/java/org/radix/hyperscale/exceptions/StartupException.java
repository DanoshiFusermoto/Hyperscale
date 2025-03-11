package org.radix.hyperscale.exceptions;

import java.util.Objects;

public class StartupException extends Exception 
{
	/**
	 * 
	 */
	private static final long serialVersionUID = -3096794654963649858L;
	
	private final Class<?> clazz;
	
	public StartupException(final String message, final Throwable throwable)
	{
		super (message, throwable);
		
		this.clazz = null;
	}

	public StartupException(final String message)
	{
		super(message);
		
		this.clazz = null;
	}

	public StartupException (final Throwable throwable)
	{
		super (throwable);
		
		this.clazz = null;
	}

	public StartupException(final Class<?> clazz)
	{
		super ();
		
		this.clazz = Objects.requireNonNull(clazz, "Startup class is null");
	}

	public StartupException(final String message, final Throwable throwable, final Class<?> clazz)
	{
		super (message, throwable);
		
		this.clazz = Objects.requireNonNull(clazz, "Startup class is null");
	}

	public StartupException(final String message, final Class<?> clazz)
	{
		super (message);
		
		this.clazz = Objects.requireNonNull(clazz, "Startup class is null");
	}

	public StartupException(final Throwable throwable, final Class<?> clazz)
	{
		super (throwable);
		
		this.clazz = Objects.requireNonNull(clazz, "Startup class is null");
	}
	
	public Class<?> getClazz() 
	{ 
		return this.clazz; 
	}
}