package org.radix.hyperscale.database;

import java.io.IOException;

@SuppressWarnings("serial")
public class DatabaseException extends IOException
{
	public DatabaseException() 
	{ 
		super (); 
	}

	public DatabaseException(final String message, final Throwable throwable) 
	{ 
		super(message, throwable); 
	}

	public DatabaseException(final String message) 
	{ 
		super(message); 
	}

	public DatabaseException(final Throwable throwable) 
	{ 
		super (throwable); 
	}
}
