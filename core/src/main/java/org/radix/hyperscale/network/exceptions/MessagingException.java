package org.radix.hyperscale.network.exceptions;

@SuppressWarnings("serial")
public class MessagingException extends Exception 
{
	public MessagingException() 
	{ 
		super (); 
	}

	public MessagingException(final String message, final Throwable throwable) 
	{ 
		super (message, throwable); 
	}

	public MessagingException(final String message) 
	{ 
		super (message); 
	}

	public MessagingException(final Throwable throwable) 
	{ 
		super (throwable); 
	}
}