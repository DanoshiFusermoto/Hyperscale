package org.radix.hyperscale.crypto;

@SuppressWarnings("serial")
public class CryptoException extends Exception 
{
	public CryptoException () 
	{ 
		super (); 
	}

	public CryptoException(final Throwable throwable) 
	{ 
		super (throwable); 
	}

	public CryptoException(final String message) 
	{ 
		super (message); 
	}

	public CryptoException(final String message, Throwable throwable) 
	{ 
		super (message, throwable); 
	}
}
