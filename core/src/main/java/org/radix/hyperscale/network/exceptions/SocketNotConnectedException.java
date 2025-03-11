package org.radix.hyperscale.network.exceptions;

import java.net.SocketException;

public class SocketNotConnectedException extends SocketException 
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
    /**
     * Constructs a new {@code SocketNotConnectedException} with the specified detail message.
     *
     * @param msg the detail message.
     */
	public SocketNotConnectedException(String msg) 
	{
		super(msg);
    }

    /**
     * Constructs a new {@code SocketNotConnectedException} with no detail message.
     */
    public SocketNotConnectedException() 
    {
    	super();
    }

}
