package org.radix.hyperscale.network.events;

import java.util.Objects;

import org.radix.hyperscale.events.Event;
import org.radix.hyperscale.network.AbstractConnection;

abstract class ConnectionEvent implements Event 
{
	private final AbstractConnection connection;
	
	protected ConnectionEvent(final AbstractConnection connection) 
	{
		super();
		
		this.connection = Objects.requireNonNull(connection, "Connection is null");
	}

	public AbstractConnection getConnection()
	{ 
		return this.connection; 
	}
	
	@Override
	public String toString()
	{
		return super.toString()+" "+this.connection.toString();
	}
}
