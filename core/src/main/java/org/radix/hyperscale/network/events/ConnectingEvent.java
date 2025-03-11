package org.radix.hyperscale.network.events;

import org.radix.hyperscale.network.AbstractConnection;

public final class ConnectingEvent extends ConnectionEvent
{
	public ConnectingEvent(final AbstractConnection connection)
	{
		super(connection);
	}
}
