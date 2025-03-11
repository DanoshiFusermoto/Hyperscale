package org.radix.hyperscale.network.events;

import org.radix.hyperscale.network.AbstractConnection;

public final class DisconnectingEvent extends ConnectionEvent
{
	public DisconnectingEvent(final AbstractConnection connection)
	{
		super(connection);
	}
}
