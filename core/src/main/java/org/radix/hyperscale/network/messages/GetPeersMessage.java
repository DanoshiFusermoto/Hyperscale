package org.radix.hyperscale.network.messages;

import org.radix.hyperscale.serialization.SerializerId2;

@SerializerId2("network.message.getpeers")
public final class GetPeersMessage extends Message
{
	public GetPeersMessage()
	{
		super();
	}
}
