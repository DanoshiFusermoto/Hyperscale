package org.radix.hyperscale.network;

import org.radix.hyperscale.network.messages.Message;

public interface MessageProcessor<T extends Message>
{
	public void process(T message, AbstractConnection connection);
}
