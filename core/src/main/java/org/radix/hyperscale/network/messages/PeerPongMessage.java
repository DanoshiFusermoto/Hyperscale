package org.radix.hyperscale.network.messages;

import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.SerializerId2;
import org.radix.hyperscale.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("network.message.pong")
public final class PeerPongMessage extends Message
{
	@JsonProperty("nonce")
	@DsonOutput(Output.ALL)
	private long nonce;

	PeerPongMessage()
	{
		super();

		this.nonce = 0l;
	}

	public PeerPongMessage(final long nonce)
	{
		this.nonce = nonce;
	}

	public long getNonce() 
	{ 
		return this.nonce; 
	}
}
