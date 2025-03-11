package org.radix.hyperscale.network.messages;

import org.radix.hyperscale.node.Node;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.SerializerId2;
import org.radix.hyperscale.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("network.message.ping")
public final class PeerPingMessage extends NodeMessage
{
	@JsonProperty("nonce")
	@DsonOutput(Output.ALL)
	private long nonce;
	
	PeerPingMessage()
	{
		super();
	}

	public PeerPingMessage(final Node node, final long nonce)
	{
		super(node);
		
		this.nonce = nonce;
	}

	public long getNonce() 
	{ 
		return this.nonce; 
	}
}
