package org.radix.hyperscale.ledger.messages;

import java.util.Objects;

import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.network.messages.Message;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.SerializerId2;
import org.radix.hyperscale.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.messages.block.sync.get")
public class GetSyncBlockMessage extends Message
{
	@JsonProperty("block")
	@DsonOutput(Output.ALL)
	private Hash block;

	GetSyncBlockMessage()
	{
		// Serializer only
	}

	public GetSyncBlockMessage(Hash block)
	{
		super();

		this.block = Objects.requireNonNull(block, "Block is null");
	}
	
	public Hash getBlock()
	{
		return this.block;
	}
}
