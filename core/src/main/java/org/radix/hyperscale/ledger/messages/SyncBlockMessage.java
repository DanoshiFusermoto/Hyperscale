package org.radix.hyperscale.ledger.messages;

import java.util.Objects;

import org.radix.hyperscale.ledger.Block;
import org.radix.hyperscale.network.messages.Message;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.SerializerId2;
import org.radix.hyperscale.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.messages.block.sync")
public class SyncBlockMessage extends Message
{
	@JsonProperty("block")
	@DsonOutput(Output.ALL)
	private Block block;

	SyncBlockMessage()
	{
		// Serializer only
	}

	public SyncBlockMessage(Block block)
	{
		super();

		this.block = Objects.requireNonNull(block, "Block is null");
	}
	
	public Block getBlock()
	{
		return this.block;
	}
}

