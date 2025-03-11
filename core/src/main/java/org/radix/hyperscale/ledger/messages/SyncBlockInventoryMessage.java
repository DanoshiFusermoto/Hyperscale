package org.radix.hyperscale.ledger.messages;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

import org.radix.hyperscale.Constants;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.network.messages.Message;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.SerializerId2;
import org.radix.hyperscale.serialization.DsonOutput.Output;
import org.radix.hyperscale.utils.Numbers;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@SerializerId2("ledger.messages.block.sync.inv")
public final class SyncBlockInventoryMessage extends Message
{
	@JsonProperty("inventory")
	@DsonOutput(Output.ALL)
	@JsonDeserialize(as=LinkedHashSet.class)
	private Set<Hash> inventory;
	
	@JsonProperty("response_seq")
	@DsonOutput(Output.ALL)
	private long responseSeq;
	
	SyncBlockInventoryMessage()
	{
		super();
	}

	public SyncBlockInventoryMessage(final long responseSeq)
	{
		super();
		
		this.responseSeq = responseSeq;
	}

	public SyncBlockInventoryMessage(final long responseSeq, final Collection<Hash> inventory)
	{
		this(responseSeq);
		
		Objects.requireNonNull(inventory, "Inventory is null");
		Numbers.greaterThan(inventory.size(), Constants.MAX_BROADCAST_INVENTORY_ITEMS, "Too many inventory items");
		if (inventory.isEmpty())
			throw new IllegalArgumentException("Inventory is empty");
		
		this.inventory = new LinkedHashSet<Hash>(inventory);
	}
	
	public long getResponseSeq()
	{
		return this.responseSeq;
	}
	
	public Set<Hash> getInventory()
	{
		if (this.inventory == null)
			return Collections.emptySet();
		
		return Collections.unmodifiableSet(this.inventory);
	}
}
