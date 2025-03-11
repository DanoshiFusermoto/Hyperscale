package org.radix.hyperscale.network.messages;

import java.nio.BufferOverflowException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.eclipse.collections.api.multimap.Multimap;
import org.eclipse.collections.api.multimap.MutableMultimap;
import org.eclipse.collections.impl.factory.Multimaps;
import org.radix.hyperscale.Constants;
import org.radix.hyperscale.common.Primitive;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.network.InventoryItem;
import org.radix.hyperscale.network.TransportParameters;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.SerializerId2;
import org.radix.hyperscale.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@SerializerId2("gossip.items")
public final class ItemsMessage extends Message
{
	@JsonProperty("inventory")
	@DsonOutput(Output.ALL)
	@JsonDeserialize(as=ArrayList.class)
	private List<Primitive> inventory;
	
	private volatile transient List<InventoryItem> inventoryItems = null;
	
	public ItemsMessage()
	{
		// Serializer only
	}

	public ItemsMessage(final Primitive item)
	{
		this.inventory = Collections.singletonList(item);
	}

	public void add(final Primitive item)
	{
		Objects.requireNonNull(item, "Item is null");
		
		if (this.inventory == null) 
			this.inventory = new ArrayList<Primitive>();

		if (this.inventory.size() == Constants.MAX_FETCH_INVENTORY_ITEMS)
			throw new BufferOverflowException();
		
		this.inventory.add(item);
	}
	
	public boolean isAtCapacity()
	{
		return this.inventory.size() == Constants.MAX_FETCH_INVENTORY_ITEMS;
	}
	
	public List<InventoryItem> asInventory()
	{
		synchronized(this)
		{
			if (this.inventoryItems == null)
			{
				this.inventoryItems = new ArrayList<InventoryItem>(this.inventory.size());
				for (Primitive item : this.inventory)
					this.inventoryItems.add(new InventoryItem(item));
				
				this.inventoryItems = Collections.unmodifiableList(this.inventoryItems);
			}
			
			return this.inventoryItems;
		}
	}
	
	public Multimap<Class<? extends Primitive>, Hash> getTyped()
	{
		final MutableMultimap<Class<? extends Primitive>, Hash> items = Multimaps.mutable.list.empty();
		if (this.inventory != null && this.inventory.isEmpty() == false)
		{
			for (final Primitive item : this.inventory)
				items.put(item.getClass(), item.getHash());
		}
		
		return items;
	}	
	
	@Override
	public boolean isUrgent()
	{
		for(int i = 0 ; i < this.inventory.size() ; i++)
		{
			final Primitive primitive = this.inventory.get(i);
			final TransportParameters transportParameters = primitive.getClass().getAnnotation(TransportParameters.class);
			if (transportParameters == null || transportParameters.urgent() == false)
				continue;
			
			return true;
		}
		
		return super.isUrgent();
	}
	
	@Override
	public int getPriority()
	{
		if (this.inventory == null || this.inventory.isEmpty())
			return super.getPriority();

		int priorityTotal = 0;
		for(final Primitive item : this.inventory)
		{
			final TransportParameters transportParameters = item.getClass().getAnnotation(TransportParameters.class);
			if (transportParameters == null)
				continue;
			
			if (transportParameters.urgent())
				return Integer.MAX_VALUE;
			
			priorityTotal += transportParameters.priority();
		}
		
		return priorityTotal;
	}
}
