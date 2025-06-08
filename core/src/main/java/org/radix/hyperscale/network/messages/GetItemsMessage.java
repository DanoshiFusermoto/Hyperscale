package org.radix.hyperscale.network.messages;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;

import org.eclipse.collections.api.multimap.Multimap;
import org.eclipse.collections.api.multimap.MutableMultimap;
import org.eclipse.collections.impl.factory.Multimaps;
import org.radix.hyperscale.common.Primitive;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.network.InventoryItem;
import org.radix.hyperscale.network.TransportParameters;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.Serialization;
import org.radix.hyperscale.serialization.SerializerId2;
import org.radix.hyperscale.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("gossip.items.get")
@TransportParameters(cache = true, priority = 10)
public final class GetItemsMessage extends Message
{
	@JsonProperty("inventory")
	@DsonOutput(Output.ALL)
	private HashMap<String, LinkedHashSet<Hash>> inventory;
	
	private volatile transient List<InventoryItem> inventoryItems = null;

	GetItemsMessage()
	{
		// Serializer only
	}

	public GetItemsMessage(final Class<? extends Primitive> type, final Collection<Hash> items)
	{
		super();

		Objects.requireNonNull(type, "Type is null");
		Objects.requireNonNull(items, "Items is null");
		if (items.isEmpty())
			throw new IllegalArgumentException("Items is empty");

		this.inventory = new HashMap<String, LinkedHashSet<Hash>>();
		for (final Hash item : items)
			this.inventory.computeIfAbsent(Serialization.getInstance().getIdForClass(type), t -> new LinkedHashSet<>(items.size())).add(item);
	}

	public GetItemsMessage(final Collection<InventoryItem> items)
	{
		super();

		Objects.requireNonNull(items, "Items is null");
		if (items.isEmpty())
			throw new IllegalArgumentException("Items is empty");

		this.inventory = new HashMap<String, LinkedHashSet<Hash>>(items.size());
		for (InventoryItem item : items)
			this.inventory.computeIfAbsent(Serialization.getInstance().getIdForClass(item.getType()), t -> new LinkedHashSet<>()).add(item.getHash());
	}
	
	public List<InventoryItem> asInventory()
	{
		synchronized(this)
		{
			if (this.inventory != null)
			{
				final List<InventoryItem> inventoryItems = new ArrayList<InventoryItem>(this.inventory.size());
				for (final Entry<String, LinkedHashSet<Hash>> items : this.inventory.entrySet())
				{
					for (final Hash item : items.getValue())
						inventoryItems.add(new InventoryItem(items.getKey(), item));
				}
				
				Collections.sort(inventoryItems);
				this.inventoryItems = Collections.unmodifiableList(inventoryItems);
			}
			
			return this.inventoryItems;
		}
	}

	public Multimap<Class<? extends Primitive>, Hash> getTyped()
	{
		final MutableMultimap<Class<? extends Primitive>, Hash> typed = Multimaps.mutable.list.empty();
		if (this.inventory != null && this.inventory.isEmpty() == false)
		{
			for (final Entry<String, LinkedHashSet<Hash>> items : this.inventory.entrySet())
			{
				final Class<?> clazz = Serialization.getInstance().getClassForId(items.getKey());
				for (final Hash item : items.getValue())
					typed.put(clazz.asSubclass(Primitive.class), item);
			}
		}
		
		return typed;
	}		
	
	public List<Hash> getTyped(final Class<? extends Primitive> type)
	{
		final String clazz = Serialization.getInstance().getIdForClass(type);
		if (this.inventory != null && this.inventory.isEmpty() == false && this.inventory.containsKey(clazz))
			return new ArrayList<Hash>(this.inventory.get(clazz));
		
		return Collections.emptyList();
	}	

	@Override
	public boolean isUrgent()
	{
		for (final String type : this.inventory.keySet())
		{
			final TransportParameters transportParameters = Serialization.getInstance().getClassForId(type).getAnnotation(TransportParameters.class);
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
		for (String type : this.inventory.keySet())
		{
			final TransportParameters transportParameters = Serialization.getInstance().getClassForId(type).getAnnotation(TransportParameters.class);
			if (transportParameters == null || transportParameters.urgent() == false)
				continue;
			
			priorityTotal += transportParameters.priority();
		}
		
		return priorityTotal;
	}
}
