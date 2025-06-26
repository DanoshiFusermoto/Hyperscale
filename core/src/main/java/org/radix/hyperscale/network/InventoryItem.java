package org.radix.hyperscale.network;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.radix.hyperscale.Constants;
import org.radix.hyperscale.common.Primitive;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.serialization.Serialization;
import org.radix.hyperscale.utils.Numbers;

public final class InventoryItem implements Comparable<InventoryItem>
{
	private final Hash hash;
	private final String type;
	private final Primitive primitive;
	private final long timestamp;
	
	private final int hashCode;
	private final Class<? extends Primitive> typeClass;
	
	
	public InventoryItem(final Class<? extends Primitive> type, final Hash item)
	{
		this(Serialization.getInstance().getIdForClass(Objects.requireNonNull(type, "Type is null")), item, null);
	}
	
	public InventoryItem(final String type, final Hash item)
	{
		this(type, item, null);
	}
	
	public InventoryItem(final Primitive primitive)
	{
		this(Serialization.getInstance().getIdForClass(Objects.requireNonNull(primitive, "Primitive is null").getClass()), primitive.getHash(), primitive);
	}
	
	@SuppressWarnings("unchecked")
	private InventoryItem(final String type, final Hash item, final Primitive primitive)
	{
		Objects.requireNonNull(item, "Item is null");
		Objects.requireNonNull(type, "Type is null");
		Numbers.isZero(type.length(), "Type is empty");
		
		this.hash = item;
		this.type = type;
		
		// Primitive can be null 
		this.primitive = primitive;
		if (this.primitive == null)
			this.typeClass = (Class<? extends Primitive>) Serialization.getInstance().getClassForId(type);
		else
			this.typeClass = this.primitive.getClass();
		
		final int prime = 31;
		int result = 1;
		result = prime * result + this.hash.hashCode();
		result = prime * result + this.type.hashCode();
		this.hashCode = result;
		
		this.timestamp = System.currentTimeMillis();
	}

	public Hash getHash()
	{
		return this.hash;
	}
	
	@Override
	public int hashCode() 
	{
		return this.hashCode;
	}

	@Override
	public boolean equals(final Object obj) 
	{
		if (null == obj)
			return false;
		
		if (this == obj)
			return true;
		
		if (obj instanceof InventoryItem other)
		{
			if (this.hashCode == other.hashCode)
			{
				if (other.hash.equals(this.hash) && other.type.equals(this.type))
					return true;
			}
		}
		
		return false;
	}

	public Class<? extends Primitive> getType()
	{
		return this.typeClass;
	}
	
	boolean isUrgent()
	{
		final TransportParameters transportParameters = this.typeClass.getAnnotation(TransportParameters.class);
		if (transportParameters == null)
			return false;
		
		return transportParameters.urgent();
	}
	
	int getWeight()
	{
		final TransportParameters transportParameters = this.typeClass.getAnnotation(TransportParameters.class);
		if (transportParameters == null)
			return 0;
		
		return transportParameters.weight();
	}
	
	boolean isStale()
	{
		return System.currentTimeMillis() - this.timestamp > TimeUnit.SECONDS.toMillis(Constants.PRIMITIVE_STALE) ? true : false;
	}

	public String getTypeName()
	{
		return this.type;
	}

	@SuppressWarnings("unchecked")
	public <T> T getPrimitive()
	{
		return (T) this.primitive;
	}
	
	@Override
	public String toString() 
	{
		return "[type = "+Serialization.getInstance().getClassForId(this.type).getName()+", hash = "+this.hash+"]";
	}

	@Override
	public int compareTo(final InventoryItem other)
	{
		final boolean ii1Urgent = this.isUrgent();
		final boolean ii2Urgent = other.isUrgent();
		if (ii1Urgent == true && ii2Urgent == false)
			return -1;
		if (ii1Urgent == false && ii2Urgent == true)
			return 1;
		
		final int ii1Weight = this.getWeight();
		final int ii2Weight = other.getWeight();
		return Integer.compare(ii1Weight, ii2Weight);
	}
}
