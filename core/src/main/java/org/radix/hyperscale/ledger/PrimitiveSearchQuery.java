package org.radix.hyperscale.ledger;

import java.util.Objects;

import org.radix.hyperscale.common.BasicObject;
import org.radix.hyperscale.common.Primitive;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.Serialization;
import org.radix.hyperscale.serialization.SerializerId2;
import org.radix.hyperscale.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.search.query.primitve")
public final class PrimitiveSearchQuery extends BasicObject
{
	@JsonProperty("primitive")
	@DsonOutput(Output.ALL)
	private Hash primitive;

	@JsonProperty("type")
	@DsonOutput(Output.ALL)
	private String type;

	@JsonProperty("isolation")
	@DsonOutput(Output.ALL)
	private Isolation isolation;

	PrimitiveSearchQuery()
	{ 
		super();
	}

	public PrimitiveSearchQuery(final Hash primitive, final Class<? extends Primitive> type)
	{ 
		this(primitive, Serialization.getInstance().getIdForClass(Objects.requireNonNull(type, "Primitive type is null")), Isolation.COMMITTED);
	}
	
	public PrimitiveSearchQuery(final Hash primitive, final Class<? extends Primitive> type, final Isolation isolation)
	{ 
		this(primitive, Serialization.getInstance().getIdForClass(Objects.requireNonNull(type, "Primitive type is null")), isolation);
	}

	public PrimitiveSearchQuery(final Hash primitive, final String type, final Isolation isolation)
	{ 
		this.primitive = Objects.requireNonNull(primitive, "Primitive hash is null");
		this.type = Objects.requireNonNull(type, "Primitive type is null");
		this.isolation = Objects.requireNonNull(isolation, "Isolation type is null");
	}

	public Hash getQuery()
	{
		return this.primitive;
	}
	
	public Class<? extends Primitive> getType()
	{
		@SuppressWarnings("unchecked")
		Class<? extends Primitive> type = (Class<? extends Primitive>) Serialization.getInstance().getClassForId(this.type);
		if (type == null)
			throw new IllegalArgumentException(this.type+" is not a known registered class");
		
		return type;
	}

	public Isolation getIsolation()
	{
		return this.isolation;
	}

	@Override
	public int hashCode() 
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + this.primitive.hashCode();
		result = prime * result + this.isolation.hashCode();
		result = prime * result + this.type.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object object) 
	{
		if (object == null)
			return false;
		
		if (object == this)
			return true;
		
		if (object instanceof PrimitiveSearchQuery primitiveSearchQuery)
		{
			if (hashCode() != object.hashCode())
				return false;
			
			if (this.primitive.equals(primitiveSearchQuery.getQuery()) == false)
				return false;
			
			if (this.isolation.equals(primitiveSearchQuery.getIsolation()) == false)
				return false;

			if (this.type.equals(primitiveSearchQuery.type) == false)
				return false;

			return true;
		}
		
		return false;
	}

	@Override
	public String toString() 
	{
		return this.primitive+" "+this.type+" "+this.isolation;
	}
}