package org.radix.hyperscale.ledger;

import java.util.Objects;

import org.radix.hyperscale.common.BasicObject;
import org.radix.hyperscale.common.Primitive;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.SerializerId2;
import org.radix.hyperscale.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.search.response.primitive")
public final class PrimitiveSearchResponse extends BasicObject
{
	@JsonProperty("query")
	@DsonOutput(Output.ALL)
	private PrimitiveSearchQuery query;

	@JsonProperty("result")
	@DsonOutput(Output.ALL)
	private Primitive result;
	
	PrimitiveSearchResponse()
	{ 
		super();
	}
	
	public PrimitiveSearchResponse(final PrimitiveSearchQuery query)
	{ 
		super();
		
		this.query = Objects.requireNonNull(query, "Primitive search query is null");
	}

	public PrimitiveSearchResponse(final PrimitiveSearchQuery query, final Primitive primitive)
	{ 
		super();
		
		this.query = Objects.requireNonNull(query, "Primitive search query is null");
		this.result = Objects.requireNonNull(primitive, "Primitive search result is null");
	}

	public PrimitiveSearchQuery getQuery()
	{
		return this.query;
	}
	
	@SuppressWarnings("unchecked")
	public <T extends Primitive> T getResult()
	{
		return (T) this.result;
	}

	@Override
	public int hashCode() 
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + (this.result == null ? 0 : this.result.hashCode());
		result = prime * result + this.query.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object object) 
	{
		if (object == null)
			return false;
		
		if (object == this)
			return true;
		
		if (object instanceof PrimitiveSearchResponse primitiveSearchResponse)
		{
			if (hashCode() != object.hashCode())
				return false;

			if (this.query.equals(primitiveSearchResponse.getQuery()) == false)
				return false;

			if (this.result != null && this.result.equals(primitiveSearchResponse.getResult()))
				return false;

			return true;
		}
		
		return false;
	}

	@Override
	public String toString() 
	{
		return this.query+" "+(this.result != null ? this.result.toString() : "");
	}
}