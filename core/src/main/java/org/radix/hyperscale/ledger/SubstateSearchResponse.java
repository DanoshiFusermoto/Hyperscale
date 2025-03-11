package org.radix.hyperscale.ledger;

import java.util.Objects;

import org.radix.hyperscale.common.BasicObject;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.SerializerId2;
import org.radix.hyperscale.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.search.response.substate")
public final class SubstateSearchResponse extends BasicObject
{
	@JsonProperty("query")
	@DsonOutput(Output.ALL)
	private SubstateSearchQuery query;

	@JsonProperty("result")
	@DsonOutput(Output.ALL)
	private SubstateCommit result;
	
	SubstateSearchResponse()
	{ 
		super();
	}
	
	public SubstateSearchResponse(final SubstateSearchQuery query)
	{ 
		super();
		
		this.query = Objects.requireNonNull(query, "State search query is null");
	}

	public SubstateSearchResponse(final SubstateSearchQuery query, final SubstateCommit substate)
	{ 
		super();
		
		this.query = Objects.requireNonNull(query, "Substate search query is null");
		this.result = Objects.requireNonNull(substate, "Substate search result is null");
	}

	public SubstateSearchQuery getQuery()
	{
		return this.query;
	}
	
	public SubstateCommit getResult()
	{
		return this.result;
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
		
		if (object instanceof SubstateSearchResponse substateSearchResponse)
		{
			if (hashCode() != object.hashCode())
				return false;

			if (this.query.equals(substateSearchResponse.getQuery()) == false)
				return false;

			if (this.result != null && this.result.equals(substateSearchResponse.getResult()))
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
