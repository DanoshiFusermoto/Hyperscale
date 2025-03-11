package org.radix.hyperscale.ledger;

import java.util.Objects;

import org.radix.hyperscale.common.BasicObject;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.SerializerId2;
import org.radix.hyperscale.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.search.query.substate")
public final class SubstateSearchQuery extends BasicObject
{
	@JsonProperty("address")
	@DsonOutput(Output.ALL)
	private StateAddress address;

	@JsonProperty("isolation")
	@DsonOutput(Output.ALL)
	private Isolation isolation;
	
	private final boolean localOnly;

	SubstateSearchQuery()
	{ 
		super();
		this.localOnly = false;
	}

	public SubstateSearchQuery(final StateAddress address)
	{ 
		this(address, Isolation.COMMITTED);
	}

	public SubstateSearchQuery(final StateAddress address, final boolean localOnly)
	{ 
		this(address, Isolation.COMMITTED, localOnly);
	}

	public SubstateSearchQuery(final StateAddress address, final Isolation isolation)
	{ 
		this(address, isolation, false);
	}

	public SubstateSearchQuery(final StateAddress address, final Isolation isolation, final boolean localOnly)
	{ 
		this.address = Objects.requireNonNull(address, "State address is null");
		this.isolation = Objects.requireNonNull(isolation, "Isolation type is null");
		this.localOnly = localOnly;
	}

	public StateAddress getAddress()
	{
		return this.address;
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
		result = prime * result + this.address.hashCode();
		result = prime * result + this.isolation.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object object) 
	{
		if (object == null)
			return false;
		
		if (object == this)
			return true;
		
		if (object instanceof SubstateSearchQuery substateSearchQuery)
		{
			if (hashCode() != object.hashCode())
				return false;
			
			if (this.address.equals(substateSearchQuery.getAddress()) == false)
				return false;
			
			if (this.isolation.equals(substateSearchQuery.getIsolation()) == false)
				return false;

			return true;
		}
		
		return false;
	}

	public boolean isLocalOnly()
	{
		return this.localOnly;
	}
	
	@Override
	public String toString() 
	{
		return this.address+" "+this.isolation+" "+this.localOnly;
	}
}

