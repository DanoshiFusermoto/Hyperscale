package org.radix.hyperscale.ledger.messages;

import java.util.Objects;

import org.radix.hyperscale.ledger.PrimitiveSearchQuery;
import org.radix.hyperscale.network.messages.Message;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.SerializerId2;
import org.radix.hyperscale.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.messages.search.query.primitive")
public class PrimitiveSearchQueryMessage extends Message
{
	@JsonProperty("query")
	@DsonOutput(Output.ALL)
	private PrimitiveSearchQuery query;

	PrimitiveSearchQueryMessage()
	{
		super();
	}

	public PrimitiveSearchQueryMessage(final PrimitiveSearchQuery query)
	{
		super();

		this.query = Objects.requireNonNull(query, "State query is null");
	}

	public PrimitiveSearchQuery getQuery()
	{
		return this.query;
	}
}
