package org.radix.hyperscale.ledger.messages;

import java.util.Objects;

import org.radix.hyperscale.ledger.AssociationSearchResponse;
import org.radix.hyperscale.network.messages.Message;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.SerializerId2;
import org.radix.hyperscale.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.messages.search.response.association")
public class AssociationSearchResponseMessage extends Message
{
	@JsonProperty("response")
	@DsonOutput(Output.ALL)
	private AssociationSearchResponse response;

	AssociationSearchResponseMessage()
	{
		super();
	}

	public AssociationSearchResponseMessage(final AssociationSearchResponse response)
	{
		super();

		this.response = Objects.requireNonNull(response, "Response is null");
	}

	public AssociationSearchResponse getResponse()
	{
		return this.response;
	}
}

