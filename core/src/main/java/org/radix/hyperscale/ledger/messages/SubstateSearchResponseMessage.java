package org.radix.hyperscale.ledger.messages;

import java.util.Objects;

import org.radix.hyperscale.ledger.SubstateSearchResponse;
import org.radix.hyperscale.network.messages.Message;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.SerializerId2;
import org.radix.hyperscale.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.messages.search.response.substate")
public class SubstateSearchResponseMessage extends Message
{
	@JsonProperty("response")
	@DsonOutput(Output.ALL)
	private SubstateSearchResponse response;

	SubstateSearchResponseMessage()
	{
		super();
	}

	public SubstateSearchResponseMessage(final SubstateSearchResponse response)
	{
		super();

		this.response = Objects.requireNonNull(response, "Substate search response is null");
	}

	public SubstateSearchResponse getResponse()
	{
		return this.response;
	}
}

