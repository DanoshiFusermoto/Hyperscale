package org.radix.hyperscale.ledger.messages;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.radix.hyperscale.crypto.Identity;
import org.radix.hyperscale.network.messages.Message;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.SerializerId2;
import org.radix.hyperscale.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.message.identities")
public class IdentitiesMessage extends Message
{
	@JsonProperty("identities")
	@DsonOutput(Output.ALL)
	private List<Identity> identities;

	@SuppressWarnings("unused")
	private IdentitiesMessage()
	{
		super();
	}
	
	public IdentitiesMessage(final Collection<Identity> identities)
	{
		super();
		
		Objects.requireNonNull(identities, "Identities is null");
		if (identities.isEmpty())
			throw new IllegalArgumentException("Identities is empty");
		
		this.identities = new ArrayList<Identity>(identities);
	}

	public List<Identity> getIdentities() 
	{ 
		return this.identities; 
	}
}
