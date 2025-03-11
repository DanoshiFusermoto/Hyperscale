package org.radix.hyperscale.ledger;

import java.util.Objects;

import org.radix.hyperscale.crypto.Identity;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.Serializable;
import org.radix.hyperscale.serialization.SerializerId2;
import org.radix.hyperscale.serialization.DsonOutput.Output;
import org.radix.hyperscale.utils.Numbers;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.proposer")
public final class Proposer extends Serializable implements Comparable<Proposer>
{
	@JsonProperty("identity")
	@DsonOutput(Output.ALL)
	private Identity identity;

	@JsonProperty("influence")
	@DsonOutput(Output.ALL)
	private int influence;
	
	@SuppressWarnings("unused")
	private Proposer()
	{
		// FOR SERIALIZER
	}

	public Proposer(final Identity identity, final int influence)
	{
		Numbers.isNegative(influence, "Influence is negative");
		this.identity = Objects.requireNonNull(identity, "Identity is null");
		this.influence = influence;
	}

	public Identity getIdentity()
	{
		return this.identity;
	}

	public int getInfluence()
	{
		return this.influence;
	}

	public int getNextInfluence()
	{
		return this.influence+1;
	}

	@Override
	public int compareTo(Proposer o)
	{
		return o.getIdentity().compareTo(this.identity);
	}
}
