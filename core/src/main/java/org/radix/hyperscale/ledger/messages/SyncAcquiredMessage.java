package org.radix.hyperscale.ledger.messages;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.ledger.BlockHeader;
import org.radix.hyperscale.network.TransportParameters;
import org.radix.hyperscale.network.messages.Message;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.SerializerId2;
import org.radix.hyperscale.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@SerializerId2("ledger.messages.sync.acquired")
@TransportParameters(async = true)
public final class SyncAcquiredMessage extends Message
{
	@JsonProperty("head")
	@DsonOutput(Output.ALL)
	private BlockHeader head;
	
	@JsonProperty("atoms")
	@DsonOutput(Output.ALL)
	@JsonDeserialize(as = LinkedHashSet.class)
	private Set<Hash> atoms;

	@SuppressWarnings("unused")
	private SyncAcquiredMessage()
	{
		super();
	}
	
	public SyncAcquiredMessage(final BlockHeader head, final Collection<Hash> pending)
	{
		super();
		
		this.head = Objects.requireNonNull(head, "Block head is null");
		this.atoms = new LinkedHashSet<Hash>(Objects.requireNonNull(pending, "Pending atoms is null"));
	}
	
	public BlockHeader getHead()
	{
		return this.head;
	}
	
	public Set<Hash> getPending()
	{
		return Collections.unmodifiableSet(this.atoms);
	}
	
	public boolean containsPending(final Hash atom)
	{
		return this.atoms.contains(atom);
	}
}