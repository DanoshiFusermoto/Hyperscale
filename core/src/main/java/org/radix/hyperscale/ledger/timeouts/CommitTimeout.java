package org.radix.hyperscale.ledger.timeouts;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.ledger.StateContext;
import org.radix.hyperscale.ledger.primitives.StateInput;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.SerializerId2;
import org.radix.hyperscale.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.atom.timeout.commit")
@StateContext("atom.timeout.commit")
public final class CommitTimeout extends AtomTimeout
{
	@JsonProperty("inputs")
	@DsonOutput(Output.ALL)
	private List<StateInput> inputs;

	@SuppressWarnings("unused")
	private CommitTimeout()
	{
		super();
			
		// FOR SERIALIZER
	}

	public CommitTimeout(final Hash atom, final Collection<StateInput> inputs)
	{
		super(atom);
		
		Objects.requireNonNull(inputs, "Timeout inputs is null");
		if (inputs.isEmpty())
			throw new IllegalArgumentException("Timeout inputs is empty");
		
		this.inputs = new ArrayList<StateInput>(inputs);
		Collections.sort(this.inputs);
	}
	
	public Collection<StateInput> getStateInputs()
	{
		return Collections.unmodifiableList(this.inputs);
	}
	
	@Override
	public String toString()
	{
		return super.toString()+" inputs="+this.inputs.toString();
	}
}
