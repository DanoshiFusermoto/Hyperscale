package org.radix.hyperscale.ledger.messages;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.radix.hyperscale.ledger.primitives.Atom;
import org.radix.hyperscale.network.messages.Message;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.SerializerId2;
import org.radix.hyperscale.serialization.DsonOutput.Output;
import org.radix.hyperscale.utils.Numbers;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@SerializerId2("ledger.messages.submit.atoms")
public final class SubmitAtomsMessage extends Message
{
	@JsonProperty("atoms")
	@DsonOutput(Output.ALL)
	@JsonDeserialize(as=ArrayList.class)
	private List<Atom> atoms;

	@SuppressWarnings("unused")
	private SubmitAtomsMessage()
	{
		super();
	}
	
	public SubmitAtomsMessage(final Atom atom)
	{
		super();
			
		Objects.requireNonNull(atom, "Atom is null");
			
		this.atoms = Collections.singletonList(atom);
	}

	public SubmitAtomsMessage(final Collection<Atom> atoms)
	{
		super();
			
		Objects.requireNonNull(atoms, "Atoms is null");
		Numbers.isZero(atoms.size(), "Atoms is empty");
			
		this.atoms = new ArrayList<Atom>(atoms);
	}

	public List<Atom> getAtoms()
	{
		return Collections.unmodifiableList(this.atoms);
	}
}
