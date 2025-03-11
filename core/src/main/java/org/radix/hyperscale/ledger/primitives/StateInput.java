package org.radix.hyperscale.ledger.primitives;

import java.util.Objects;

import org.radix.hyperscale.common.EphemeralPrimitive;
import org.radix.hyperscale.common.ExtendedObject;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.ledger.StateAddress;
import org.radix.hyperscale.ledger.Substate;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.SerializerId2;
import org.radix.hyperscale.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.state.input")
public final class StateInput extends ExtendedObject implements EphemeralPrimitive
{
	@JsonProperty("atom")
	@DsonOutput(Output.ALL)
	private Hash atom;
		
	@JsonProperty("substate")
	@DsonOutput(Output.ALL)
	private Substate substate;
	
	private StateInput()
	{
		super();
	}
	
	public StateInput(final Hash atom, final Substate substate)
	{
		this();
		
		Objects.requireNonNull(substate, "Substate is null");
		Objects.requireNonNull(atom, "Atom is null");
		Hash.notZero(atom, "Atom hash is zero");

		this.atom = atom;
		this.substate = new Substate(substate);
	}
	
	/**
	 * State inputs output a hash which is used as a reference to the atom:stateaddress substate.
	 * 
	 * It can NOT be used to verify the integrity of the state input.
	 * 
	 * TODO At some point implement IDs into primitives which would allow both references by ID and integrity hashes
	 */
	@Override
	protected synchronized Hash computeHash() 
	{
		return Hash.hash(this.atom.toByteArray(), this.substate.getAddress().getHash().toByteArray());
	}

	public Hash getAtom()
	{
		return this.atom;
	}
	
	public StateAddress getAddress() 
	{
		return this.substate.getAddress();
	}

	public Substate getSubstate()
	{
		return this.substate;
	}

	@Override
	public final String toString()
	{
		return getHash()+" atom="+this.atom+" substate="+this.substate+" "+this.substate.values();
	}
}
