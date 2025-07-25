package org.radix.hyperscale.ledger.primitives;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.radix.hyperscale.collections.AdaptiveArrayList;
import org.radix.hyperscale.crypto.Certificate;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.exceptions.ValidationException;
import org.radix.hyperscale.ledger.CommitDecision;
import org.radix.hyperscale.ledger.StateAddress;
import org.radix.hyperscale.ledger.StateContext;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.SerializerId2;
import org.radix.hyperscale.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.atom.certificate")
@StateContext("atom.certificate")
public final class AtomCertificate extends Certificate
{
	@JsonProperty("atom")
	@DsonOutput(Output.ALL)
	private Hash atom;
	
	@JsonProperty("inventory")
	@DsonOutput(value = {Output.API, Output.WIRE, Output.PERSIST})
	private List<StateOutput> inventory;

	@SuppressWarnings("unused")
	private AtomCertificate()
	{
		super();
		
		// FOR SERIALIZER
	}
	
	public AtomCertificate(final Hash atom, final CommitDecision decision, final List<StateOutput> inventory) throws ValidationException
	{
		super(decision);
		
		Objects.requireNonNull(inventory, "State output inventory is null");
		Objects.requireNonNull(atom, "Atom is null");
		Hash.notZero(atom, "Atom is ZERO");
		this.atom = atom;
		
		if (inventory.isEmpty())
			throw new IllegalArgumentException("State output inventory is empty");
		
		this.inventory = new ArrayList<>(inventory);
		
		StateAddress prevStateOutput = null;
		for (int i = 0 ; i < this.inventory.size() ; i++)
		{
			StateOutput stateOutput = this.inventory.get(i);
			if (stateOutput.getAtom().equals(this.atom) == false)
				throw new ValidationException(stateOutput, "State certificate for "+stateOutput.getAddress()+" does not reference atom "+this.atom);
			
			if (prevStateOutput != null && prevStateOutput.compareTo(stateOutput.getAddress()) > 0)
				throw new ValidationException(stateOutput, "State certificate outputs are not sorted for atom "+this.atom);
			
			prevStateOutput = stateOutput.getAddress();
		}
	}

	public Hash getAtom()
	{
		return this.atom;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T getObject()
	{
		return (T) this.atom;
	}
	
	public int getInventorySize() 
	{
		return this.inventory.size();
	}
	
	public <T extends StateOutput> List<T> getInventory(Class<T> clazz)
	{
		final AdaptiveArrayList<T> inventory = new AdaptiveArrayList<T>(this.inventory.size());
		for (int i = 0 ; i < this.inventory.size() ; i++)
		{
			StateOutput output = this.inventory.get(i);
			if (clazz.isInstance(output) == false)
				continue;
			
			inventory.add(clazz.cast(output));
		}
		
		return inventory.freeze();
	}
}
