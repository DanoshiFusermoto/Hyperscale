package org.radix.hyperscale.ledger.timeouts;

import java.util.Objects;

import org.radix.hyperscale.common.ExtendedObject;
import org.radix.hyperscale.common.Primitive;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.SerializerId2;
import org.radix.hyperscale.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.atom.timeout")
public abstract class AtomTimeout extends ExtendedObject implements Primitive
{
	@JsonProperty("atom")
	@DsonOutput(Output.ALL)
	private Hash atom;

	/** Whether this timeout was triggered or passively created **/
	private final transient boolean active;

	AtomTimeout()
	{
		super();

		// If received via serializer, then must be active timeout
		this.active = true;
	}
		
	AtomTimeout(final Hash atom, final boolean active)
	{
		Objects.requireNonNull(atom, "Timeout atom is null");
		Hash.notZero(atom, "Timeout atom hash is ZERO");

		this.atom = atom;
		this.active = active;
	}

	public final Hash getAtom()
	{
		return this.atom;
	}
	
	public final boolean isActive()
	{
		return this.active;
	}
	
	@Override
	public String toString()
	{
		return super.toString()+" atom="+getAtom()+" active="+isActive();
	}
}
