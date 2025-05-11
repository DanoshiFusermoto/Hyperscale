package org.radix.hyperscale.ledger.timeouts;

import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.ledger.StateContext;
import org.radix.hyperscale.serialization.SerializerId2;

@SerializerId2("ledger.atom.timeout.prepare")
@StateContext("atom.timeout.prepare")
public final class PrepareTimeout extends AtomTimeout
{
	@SuppressWarnings("unused")
	private PrepareTimeout()
	{
		super();
			
		// FOR SERIALIZER
	}

	public PrepareTimeout(final Hash atom, final boolean active) 
	{
		super(atom, active);
	}
}
