package org.radix.hyperscale.ledger.timeouts;

import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.ledger.StateContext;
import org.radix.hyperscale.serialization.SerializerId2;

@SerializerId2("ledger.atom.timeout.accept")
@StateContext("atom.timeout.accept")
public final class AcceptTimeout extends AtomTimeout
{
	@SuppressWarnings("unused")
	private AcceptTimeout()
	{
		super();
			
		// FOR SERIALIZER
	}

	public AcceptTimeout(final Hash atom) 
	{
		super(atom);
	}
}
