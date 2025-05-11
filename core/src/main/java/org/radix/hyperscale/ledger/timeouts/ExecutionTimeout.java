package org.radix.hyperscale.ledger.timeouts;

import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.ledger.StateContext;
import org.radix.hyperscale.serialization.SerializerId2;

@SerializerId2("ledger.atom.timeout.execution")
@StateContext("atom.timeout.execution")
public final class ExecutionTimeout extends AtomTimeout
{
	@SuppressWarnings("unused")
	private ExecutionTimeout()
	{
		super();
			
		// FOR SERIALIZER
	}

	public ExecutionTimeout(final Hash atom, final boolean active) 
	{
		super(atom, active);
	}
}
