package org.radix.hyperscale.ledger.timeouts;

import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.ledger.StateContext;
import org.radix.hyperscale.serialization.SerializerId2;

@SerializerId2("ledger.atom.timeout.execution.latent")
@StateContext("atom.timeout.execution.latent")
public final class ExecutionLatentTimeout extends AtomTimeout
{
	@SuppressWarnings("unused")
	private ExecutionLatentTimeout()
	{
		super();
			
		// FOR SERIALIZER
	}

	public ExecutionLatentTimeout(final Hash atom, final boolean active) 
	{
		super(atom, active);
	}
}