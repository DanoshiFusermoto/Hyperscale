package org.radix.hyperscale.ledger.sme.arguments;

import java.util.Objects;

import org.radix.hyperscale.crypto.Identity;
import org.radix.hyperscale.ledger.sme.Argument;
import org.radix.hyperscale.ledger.sme.ArgumentContext;

@ArgumentContext("identity")
public class IdentityArgument implements Argument<Identity>
{
	private final Identity identity;
	
	public IdentityArgument(Identity identity)
	{
		this.identity = Objects.requireNonNull(identity, "Identity is null");
	}
	
	public Identity get()
	{
		return this.identity;
	}
}
