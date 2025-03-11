package org.radix.hyperscale.ledger.sme.arguments;

import java.util.Objects;

import org.radix.hyperscale.crypto.Identity;
import org.radix.hyperscale.ledger.sme.Argument;
import org.radix.hyperscale.ledger.sme.ArgumentContext;

@ArgumentContext("badge")
public class BadgeArgument implements Argument<Identity>
{
	private final Identity identity;
	
	public BadgeArgument(Identity identity)
	{
		this.identity = Objects.requireNonNull(identity, "Badge identity is null");
	}
	
	public Identity get()
	{
		return this.identity;
	}
}
