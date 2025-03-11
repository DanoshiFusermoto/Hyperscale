package org.radix.hyperscale.ledger.sme.arguments;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;

import org.radix.hyperscale.crypto.Identity;
import org.radix.hyperscale.ledger.StateAddress;
import org.radix.hyperscale.ledger.StateContext;
import org.radix.hyperscale.ledger.StateLockMode;
import org.radix.hyperscale.ledger.sme.ArgumentContext;
import org.radix.hyperscale.ledger.sme.SubstateArgument;

import java.util.Objects;

@StateContext("account")
@ArgumentContext("account")
public class AccountArgument implements SubstateArgument<Identity>
{
	// Cached because expensive to lookup
	private static final String ACCOUNT_STATE_CONTEXT = AccountArgument.class.getAnnotation(StateContext.class).value();

	private final Identity identity;
	private final StateLockMode lockMode;
	private final StateAddress accountAddress;
	
	public AccountArgument(final Identity identity)
	{
		this(identity, StateLockMode.WRITE);
	}

	public AccountArgument(final Identity identity, final StateLockMode lockMode)
	{
		Objects.requireNonNull(lockMode, "State lock mode is null");

		this.lockMode = lockMode;
		this.identity = Objects.requireNonNull(identity, "Account identity is null");
		this.accountAddress = StateAddress.from(ACCOUNT_STATE_CONTEXT, identity);
	}

	@Override
	public Identity get()
	{
		return this.identity;
	}
	
	@Override
	public List<Entry<StateAddress, StateLockMode>> getAddresses() 
	{
		return Collections.singletonList(new AbstractMap.SimpleEntry<>(this.accountAddress, this.lockMode));
	}
}
