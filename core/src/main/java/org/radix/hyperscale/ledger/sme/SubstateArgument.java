package org.radix.hyperscale.ledger.sme;

import java.util.List;
import java.util.Map.Entry;

import org.radix.hyperscale.ledger.StateAddress;
import org.radix.hyperscale.ledger.StateLockMode;

public interface SubstateArgument<T> extends Argument<T>
{
	public List<Entry<StateAddress, StateLockMode>> getAddresses();
}
