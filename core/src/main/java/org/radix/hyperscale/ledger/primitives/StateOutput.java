package org.radix.hyperscale.ledger.primitives;

import org.radix.hyperscale.common.EphemeralPrimitive;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.ledger.StateAddressable;
import org.radix.hyperscale.ledger.sme.SubstateTransitions;

public interface StateOutput extends EphemeralPrimitive, StateAddressable
{
	public Hash getAtom();

	public SubstateTransitions getStates();
	
	public Hash getExecution();
}
