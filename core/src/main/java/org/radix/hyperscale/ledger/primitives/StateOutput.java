package org.radix.hyperscale.ledger.primitives;

import org.radix.hyperscale.common.EphemeralPrimitive;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.ledger.StateAddressable;
import org.radix.hyperscale.ledger.sme.SubstateTransitions;

public interface StateOutput extends EphemeralPrimitive, StateAddressable {
  Hash getAtom();

  SubstateTransitions getStates();

  Hash getExecution();
}
