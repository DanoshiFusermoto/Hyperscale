package org.radix.hyperscale.ledger.sme.components;

import org.radix.hyperscale.crypto.ComputeKey;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.crypto.Identity;
import org.radix.hyperscale.ledger.StateAddress;
import org.radix.hyperscale.ledger.StateContext;
import org.radix.hyperscale.ledger.StateLockMode;
import org.radix.hyperscale.ledger.sme.Component;
import org.radix.hyperscale.ledger.sme.NativeComponent;
import org.radix.hyperscale.ledger.sme.StateMachine;
import org.radix.hyperscale.utils.UInt256;

@StateContext("unique")
public final class UniqueValueComponent extends NativeComponent {
  static {
    Component.registerNative(UniqueValueComponent.class);
  }

  public UniqueValueComponent(StateMachine stateMachine) {
    super(stateMachine, UniqueValueComponent.class);
  }

  @Override
  public void lock(String method, Object[] arguments) {
    lock(StateAddress.from(context(), Hash.valueOf(arguments[0])), StateLockMode.WRITE);
  }

  public void set(UInt256 value, Identity identity) {
    StateAddress uniqueStateAddress = StateAddress.from(context(), Hash.valueOf(value));
    assertCreate(uniqueStateAddress, ComputeKey.NULL.getIdentity());

    set(uniqueStateAddress, "unique", value);
    associate(uniqueStateAddress, identity);
  }
}
