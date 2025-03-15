package org.radix.hyperscale.ledger.sme.components;

import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.crypto.Identity;
import org.radix.hyperscale.ledger.StateAddress;
import org.radix.hyperscale.ledger.StateContext;
import org.radix.hyperscale.ledger.Substate.NativeField;
import org.radix.hyperscale.ledger.primitives.Blob;
import org.radix.hyperscale.ledger.sme.Component;
import org.radix.hyperscale.ledger.sme.NativeComponent;
import org.radix.hyperscale.ledger.sme.StateMachine;
import org.radix.hyperscale.utils.Strings;

@StateContext("blob")
public class DataResourceComponent extends NativeComponent {
  static {
    Component.registerNative(DataResourceComponent.class);
  }

  public DataResourceComponent(StateMachine stateMachine) {
    super(stateMachine, DataResourceComponent.class);
  }

  public void store(String url, String mimeType, Blob blob, Identity authority) {
    StateAddress stateAddress =
        StateAddress.from(context(), Hash.valueOf(Strings.toLowerCase(url)));
    assertCreate(stateAddress, authority);

    set(stateAddress, NativeField.BLOB, blob.getHash());
    set(stateAddress, "url", Strings.toLowerCase(url));
    set(stateAddress, "mime-type", mimeType);
    associate(stateAddress, authority);
  }
}
