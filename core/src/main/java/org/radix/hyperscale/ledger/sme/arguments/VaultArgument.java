package org.radix.hyperscale.ledger.sme.arguments;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.crypto.Identity;
import org.radix.hyperscale.ledger.StateAddress;
import org.radix.hyperscale.ledger.StateContext;
import org.radix.hyperscale.ledger.StateLockMode;
import org.radix.hyperscale.ledger.sme.ArgumentContext;
import org.radix.hyperscale.ledger.sme.SubstateArgument;

@StateContext("vault")
@ArgumentContext("vault")
public class VaultArgument implements SubstateArgument<Identity> {
  // Cached because expensive to lookup
  private static final String VAULT_STATE_CONTEXT =
      VaultArgument.class.getAnnotation(StateContext.class).value();
  private static final String ACCOUNT_STATE_CONTEXT =
      AccountArgument.class.getAnnotation(StateContext.class).value();

  private final Identity identity;
  private final StateLockMode lockMode;
  private final StateAddress vaultAddress;
  private final StateAddress accountAddress;
  private final List<Entry<StateAddress, StateLockMode>> addresses;

  public VaultArgument(final Identity identity) {
    this(identity, StateLockMode.WRITE);
  }

  public VaultArgument(final Identity identity, final StateLockMode lockMode) {
    Objects.requireNonNull(identity, "Vault identity is null");
    Objects.requireNonNull(lockMode, "State lock mode is null");

    this.lockMode = lockMode;
    this.identity = identity;

    final Hash vaultScope = identity.getHash();
    this.vaultAddress = StateAddress.from(VAULT_STATE_CONTEXT, vaultScope);
    this.accountAddress = StateAddress.from(ACCOUNT_STATE_CONTEXT, vaultScope);

    final List<Entry<StateAddress, StateLockMode>> addresses = new ArrayList<>(2);
    addresses.add(new AbstractMap.SimpleEntry<>(this.vaultAddress, this.lockMode));
    addresses.add(new AbstractMap.SimpleEntry<>(this.accountAddress, StateLockMode.READ));
    this.addresses = Collections.unmodifiableList(addresses);
  }

  @Override
  public Identity get() {
    return this.identity;
  }

  @Override
  public List<Entry<StateAddress, StateLockMode>> getAddresses() {
    return this.addresses;
  }
}
