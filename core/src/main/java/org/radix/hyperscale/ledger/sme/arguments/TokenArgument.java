package org.radix.hyperscale.ledger.sme.arguments;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.ledger.StateAddress;
import org.radix.hyperscale.ledger.StateContext;
import org.radix.hyperscale.ledger.StateLockMode;
import org.radix.hyperscale.ledger.sme.ArgumentContext;
import org.radix.hyperscale.ledger.sme.SubstateArgument;
import org.radix.hyperscale.utils.Strings;

@StateContext("token")
@ArgumentContext("token")
public class TokenArgument implements SubstateArgument<String> {
  // Cached because expensive to lookup
  private static final String TOKEN_STATE_CONTEXT =
      TokenArgument.class.getAnnotation(StateContext.class).value();

  private final String symbol;
  private final StateLockMode lockMode;
  private final StateAddress tokenAddress;
  private final List<Entry<StateAddress, StateLockMode>> addresses;

  TokenArgument(final String symbol) {
    this(symbol, StateLockMode.READ);
  }

  public TokenArgument(final String symbol, final StateLockMode lockMode) {
    Objects.requireNonNull(symbol, "Token symbol is null");
    Objects.requireNonNull(lockMode, "State lock mode is null");

    this.lockMode = lockMode;
    this.symbol = Strings.toLowerCase(symbol);
    this.tokenAddress = StateAddress.from(TOKEN_STATE_CONTEXT, Hash.valueOf(this.symbol));
    this.addresses =
        Collections.singletonList(new AbstractMap.SimpleEntry<>(this.tokenAddress, this.lockMode));
  }

  @Override
  public String get() {
    return this.symbol;
  }

  @Override
  public List<Entry<StateAddress, StateLockMode>> getAddresses() {
    return this.addresses;
  }
}
