package org.radix.hyperscale.ledger.sme;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.crypto.Identity;
import org.radix.hyperscale.exceptions.ValidationException;
import org.radix.hyperscale.ledger.StateAddress;
import org.radix.hyperscale.ledger.StateContext;
import org.radix.hyperscale.ledger.Substate.NativeField;
import org.radix.hyperscale.ledger.sme.components.AccountComponent;
import org.radix.hyperscale.utils.Strings;
import org.radix.hyperscale.utils.UInt256;

@StateContext("vault")
public class Vault {
  private static final List<String> ALL_TOKEN_WHITELIST = Collections.singletonList("*");

  private final StateMachine stateMachine;
  private final SubstateLog substateLog;

  private final boolean debitable;
  private final StateAddress accountAddress;

  Vault(final StateMachine stateMachine, final SubstateLog substateLog, final boolean debitable) {
    Objects.requireNonNull(stateMachine, "Statemachine is null");
    Objects.requireNonNull(substateLog, "Substate log is null");
    if (getClass()
            .getAnnotation(StateContext.class)
            .value()
            .equalsIgnoreCase(substateLog.getAddress().context())
        == false)
      throw new IllegalArgumentException(
          "Substate log " + substateLog.getAddress() + " is not a vault");

    this.stateMachine = stateMachine;
    this.substateLog = substateLog;
    this.debitable = debitable;

    // Pre-computed account address associated with this Vault
    // Compute identities don't have accounts (yet)
    final Identity authority = this.substateLog.getAuthority();
    if (authority.getPrefix() != Identity.COMPUTE)
      this.accountAddress = StateAddress.from(AccountComponent.class, authority);
    else this.accountAddress = null;
  }

  public UInt256 balance(final String symbol) {
    Objects.requireNonNull(symbol, "Token symbol is null");
    return this.substateLog.getOrDefault(Strings.toLowerCase(symbol), UInt256.ZERO);
  }

  public UInt256 balance(final Hash token) {
    Objects.requireNonNull(token, "Token hash is null");
    Hash.notZero(token, "Token hash is ZERO");
    return this.substateLog.getOrDefault(token.toString(), UInt256.ZERO);
  }

  public UInt256 put(final String symbol, final UInt256 quantity, final Bucket bucket)
      throws ValidationException {
    Objects.requireNonNull(symbol, "Token symbol is null");
    Objects.requireNonNull(quantity, "Token quantity is null");
    Objects.requireNonNull(bucket, "Bucket is null");

    final String lowerSymbol = symbol.toLowerCase();
    if (this.accountAddress != null) {
      final List<String> acceptedTokens =
          this.stateMachine.get(this.accountAddress, "tokens.accept", ALL_TOKEN_WHITELIST);
      if (acceptedTokens.contains("*") == false && acceptedTokens.contains(lowerSymbol) == false)
        throw new ValidationException(
            "Account associated with "
                + this.stateMachine.get(accountAddress, NativeField.AUTHORITY)
                + " does not accept "
                + lowerSymbol.toUpperCase());
    }

    bucket.take(lowerSymbol, quantity);

    UInt256 balance = this.substateLog.getOrDefault(lowerSymbol, UInt256.ZERO);
    balance = balance.add(quantity);
    this.substateLog.set(lowerSymbol, balance);
    return balance;
  }

  public UInt256 put(final Hash token, final UInt256 quantity, final Bucket bucket)
      throws ValidationException {
    Objects.requireNonNull(token, "Token hash is null");
    Objects.requireNonNull(quantity, "Token quantity is null");
    Objects.requireNonNull(bucket, "Bucket is null");
    Hash.notZero(token, "Token hash is ZERO");

    if (this.accountAddress != null) {
      final List<String> acceptedTokens =
          this.stateMachine.get(this.accountAddress, "tokens.accept", ALL_TOKEN_WHITELIST);
      if (acceptedTokens.contains("*") == false
          && acceptedTokens.contains(token.toString()) == false)
        throw new ValidationException(
            "Account associated with "
                + this.stateMachine.get(accountAddress, NativeField.AUTHORITY)
                + " does not accept "
                + token.toString());
    }

    bucket.take(token, quantity);

    UInt256 balance = this.substateLog.getOrDefault(token.toString(), UInt256.ZERO);
    balance = balance.add(quantity);
    this.substateLog.set(token.toString(), balance);
    return balance;
  }

  public UInt256 take(final String symbol, final UInt256 quantity, final Bucket bucket)
      throws ValidationException {
    Objects.requireNonNull(symbol, "Token symbol is null");
    Objects.requireNonNull(quantity, "Token quantity is null");
    Objects.requireNonNull(bucket, "Bucket is null");

    final String lowerSymbol = symbol.toLowerCase();
    if (this.debitable == false)
      throw new ValidationException(
          "Vault "
              + this.substateLog.getAddress()
              + " authority not present for debit of "
              + quantity
              + " "
              + lowerSymbol);

    UInt256 balance = this.substateLog.getOrDefault(lowerSymbol, UInt256.ZERO);
    if (balance.compareTo(quantity) < 0)
      throw new ValidationException(
          "Vault "
              + this.substateLog.getAddress()
              + " does not hold a balance of "
              + quantity
              + " "
              + lowerSymbol
              + " but "
              + balance
              + " "
              + lowerSymbol);

    balance = balance.subtract(quantity);
    this.substateLog.set(lowerSymbol, balance);
    bucket.put(lowerSymbol, quantity);
    return balance;
  }

  public UInt256 take(final Hash token, final UInt256 quantity, final Bucket bucket)
      throws ValidationException {
    Objects.requireNonNull(token, "Token hash is null");
    Objects.requireNonNull(quantity, "Token quantity is null");
    Objects.requireNonNull(bucket, "Bucket is null");
    Hash.notZero(token, "Token hash is ZERO");

    if (this.debitable == false)
      throw new ValidationException(
          "Vault "
              + this.substateLog.getAddress()
              + " authority not present for debit of "
              + quantity
              + " "
              + token);

    UInt256 balance = this.substateLog.getOrDefault(token.toString(), UInt256.ZERO);
    if (balance.compareTo(quantity) < 0)
      throw new ValidationException(
          "Vault "
              + this.substateLog.getAddress()
              + " does not hold a balance of "
              + quantity
              + " "
              + token
              + " but "
              + balance
              + " "
              + token);

    balance = balance.subtract(quantity);
    this.substateLog.set(token.toString(), balance);
    bucket.put(token, quantity);
    return balance;
  }
}
