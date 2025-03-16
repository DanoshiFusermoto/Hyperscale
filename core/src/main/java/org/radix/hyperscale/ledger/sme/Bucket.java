package org.radix.hyperscale.ledger.sme;

import java.util.Map;
import org.eclipse.collections.api.factory.Maps;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.exceptions.ValidationException;
import org.radix.hyperscale.utils.UInt256;

public class Bucket {
  private final String label;

  private final Map<String, UInt256> tokens;

  Bucket(String label) {
    this.label = label;
    this.tokens = Maps.mutable.ofInitialCapacity(4);
  }

  public String getLabel() {
    return this.label;
  }

  public boolean isEmpty() {
    synchronized (this.tokens) {
      return this.tokens.isEmpty();
    }
  }

  @Override
  public String toString() {
    return "Bucket [label=" + this.label + "]";
  }

  public void put(String symbol, UInt256 quantity) {
    synchronized (this.tokens) {
      final UInt256 current = this.tokens.get(symbol);
      if (current == null) this.tokens.put(symbol, quantity);
      else this.tokens.put(symbol, current.add(quantity));
    }
  }

  public void put(Hash token, UInt256 quantity) {
    synchronized (this.tokens) {
      final String tokenHex = token.toString();
      final UInt256 current = this.tokens.get(tokenHex);
      if (current == null) this.tokens.put(tokenHex, quantity);
      else this.tokens.put(tokenHex, current.add(quantity));
    }
  }

  public void take(String symbol, UInt256 quantity) throws ValidationException {
    synchronized (this.tokens) {
      UInt256 balance = this.tokens.getOrDefault(symbol, UInt256.ZERO);
      if (balance.compareTo(quantity) < 0)
        throw new ValidationException(
            "Bucket "
                + this.label
                + " does not hold a balance of "
                + quantity
                + " "
                + symbol
                + " but "
                + balance
                + " "
                + symbol);

      balance = balance.subtract(quantity);
      if (balance.compareTo(UInt256.ZERO) == 0) this.tokens.remove(symbol);
      else this.tokens.put(symbol, balance);
    }
  }

  public void take(Hash token, UInt256 quantity) throws ValidationException {
    synchronized (this.tokens) {
      UInt256 balance = this.tokens.getOrDefault(token.toString(), UInt256.ZERO);
      if (balance.compareTo(quantity) < 0)
        throw new ValidationException(
            "Bucket "
                + this.label
                + " does not hold a balance of "
                + quantity
                + " "
                + token
                + " but "
                + balance
                + " "
                + token);

      balance = balance.subtract(quantity);
      if (balance.compareTo(UInt256.ZERO) == 0) this.tokens.remove(token.toString());
      else this.tokens.put(token.toString(), balance);
    }
  }
}
