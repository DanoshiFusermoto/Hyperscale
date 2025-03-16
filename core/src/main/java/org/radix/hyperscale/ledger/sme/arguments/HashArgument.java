package org.radix.hyperscale.ledger.sme.arguments;

import java.util.Objects;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.ledger.sme.Argument;
import org.radix.hyperscale.ledger.sme.ArgumentContext;

@ArgumentContext("hash")
public class HashArgument implements Argument<Hash> {
  private final Hash hash;

  public HashArgument(Hash hash) {
    this.hash = Objects.requireNonNull(hash, "Hash is null");
  }

  public Hash get() {
    return this.hash;
  }
}
