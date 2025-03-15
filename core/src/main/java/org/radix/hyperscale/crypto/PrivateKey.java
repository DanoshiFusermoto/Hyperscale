package org.radix.hyperscale.crypto;

import java.util.Objects;

public abstract class PrivateKey<S extends Signature> extends Key {
  @Override
  public final boolean canSign() {
    return true;
  }

  @Override
  public final boolean canVerify() {
    return false;
  }

  public final Identity getIdentity() {
    throw new UnsupportedOperationException("Private keys do not support identites");
  }

  public final S sign(final Hash hash) throws CryptoException {
    Objects.requireNonNull(hash, "Hash to sign is null");
    return sign(hash.toByteArray());
  }

  public abstract S sign(byte[] hash) throws CryptoException;
}
