package org.radix.hyperscale.crypto.bls12381;

import java.util.Objects;
import org.miracl.core.BLS12381.BIG;
import org.radix.hyperscale.crypto.CryptoException;
import org.radix.hyperscale.crypto.PrivateKey;
import org.radix.hyperscale.crypto.Signature.VerificationResult;
import org.radix.hyperscale.crypto.bls12381.group.G1Point;
import org.radix.hyperscale.crypto.bls12381.group.Scalar;
import org.radix.hyperscale.utils.Numbers;

/**
 * Forked from
 * https://github.com/ConsenSys/mikuli/tree/master/src/main/java/net/consensys/mikuli/crypto
 *
 * <p>Modified for use with Cassandra as internal code not a dependency
 *
 * <p>Original repo source has no license headers.
 */
public final class BLSPrivateKey extends PrivateKey<BLSSignature> {
  public static final BLSPrivateKey from(final byte[] bytes) {
    Objects.requireNonNull(bytes, "Bytes is null");
    Numbers.isZero(bytes.length, "Bytes length is zero");
    return new BLSPrivateKey(new Scalar(BIG.fromBytes(bytes)));
  }

  private final Scalar scalarValue;

  BLSPrivateKey(final Scalar value) {
    Objects.requireNonNull(value, "PrivateKey was not properly initialized");
    this.scalarValue = value;
  }

  @Override
  public BLSSignature sign(final byte[] hash) throws CryptoException {
    BLSSignature signature = BLS12381.sign(this, hash);
    signature.setVerified(VerificationResult.SUCCESS);
    return signature;
  }

  protected G1Point sign(final G1Point message) {
    Objects.requireNonNull(message, "Message to sign is null");
    if (BLS12381.SKIP_SIGNING) return G1Point.NULL;

    final G1Point signature = message.mul(this.scalarValue);
    BLSKeyPair.signings.incrementAndGet();
    return signature;
  }

  public byte[] toByteArray() {
    return this.scalarValue.toByteArray();
  }
}
