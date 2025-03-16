package org.radix.hyperscale.crypto.bls12381.group;

import java.util.Objects;
import org.miracl.core.BLS12381.BIG;
import org.miracl.core.BLS12381.CONFIG_BIG;
import org.radix.hyperscale.utils.Numbers;

/**
 * Forked from
 * https://github.com/ConsenSys/mikuli/tree/master/src/main/java/net/consensys/mikuli/crypto
 *
 * <p>Modified for use with Cassandra as internal code not a dependency
 *
 * <p>Original repo source has no license headers.
 */
public class Scalar {
  public static final Scalar from(final byte[] bytes) {
    Objects.requireNonNull(bytes, "Bytes is null");
    Numbers.isZero(bytes.length, "Bytes length is zero");
    return new Scalar(BIG.fromBytes(bytes));
  }

  private final BIG value;

  public Scalar(final BIG value) {
    Objects.requireNonNull(value, "Value is null");
    this.value = value;
  }

  BIG value() {
    return new BIG(value);
  }

  public byte[] toByteArray() {
    byte[] bytes = new byte[CONFIG_BIG.MODBYTES];
    this.value.toBytes(bytes);
    return bytes;
  }
}
