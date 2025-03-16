package org.radix.hyperscale.crypto.bls12381;

import org.miracl.core.BLS12381.BIG;
import org.miracl.core.BLS12381.ECP2;
import org.miracl.core.BLS12381.ROM;
import org.radix.hyperscale.crypto.bls12381.group.G2Point;

/**
 * Forked from
 * https://github.com/ConsenSys/mikuli/tree/master/src/main/java/net/consensys/mikuli/crypto
 *
 * <p>Modified for use with Cassandra as internal code not a dependency
 *
 * <p>Original repo source has no license headers.
 */
public final class BLSCurveParameters {
  public static final G2Point g2Generator() {
    return new G2Point(ECP2.generator());
  }

  public static final BIG curveOrder() {
    return new BIG(ROM.CURVE_Order);
  }

  private BLSCurveParameters() {}
}
