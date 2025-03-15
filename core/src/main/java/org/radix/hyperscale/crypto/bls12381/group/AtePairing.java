package org.radix.hyperscale.crypto.bls12381.group;

import java.util.Objects;
import org.miracl.core.BLS12381.FP12;
import org.miracl.core.BLS12381.PAIR;

/**
 * Forked from
 * https://github.com/ConsenSys/mikuli/tree/master/src/main/java/net/consensys/mikuli/crypto
 *
 * <p>Modified for use with Cassandra as internal code not a dependency
 *
 * <p>Original repo source has no license headers.
 */
public class AtePairing {
  /**
   * @param p1 the point in Group1, not null
   * @param p2 the point in Group2, not null
   * @return GTPoint
   */
  public static GTPoint pair(final G1Point p, final G2Point q) {
    Objects.requireNonNull(p, "Point p is null");
    Objects.requireNonNull(q, "Point q is null");

    FP12 e = PAIR.ate(q.ecp2Point(), p.ecpPoint());
    return new GTPoint(PAIR.fexp(e));
  }

  public static GTPoint pair2(final G1Point p, final G2Point q, final G1Point r, final G2Point s) {
    Objects.requireNonNull(p, "Point p is null");
    Objects.requireNonNull(q, "Point q is null");
    Objects.requireNonNull(r, "Point r is null");
    Objects.requireNonNull(s, "Point s is null");

    FP12 e = PAIR.ate2(q.ecp2Point(), p.ecpPoint(), s.ecp2Point(), r.ecpPoint());
    return new GTPoint(PAIR.fexp(e));
  }

  private AtePairing() {}
}
