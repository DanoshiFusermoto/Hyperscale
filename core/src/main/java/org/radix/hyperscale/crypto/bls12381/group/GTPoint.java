package org.radix.hyperscale.crypto.bls12381.group;

import java.util.Objects;
import org.miracl.core.BLS12381.FP12;

/**
 * Forked from
 * https://github.com/ConsenSys/mikuli/tree/master/src/main/java/net/consensys/mikuli/crypto
 *
 * <p>Modified for use with Cassandra as internal code not a dependency
 *
 * <p>Original repo source has no license headers.
 */
public class GTPoint {
  private final FP12 point;

  GTPoint(final FP12 point) {
    Objects.requireNonNull(point, "Point is null");
    this.point = point;
  }

  public boolean isunity() {
    return point.isunity();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((this.point == null) ? 0 : this.point.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;

    GTPoint other = (GTPoint) obj;
    if (this.point == null) {
      if (other.point != null) return false;
    } else if (this.point.equals(other.point) == false) return false;

    return true;
  }
}
