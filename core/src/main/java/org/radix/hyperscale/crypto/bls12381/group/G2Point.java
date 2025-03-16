package org.radix.hyperscale.crypto.bls12381.group;

import java.util.Collection;
import java.util.Objects;
import org.miracl.core.BLS12381.CONFIG_BIG;
import org.miracl.core.BLS12381.ECP2;
import org.radix.hyperscale.utils.Numbers;

/**
 * Forked from
 * https://github.com/ConsenSys/mikuli/tree/master/src/main/java/net/consensys/mikuli/crypto
 *
 * <p>Modified for use with Cassandra as internal code not a dependency
 *
 * <p>Original repo source has no license headers.
 */
public final class G2Point implements Group<G2Point> {
  private final ECP2 point;
  private static final int FP_POINT_SIZE = CONFIG_BIG.MODBYTES;

  public static final G2Point NULL;

  static {
    byte[] nullbytes = new byte[4 * FP_POINT_SIZE];
    NULL = fromBytes(nullbytes);
  }

  public static G2Point fromBytes(final byte[] bytes) {
    Objects.requireNonNull(bytes, "Point bytes is null");
    Numbers.equals(bytes.length, 4 * FP_POINT_SIZE, "Point bytes is invalid size");
    return new G2Point(ECP2.fromBytes(bytes));
  }

  public G2Point(final G2Point point) {
    Objects.requireNonNull(point, "Point is null");
    this.point = new ECP2(point.ecp2Point());
  }

  public G2Point(final ECP2 point) {
    Objects.requireNonNull(point, "ECP2 point is null");
    this.point = new ECP2(point);
  }

  public G2Point add(final Collection<G2Point> others) {
    Objects.requireNonNull(others, "Points to add is null");
    ECP2 sum = new ECP2(this.point);
    for (G2Point other : others) sum.add(other.point);
    sum.affine();
    return new G2Point(sum);
  }

  public G2Point add(final G2Point... others) {
    Objects.requireNonNull(others, "Points to add is null");
    ECP2 sum = new ECP2(this.point);
    for (G2Point other : others) sum.add(other.point);
    sum.affine();
    return new G2Point(sum);
  }

  public G2Point add(final G2Point other) {
    Objects.requireNonNull(other, "Point to add is null");
    ECP2 sum = new ECP2(this.point);
    sum.add(other.point);
    sum.affine();
    return new G2Point(sum);
  }

  public G2Point neg() {
    ECP2 neg = new ECP2(this.point);
    neg.neg();
    return new G2Point(neg);
  }

  public G2Point mul(final Scalar scalar) {
    Objects.requireNonNull(scalar, "Scalar is null");
    ECP2 newPoint = this.point.mul(scalar.value());
    return new G2Point(newPoint);
  }

  public byte[] toBytes() {
    byte[] bytes = new byte[4 * FP_POINT_SIZE];
    this.point.toBytes(bytes, true);
    return bytes;
  }

  ECP2 ecp2Point() {
    return new ECP2(this.point);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;

    G2Point other = (G2Point) obj;
    if (this.point == null) {
      if (other.point != null) return false;
    } else if (this.point.equals(other.point) == false) return false;

    return true;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    long xa = this.point.getX().getA().norm();
    long ya = this.point.getY().getA().norm();
    long xb = this.point.getX().getB().norm();
    long yb = this.point.getY().getB().norm();
    result = prime * result + (int) (xa ^ (xa >>> 32));
    result = prime * result + (int) (ya ^ (ya >>> 32));
    result = prime * result + (int) (xb ^ (xb >>> 32));
    result = prime * result + (int) (yb ^ (yb >>> 32));
    return result;
  }
}
