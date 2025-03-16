package org.radix.hyperscale.crypto.bls12381.group;

import java.util.Collection;
import java.util.Objects;
import org.miracl.core.BLS12381.CONFIG_BIG;
import org.miracl.core.BLS12381.ECP;
import org.radix.hyperscale.utils.Numbers;

/**
 * Forked from
 * https://github.com/ConsenSys/mikuli/tree/master/src/main/java/net/consensys/mikuli/crypto
 *
 * <p>Modified for use with Cassandra as internal code not a dependency
 *
 * <p>Original repo source has no license headers.
 */
public final class G1Point implements Group<G1Point> {
  private static final int FP_POINT_SIZE = CONFIG_BIG.MODBYTES;
  public static final G1Point NULL;

  static {
    byte[] nullbytes = new byte[FP_POINT_SIZE + 1];
    NULL = fromBytes(nullbytes);
  }

  public static G1Point fromBytes(final byte[] bytes) {
    Objects.requireNonNull(bytes, "Point bytes is null");
    Numbers.equals(bytes.length, FP_POINT_SIZE + 1, "Point bytes is invalid size");
    return new G1Point(ECP.fromBytes(bytes));
  }

  final ECP point;

  public G1Point(final G1Point point) {
    Objects.requireNonNull(point, "Point is null");
    this.point = new ECP(point.ecpPoint());
  }

  public G1Point(final ECP point) {
    Objects.requireNonNull(point, "ECP point is null");
    this.point = new ECP(point);
  }

  public G1Point add(final G1Point other) {
    Objects.requireNonNull(other, "Point to add is null");
    ECP sum = new ECP(this.point);
    sum.add(other.point);
    sum.affine();
    return new G1Point(sum);
  }

  public G1Point add(final Collection<G1Point> others) {
    Objects.requireNonNull(others, "Points to add is null");
    ECP sum = new ECP(this.point);
    for (G1Point other : others) sum.add(other.point);
    sum.affine();
    return new G1Point(sum);
  }

  public G1Point add(final G1Point... others) {
    Objects.requireNonNull(others, "Points to add is null");
    ECP sum = new ECP(this.point);
    for (G1Point other : others) sum.add(other.point);
    sum.affine();
    return new G1Point(sum);
  }

  public G1Point sub(final G1Point other) {
    Objects.requireNonNull(other, "Point to subtract is null");
    ECP sum = new ECP(this.point);
    sum.sub(other.point);
    sum.affine();
    return new G1Point(sum);
  }

  public G1Point neg() {
    ECP newPoint = new ECP(this.point);
    newPoint.neg();
    return new G1Point(newPoint);
  }

  public G1Point mul(final Scalar scalar) {
    Objects.requireNonNull(scalar, "Scalar is null");
    ECP newPoint = this.point.mul(scalar.value());
    return new G1Point(newPoint);
  }

  /**
   * @return byte[] the byte array representation of compressed point in G1
   */
  public byte[] toBytes() {
    // Size of the byte array representing compressed ECP point for BLS12-381 is
    // 49 bytes in milagro
    // size of the point = 48 bytes
    // meta information (parity bit, curve type etc) = 1 byte
    byte[] bytes = new byte[FP_POINT_SIZE + 1];
    this.point.toBytes(bytes, true);
    return bytes;
  }

  ECP ecpPoint() {
    return this.point;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    long x = this.point.getX().norm();
    long y = this.point.getY().norm();
    result = prime * result + (int) (x ^ (x >>> 32));
    result = prime * result + (int) (y ^ (y >>> 32));
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;

    G1Point other = (G1Point) obj;
    if (this.point == null) {
      if (other.point != null) return false;
    } else if (this.point.equals(other.point) == false) return false;

    return true;
  }
}
