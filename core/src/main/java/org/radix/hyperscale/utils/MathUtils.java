package org.radix.hyperscale.utils;

import com.sleepycat.je.utilint.Pair;
import java.math.BigInteger;

public class MathUtils {
  private MathUtils() {
    throw new IllegalStateException("Can't construct");
  }

  public static final long EWMA(long value, long increment, double ratio) {
    return (long) (((1.0 - ratio) * value) + (ratio * increment));
  }

  public static final int log2(int value) {
    return (Integer.SIZE - 1) - Integer.numberOfLeadingZeros(value);
  }

  public static final int log2(long value) {
    return (Long.SIZE - 1) - Long.numberOfLeadingZeros(value);
  }

  public static final int roundUpBase2(int value) {
    value--;
    value |= value >> 1;
    value |= value >> 2;
    value |= value >> 4;
    value |= value >> 8;
    value |= value >> 16;
    value++;
    return value;
  }

  public static final int roundDownBase2(int value) {
    value |= value >> 1;
    value |= value >> 2;
    value |= value >> 4;
    value |= value >> 8;
    value |= value >> 16;
    return value;
  }

  private static final double RING_SIZE_64 = BigInteger.valueOf(1).shiftLeft(64).doubleValue();
  private static final double HALF_RING_SIZE_64 = BigInteger.valueOf(1).shiftLeft(63).doubleValue();

  // TODO much faster as an approximation using doubles, but more accurate if BigInt, convert?
  public static final long ringDistance(long origin, long point, int min, int max) {
    long ringSize = max - min;
    long halfRingSize = ringSize / 2;
    double o = origin + halfRingSize;
    double p = point + halfRingSize;
    double delta = p - o;
    double absdelta = Math.abs(delta);

    double arc_1_dist = absdelta;
    double arc_2_dist = ringSize - absdelta;

    if (arc_1_dist - arc_2_dist <= 0) return (long) arc_1_dist;
    else return (long) arc_2_dist;
  }

  public static final long ringDistance64(long origin, long point) {
    double o = origin + HALF_RING_SIZE_64;
    double p = point + HALF_RING_SIZE_64;
    double delta = p - o;
    double absdelta = Math.abs(delta);

    double arc_1_dist = absdelta;
    double arc_2_dist = RING_SIZE_64 - absdelta;

    if (arc_1_dist - arc_2_dist <= 0) return (long) arc_1_dist;
    else return (long) arc_2_dist;
  }

  public static final Pair<Double, Boolean> ringArc64(long origin, long point, boolean minimum) {
    double o = origin + HALF_RING_SIZE_64;
    double p = point + HALF_RING_SIZE_64;
    double delta = p - o;

    Pair<Double, Boolean> arc_1 =
        new Pair<>(Math.abs(delta), Math.signum(delta) >= 0 ? true : false);
    Pair<Double, Boolean> arc_2 = new Pair<>(RING_SIZE_64 - Math.abs(delta), !arc_1.second());

    if (arc_1.first().compareTo(arc_2.first()) <= 0) return minimum == true ? arc_1 : arc_2;
    else return minimum == true ? arc_2 : arc_1;
  }
}
