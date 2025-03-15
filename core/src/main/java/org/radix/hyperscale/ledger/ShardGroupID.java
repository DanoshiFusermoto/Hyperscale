package org.radix.hyperscale.ledger;

import java.util.Objects;
import org.radix.hyperscale.utils.Ints;
import org.radix.hyperscale.utils.Numbers;

public final class ShardGroupID extends Number implements Comparable<ShardGroupID> {
  // A network with more than 4096 shards is HUGE.  Throughput capabilities in excess of 20M per
  // second.  Large enough for now.
  private static volatile ShardGroupID[] shardGroupIDs = new ShardGroupID[4096];

  static {
    for (int sg = 0; sg < shardGroupIDs.length; sg++) shardGroupIDs[sg] = new ShardGroupID(sg);
  }

  private static final ShardGroupID cached(int value) {
    ShardGroupID cached = shardGroupIDs[value];

    // TODO grow if null
    if (cached == null) return new ShardGroupID(value);

    return cached;
  }

  /** */
  private static final long serialVersionUID = -7990272741032722231L;

  public static final int MIN_VALUES = 0x00000000;
  public static final int MAX_VALUES = 0x7fffffff;

  public static final int SIZE = 32;
  public static final int BYTES = SIZE / Byte.SIZE;

  public static ShardGroupID from(String string) {
    Objects.requireNonNull(string, "Shard group ID string is null");
    int value = Integer.parseInt(string);
    Numbers.isNegative(value, "Shard group ID is negative");
    return cached(value);
  }

  public static ShardGroupID from(final int value) {
    Numbers.isNegative(value, "Shard group ID is negative");
    return cached(value);
  }

  public static ShardGroupID from(final long value) {
    Numbers.isNegative(value, "Shard group ID is negative");
    Numbers.greaterThan(
        value, Integer.MAX_VALUE, "Shard group ID value is greater than Integer.MAX");
    return cached((int) value);
  }

  public static ShardGroupID from(final byte[] bytes) {
    Objects.requireNonNull(bytes, "Shard group ID bytes is null");
    int value = Ints.fromByteArray(bytes);
    Numbers.isNegative(value, "Shard group ID is negative");
    return cached(value);
  }

  public static ShardGroupID from(final byte[] bytes, int offset) {
    Objects.requireNonNull(bytes, "Shard group ID bytes is null");
    int value = Ints.fromByteArray(bytes, offset);
    Numbers.isNegative(value, "Shard group ID is negative");
    return cached(value);
  }

  private final int value;

  private ShardGroupID(final int value) {
    Numbers.inRange(
        value,
        MIN_VALUES,
        MAX_VALUES,
        "Shard group ID is not within range " + MIN_VALUES + " -> " + MAX_VALUES);
    this.value = value;
  }

  public int intValue() {
    return this.value;
  }

  public long longValue() {
    return this.value;
  }

  public float floatValue() {
    return this.value;
  }

  public double doubleValue() {
    return this.value;
  }

  public String toString() {
    return Integer.toString(this.value);
  }

  @Override
  public int hashCode() {
    return this.value;
  }

  public boolean equals(Object obj) {
    if (obj == null) return false;
    if (obj == this) return true;

    if (obj instanceof ShardGroupID shardGroupID) return this.value == shardGroupID.intValue();

    return false;
  }

  public int compareTo(ShardGroupID other) {
    return Integer.compare(this.value, other.value);
  }
}
