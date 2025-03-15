package org.radix.hyperscale.ledger;

import com.google.common.primitives.UnsignedBytes;
import java.util.Objects;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.utils.UInt256;

public final class ShardID implements Comparable<ShardID> {
  public static final int BYTES = 32;
  public static final int BITS = BYTES * Byte.SIZE;
  public static final ShardID ZERO = new ShardID(new byte[BYTES], 0);

  public static ShardID from(String string) {
    return new ShardID(UInt256.from(string));
  }

  public static ShardID from(String string, int offset) {
    return new ShardID(UInt256.from(string, offset));
  }

  public static ShardID from(final Hash hash) {
    return new ShardID(hash);
  }

  public static ShardID from(final byte[] bytes) {
    return new ShardID(bytes, 0);
  }

  public static ShardID from(final byte[] bytes, final int offset) {
    return new ShardID(bytes, offset);
  }

  public static ShardID from(UInt256 value) {
    return new ShardID(value);
  }

  private byte[] data;

  private final transient int hashCode;
  private final transient long longCode;

  private ShardID(final Hash hash) {
    Objects.requireNonNull(hash, "Hash is null");

    // Hashes are immutable, so we can just take the byte array
    this.data = hash.toByteArray();

    int hashCode = 1;
    long longCode = 0;
    for (int i = 0; i < BYTES; i++) {
      hashCode = 31 * hashCode + data[i];
      longCode = (longCode << 8) | (data[i] & 0xFFL);
    }

    this.hashCode = hashCode;
    this.longCode = longCode;
  }

  private ShardID(final UInt256 value) {
    Objects.requireNonNull(value, "UInt256 is null");

    // UInt256 is immutable, so we can just take the byte array
    this.data = value.toByteArray();

    int hashCode = 1;
    long longCode = 0;
    for (int i = 0; i < BYTES; i++) {
      hashCode = 31 * hashCode + data[i];
      longCode = (longCode << 8) | (data[i] & 0xFFL);
    }

    this.hashCode = hashCode;
    this.longCode = longCode;
  }

  private ShardID(final byte[] bytes, final int offset) {
    Objects.requireNonNull(bytes, "Shard bytes is null");

    if (bytes.length < BYTES)
      throw new IllegalArgumentException(
          "Shard bytes length must be " + BYTES + " bytes for Hash, was " + data.length);

    if (offset + BYTES > bytes.length)
      throw new IllegalArgumentException(
          String.format(
              "Shard bytes length must be at least %s for offset %s, but was %s",
              offset + BYTES, offset, bytes.length));

    this.data = new byte[BYTES];

    int hashCode = 1;
    long longCode = 0;
    for (int i = 0; i < BYTES; i++) {
      hashCode = 31 * hashCode + data[i];
      longCode = (longCode << 8) | (data[i] & 0xFFL);
    }

    this.hashCode = hashCode;
    this.longCode = longCode;
  }

  @Override
  public int compareTo(final ShardID object) {
    Objects.requireNonNull(object, "Hash comparison is null");
    return UnsignedBytes.lexicographicalComparator().compare(this.data, object.data);
  }

  @Override
  public String toString() {
    return UInt256.from(this.data).toString();
  }

  @Override
  public boolean equals(final Object object) {
    if (null == object) return false;

    if (object == this) return true;

    if (object.getClass() == ShardID.class) {
      ShardID other = (ShardID) object;

      if (hashCode() == other.hashCode()) {
        if (this.data.length != other.data.length) return false;

        for (int i = 0; i < this.data.length; i++) {
          if (this.data[i] != other.data[i]) return false;
        }
      }
    }

    return false;
  }

  @Override
  public int hashCode() {
    return this.hashCode;
  }

  public long asLong() {
    return this.longCode;
  }

  public byte[] toByteArray() {
    return this.data;
  }
}
