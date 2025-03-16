package org.radix.hyperscale.crypto;

import com.google.common.primitives.UnsignedBytes;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import org.radix.hyperscale.utils.Bytes;
import org.radix.hyperscale.utils.Longs;
import org.radix.hyperscale.utils.UInt256;

public final class Hash implements Comparable<Hash> {
  // TODO stream to digester rather than batching into arrays / lists

  private static final Comparator<byte[]> COMPARATOR = UnsignedBytes.lexicographicalComparator();
  private static final SecureRandom secureRandom = new SecureRandom();
  private static HashProvider hashProvider = new HashProvider();

  public static final int BYTES = 32;
  public static final int BITS = BYTES * Byte.SIZE;
  public static final Hash ZERO = new Hash(new byte[BYTES]);
  public static final Hash MAX =
      new Hash(
          new byte[] {
            (byte) 0xff,
            (byte) 0xff,
            (byte) 0xff,
            (byte) 0xff,
            (byte) 0xff,
            (byte) 0xff,
            (byte) 0xff,
            (byte) 0xff,
            (byte) 0xff,
            (byte) 0xff,
            (byte) 0xff,
            (byte) 0xff,
            (byte) 0xff,
            (byte) 0xff,
            (byte) 0xff,
            (byte) 0xff,
            (byte) 0xff,
            (byte) 0xff,
            (byte) 0xff,
            (byte) 0xff,
            (byte) 0xff,
            (byte) 0xff,
            (byte) 0xff,
            (byte) 0xff,
            (byte) 0xff,
            (byte) 0xff,
            (byte) 0xff,
            (byte) 0xff,
            (byte) 0xff,
            (byte) 0xff,
            (byte) 0xff,
            (byte) 0xff
          });

  public static void notZero(final Hash hash) {
    notZero(hash, "Hash is ZERO");
  }

  public static void notZero(final Hash hash, final String message) {
    // This method should be faster than using an equals
    byte[] bytes = hash.data;
    for (int i = bytes.length - 1; i >= 0; i--) if (bytes[i] != 0) return;

    throw new IllegalArgumentException(message);
  }

  public static Hash random() {
    byte[] randomBytes = new byte[BYTES];
    secureRandom.nextBytes(randomBytes);
    return hash(randomBytes);
  }

  public static <T> Hash valueOf(final List<T> objects, final Function<T, byte[]> byteMapper) {
    Objects.requireNonNull(byteMapper, "Byte mapping function is null");
    if (Objects.requireNonNull(objects, "Objects is null").isEmpty())
      throw new IllegalArgumentException("Objects is empty");

    ArrayList<byte[]> data = new ArrayList<>(objects.size());
    for (T object : objects) data.add(byteMapper.apply(object));

    return hash(data);
  }

  public static Hash valueOf(final Object object) {
    Objects.requireNonNull(object, "Object is null");

    if (Hashable.class.isAssignableFrom(object.getClass())) return ((Hashable) object).getHash();

    if (Hash.class == object.getClass()) return ((Hash) object);

    if (UInt256.class == object.getClass()) return hash(((UInt256) object).toByteArray());

    if (String.class == object.getClass()) return hash((String) object);

    if (Number.class.isAssignableFrom(object.getClass()))
      return hash(Longs.toByteArray(((Number) object).longValue()));

    if (byte[].class == object.getClass()) return hash((byte[]) object);

    if (Collection.class.isAssignableFrom(object.getClass())) {
      Collection<?> objects = (Collection<?>) object;
      ArrayList<byte[]> data = new ArrayList<>(objects.size());
      for (Object child : objects) {
        if (child instanceof Hash hash) data.add(hash.toByteArray());
        else data.add(Hash.valueOf(child).toByteArray());
      }

      return hash(data);
    }

    throw new IllegalArgumentException(
        "Objects of type " + object.getClass() + " not supported for Hash");
  }

  public static Hash hash(final Hash... hashes) {
    if (Objects.requireNonNull(hashes, "Hashes is null").length == 0)
      throw new IllegalArgumentException("Hashes is empty");

    ArrayList<byte[]> data = new ArrayList<>(hashes.length);
    for (int i = 0; i < hashes.length; i++) data.add(hashes[i].toByteArray());

    return hash(data);
  }

  public static Hash hash(final byte[] data) {
    byte[] digest = Hash.hashProvider.hash256(data);
    return new Hash(digest);
  }

  public static Hash hash(final byte[] data, int offset, int length) {
    byte[] digest = Hash.hashProvider.hash256(data, offset, length);
    return new Hash(digest);
  }

  public static Hash hash(final Hash hash1, final Hash hash2) {
    byte[] digest = Hash.hashProvider.hash256(hash1.toByteArray(), hash2.toByteArray());
    return new Hash(digest);
  }

  public static Hash hash(final byte[] data1, final byte[] data2) {
    byte[] digest = Hash.hashProvider.hash256(data1, data2);
    return new Hash(digest);
  }

  public static Hash hash(final String data1, final byte[] data2) {
    byte[] digest = Hash.hashProvider.hash256(data1, data2);
    return new Hash(digest);
  }

  public static Hash hash(final byte[] data1, final String data2) {
    byte[] digest = Hash.hashProvider.hash256(data1, data2);
    return new Hash(digest);
  }

  public static Hash hash(final String data1, final String data2) {
    byte[] digest = Hash.hashProvider.hash256(data1, data2);
    return new Hash(digest);
  }

  public static Hash hash(final byte[]... data) {
    byte[] digest = Hash.hashProvider.hash256(data);
    return new Hash(digest);
  }

  public static Hash hash(final ArrayList<byte[]> data) {
    byte[] digest = Hash.hashProvider.hash256(data);
    return new Hash(digest);
  }

  public static Hash hash(final String data) {
    byte[] digest = Hash.hashProvider.hash256(data);
    return new Hash(digest);
  }

  public static Hash hash(final String data, int offset, int length) {
    byte[] digest = Hash.hashProvider.hash256(data, offset, length);
    return new Hash(digest);
  }

  public static Hash from(final byte[] digest) {
    return from(digest, 0);
  }

  public static Hash from(final byte[] digest, final int offset) {
    return new Hash(digest, offset);
  }

  public static Hash from(final String hex) {
    return from(hex, 0);
  }

  public static Hash from(final String hex, int offset) {
    Objects.requireNonNull(hex, "Hash hex string is null");
    if ((hex.length() - offset) != (BYTES * 2))
      throw new IllegalArgumentException(
          String.format(
              "Digest length must be %s hex characters for Hash, was %s",
              BYTES * 2, (hex.length() - offset)));

    if (hex.startsWith("0x")) offset += 2;

    byte[] digest = Bytes.fromHexString(hex, offset);
    return new Hash(digest);
  }

  private final byte[] data;
  private final long cachedCode;

  private Hash(final byte[] digest) {
    Objects.requireNonNull(digest, "Hash digest is null");

    if (digest.length != BYTES)
      throw new IllegalArgumentException(
          "Digest length must be " + BYTES + " bytes for Hash, was " + digest.length);

    long hashCode = 17l;
    this.data = digest;
    for (int i = 0; i < BYTES; i++) hashCode = 31l * hashCode + digest[i];

    this.cachedCode = hashCode;
  }

  private Hash(final byte[] digest, final int offset) {
    Objects.requireNonNull(digest, "Hash bytes is null");

    if (digest.length < BYTES)
      throw new IllegalArgumentException(
          "Digest length must be " + BYTES + " bytes for Hash, was " + digest.length);

    if (offset + BYTES > digest.length)
      throw new IllegalArgumentException(
          String.format(
              "Hash length must be at least %s for offset %s, but was %s",
              offset + BYTES, offset, digest.length));

    long hashCode = 17l;
    this.data = new byte[BYTES];
    for (int i = 0; i < BYTES; i++) {
      this.data[i] = digest[offset + i];
      hashCode = 31l * hashCode + digest[offset + i];
    }

    this.cachedCode = hashCode;
  }

  /**
   * Retrieve the hash bytes.
   *
   * <p>Note that for performance reasons, the underlying array is returned. If callers are passing
   * this array to mutating methods, a copy should be taken.
   *
   * @return The hash data
   */
  public byte[] toByteArray() {
    return this.data;
  }

  public int trailingZeroBits() {
    for (int pos = Hash.BYTES - 1; pos >= 0; pos--) {
      if (this.data[pos] != 0) {
        for (int nbits = 7; nbits > 0; nbits--) {
          if ((this.data[pos] & 1 << nbits) != 0) return 8 * pos + nbits + 1;
        }
        return 8 * pos + 1;
      }
    }
    return 0;
  }

  public int leadingZeroBits() {
    for (int pos = 0; pos < Hash.BYTES; pos++) {
      if (this.data[pos] != 0) {
        for (int nbits = 0; nbits < 8; nbits++) {
          if ((this.data[pos] & 1 << (7 - nbits)) != 0) return 8 * pos + nbits;
        }
        return 8 * pos;
      }
    }
    return 0;
  }

  public void clearLeadingBits(int count, boolean terminate) {
    int index = 0;
    for (int pos = 0; pos < Hash.BYTES; pos++) {
      for (int nbits = 0; nbits < 8; nbits++) {
        if (index == count && terminate) this.data[pos] |= (byte) (1 << (7 - nbits));
        else this.data[pos] &= (byte) ~(1 << (7 - nbits));

        index++;

        if (index == count + (terminate ? 1 : 0)) return;
      }
    }
  }

  public Hash invert() {
    byte[] inverted = new byte[Hash.BYTES];
    for (int i = 0; i < Hash.BYTES; i++) inverted[i] = (byte) (0xFF - (this.data[i] & 0xFF));

    return new Hash(inverted);
  }

  public void copyTo(final byte[] array, final int offset) {
    copyTo(array, offset, BYTES);
  }

  public void copyTo(final byte[] array, final int offset, final int length) {
    Objects.requireNonNull(array, "Hash destination array is null");

    if (array.length - offset < BYTES)
      throw new IllegalArgumentException(
          String.format("Array must be bigger than offset + %d but was %d", BYTES, array.length));

    System.arraycopy(this.data, 0, array, offset, length);
  }

  public byte getFirstByte() {
    return data[0];
  }

  @Override
  public int compareTo(final Hash object) {
    Objects.requireNonNull(object, "Hash comparison is null");
    return COMPARATOR.compare(this.data, object.data);
  }

  @Override
  public String toString() {
    return Bytes.toHexString(this.data);
  }

  public String toBinary() {
    StringBuilder sb = new StringBuilder(this.data.length * Byte.SIZE);
    for (int i = 0; i < Byte.SIZE * this.data.length; i++)
      sb.append((this.data[i / Byte.SIZE] << i % Byte.SIZE & 0x80) == 0 ? '0' : '1');
    return sb.toString();
  }

  @Override
  public boolean equals(final Object object) {
    if (null == object) return false;

    if (object == this) return true;

    if (object instanceof Hash other) {
      if (this.cachedCode != other.cachedCode) return false;

      if (this.data.length != other.data.length) return false;

      return Arrays.equals(this.data, other.data);
    }

    return false;
  }

  @Override
  public int hashCode() {
    return (int) this.cachedCode;
  }

  public long asLong() {
    return this.cachedCode;
  }
}
