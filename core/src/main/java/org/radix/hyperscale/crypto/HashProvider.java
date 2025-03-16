package org.radix.hyperscale.crypto;

import java.util.ArrayList;
import org.radix.hyperscale.crypto.blake2.Blake2b;

class HashProvider {
  private final ThreadLocal<Blake2b> hash256Digester;
  private final ThreadLocal<Blake2b> hash512Digester;

  HashProvider() {
    this.hash256Digester = ThreadLocal.withInitial(() -> new Blake2b(32));
    this.hash512Digester = ThreadLocal.withInitial(() -> new Blake2b(64));
  }

  /**
   * Hashes the supplied array, returning a cryptographically secure 256-bit hash.
   *
   * @param data The data to hash
   * @return The 256-bit/32-byte hash
   */
  public byte[] hash256(final byte[] data) {
    return hash256(data, 0, data.length);
  }

  /**
   * Hashes the specified portion of the array, returning a cryptographically secure 256-bit hash.
   *
   * @param data The data to hash
   * @param offset The offset within the array to start hashing data
   * @param length The number of bytes in the array to hash
   * @return The 256-bit/32-byte hash
   */
  public byte[] hash256(final byte[] data, final int offset, final int length) {
    final Blake2b hash256Digester = this.hash256Digester.get();
    hash256Digester.reset();
    hash256Digester.update(data, offset, length);

    byte[] digest = hash256Digester.digest();
    if (digest.length > 32)
      throw new IllegalStateException("Digest output is greater than 256 bits");
    return digest;
  }

  /**
   * Hashes the supplied String, returning a cryptographically secure 256-bit hash.
   *
   * @param data The String to hash
   * @return The 256-bit/32-byte hash
   */
  public byte[] hash256(final String data) {
    return hash256(data, 0, data.length());
  }

  /**
   * Hashes the specified portion of the String, returning a cryptographically secure 256-bit hash.
   *
   * @param data The String to hash
   * @param offset The offset within the array to start hashing data
   * @param length The number of bytes in the array to hash
   * @return The 256-bit/32-byte hash
   */
  public byte[] hash256(final String data, final int offset, final int length) {
    final Blake2b hash256Digester = this.hash256Digester.get();
    hash256Digester.reset();

    processString(hash256Digester, data, offset, length);

    byte[] digest = hash256Digester.digest();
    if (digest.length > 32)
      throw new IllegalStateException("Digest output is greater than 256 bits");
    return digest;
  }

  /**
   * Hashes the supplied String and byte array, returning a cryptographically secure 256-bit hash.
   * The hash is calculated as if the String characters and byte array were concatenated into a
   * single byte array.
   *
   * @param data1 The first part of the data to hash
   * @param data2 The second part of the data to hash
   * @return The 256-bit/32-byte hash
   */
  public byte[] hash256(final String data1, final byte[] data2) {
    final Blake2b hash256Digester = this.hash256Digester.get();
    hash256Digester.reset();

    processString(hash256Digester, data1, 0, data1.length());
    hash256Digester.update(data2);

    byte[] digest = hash256Digester.digest();
    if (digest.length > 32)
      throw new IllegalStateException("Digest output is greater than 256 bits");
    return digest;
  }

  /**
   * Hashes the supplied byte array and String, returning a cryptographically secure 256-bit hash.
   * The hash is calculated as if the String characters and byte array were concatenated into a
   * single byte array.
   *
   * @param data1 The first part of the data to hash
   * @param data2 The second part of the data to hash
   * @return The 256-bit/32-byte hash
   */
  public byte[] hash256(final byte[] data1, final String data2) {
    final Blake2b hash256Digester = this.hash256Digester.get();
    hash256Digester.reset();

    hash256Digester.update(data1);
    processString(hash256Digester, data2, 0, data2.length());

    byte[] digest = hash256Digester.digest();
    if (digest.length > 32)
      throw new IllegalStateException("Digest output is greater than 256 bits");
    return digest;
  }

  /**
   * Hashes the supplied Strings, returning a cryptographically secure 256-bit hash. The hash is
   * calculated as if the String bytes were contatenated into a single byte array.
   *
   * @param data1 The first part of the data to hash
   * @param data2 The second part of the data to hash
   * @return The 256-bit/32-byte hash
   */
  public byte[] hash256(final String data1, final String data2) {
    final Blake2b hash256Digester = this.hash256Digester.get();
    hash256Digester.reset();

    processString(hash256Digester, data1, 0, data1.length());
    processString(hash256Digester, data2, 0, data2.length());

    byte[] digest = hash256Digester.digest();
    if (digest.length > 32)
      throw new IllegalStateException("Digest output is greater than 256 bits");
    return digest;
  }

  /**
   * Hashes the supplied byte arrays, returning a cryptographically secure 256-bit hash. The hash is
   * calculated as if the arrays were concatenated into a single byte array.
   *
   * @param data1 The first part of the data to hash
   * @param data2 The second part of the data to hash
   * @return The 256-bit/32-byte hash
   */
  public byte[] hash256(final byte[] data1, final byte[] data2) {
    final Blake2b hash256Digester = this.hash256Digester.get();
    hash256Digester.reset();

    hash256Digester.update(data1);
    hash256Digester.update(data2);

    byte[] digest = hash256Digester.digest();
    if (digest.length > 32)
      throw new IllegalStateException("Digest output is greater than 256 bits");
    return digest;
  }

  /**
   * Hashes the supplied arrays, returning a cryptographically secure 256-bit hash. The hash is
   * calculated as if the arrays were concatenated into a single array.
   *
   * @param data The array of byte arrays
   * @return The 256-bit/32-byte hash
   */
  public byte[] hash256(final byte[]... data) {
    final Blake2b hash256Digester = this.hash256Digester.get();
    hash256Digester.reset();
    for (int i = 0; i < data.length; i++) hash256Digester.update(data[i]);

    byte[] digest = hash256Digester.digest();
    if (digest.length > 32)
      throw new IllegalStateException("Digest output is greater than 256 bits");
    return digest;
  }

  /**
   * Hashes the supplied List of byte arrays, returning a cryptographically secure 256-bit hash. The
   * hash is calculated as if the items in the list were concatenated into a single array.
   *
   * @param data The List of byte arrays to hash
   * @return The 256-bit/32-byte hash
   */
  public byte[] hash256(final ArrayList<byte[]> data) {
    final Blake2b hash256Digester = this.hash256Digester.get();
    hash256Digester.reset();
    for (int i = 0; i < data.size(); i++) hash256Digester.update(data.get(i));

    byte[] digest = hash256Digester.digest();
    if (digest.length > 32)
      throw new IllegalStateException("Digest output is greater than 256 bits");
    return digest;
  }

  /**
   * Hashes the specified portion of the array, returning a cryptographically secure 512-bit hash.
   *
   * @param data The data to hash
   * @param offset The offset within the array to start hashing data
   * @param length The number of bytes in the array to hash
   * @return The 512-bit/64-byte hash
   */
  public byte[] hash512(final byte[] data, final int offset, final int length) {
    final Blake2b hash512Digester = this.hash512Digester.get();
    hash512Digester.reset();
    hash512Digester.update(data, offset, length);
    return hash512Digester.digest();
  }

  private void processString(
      final Blake2b hashDigester, final String data, final int offset, final int length) {
    for (int i = offset; i < offset + length; i++) {
      char c = data.charAt(i);
      if (c < 128) { // ASCII character - single byte
        hashDigester.update((byte) c);
      } else if (c < 0x800) { // 2-byte UTF-8
        hashDigester.update((byte) (0xC0 | (c >> 6)));
        hashDigester.update((byte) (0x80 | (c & 0x3F)));
      } else { // 3-byte UTF-8
        hashDigester.update((byte) (0xE0 | (c >> 12)));
        hashDigester.update((byte) (0x80 | ((c >> 6) & 0x3F)));
        hashDigester.update((byte) (0x80 | (c & 0x3F)));
      }
    }
  }
}
