/*----------------------------------------------------------------------------*
 * This file is part of JBlake2.                                              *
 * Copyright (C) Osman Koçak <kocakosm@gmail.com>                             *
 *                                                                            *
 * This program is free software: you can redistribute it and/or modify it    *
 * under the terms of the GNU Lesser General Public License as published by   *
 * the Free Software Foundation, either version 3 of the License, or (at your *
 * option) any later version.                                                 *
 * This program is distributed in the hope that it will be useful, but        *
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY *
 * or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public     *
 * License for more details.                                                  *
 * You should have received a copy of the GNU Lesser General Public License   *
 * along with this program. If not, see <http://www.gnu.org/licenses/>.       *
 *----------------------------------------------------------------------------*/

package org.radix.hyperscale.crypto.blake2;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * The BLAKE2b digest algorithm. This BLAKE2 flavor is highly optimized for 64-bit platforms and
 * produces digests of any size between 1 and 64 bytes. This class implements the BLAKE2b algorithm
 * as specified in RFC 7693. The <a href="https://blake2.net/blake2.pdf">original paper</a> defines
 * some additional variants with features such as salting, personalization and tree hashing. These
 * features are considered optional and not covered by the RFC. BLAKE2b can be directly keyed,
 * making it functionally equivalent to a Message Authentication Code (it does not require a special
 * construction like HMAC). Instances of this class are not thread safe.
 *
 * @see <a href="https://blake2.net">blake2.net</a>
 * @see <a href="https://tools.ietf.org/html/rfc7693">RFC 7693</a>
 * @author Osman Koçak
 */
public final class Blake2b implements Blake2 {
  private static final VarHandle LONG_HANDLE =
      MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

  private static final int BLOCK_LENGTH = 128;
  private static final byte[] EMPTY_BLOCK = new byte[BLOCK_LENGTH];

  static {
    Arrays.fill(EMPTY_BLOCK, (byte) 0);
  }

  private static final long IV[] = {
    0X6A09E667F3BCC908L, 0XBB67AE8584CAA73BL, 0X3C6EF372FE94F82BL,
    0XA54FF53A5F1D36F1L, 0X510E527FADE682D1L, 0X9B05688C2B3E6C1FL,
    0X1F83D9ABFB41BD6BL, 0X5BE0CD19137E2179L
  };
  private static final int[][] SIGMA = {
    {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
    {14, 10, 4, 8, 9, 15, 13, 6, 1, 12, 0, 2, 11, 7, 5, 3},
    {11, 8, 12, 0, 5, 2, 15, 13, 10, 14, 3, 6, 7, 1, 9, 4},
    {7, 9, 3, 1, 13, 12, 11, 14, 2, 6, 5, 10, 4, 0, 15, 8},
    {9, 0, 5, 7, 2, 4, 10, 15, 14, 1, 11, 12, 6, 8, 3, 13},
    {2, 12, 6, 10, 0, 11, 8, 3, 4, 13, 7, 5, 15, 14, 1, 9},
    {12, 5, 1, 15, 14, 13, 4, 10, 0, 7, 6, 3, 9, 2, 8, 11},
    {13, 11, 7, 14, 12, 1, 3, 9, 5, 0, 15, 4, 8, 6, 2, 10},
    {6, 15, 14, 9, 11, 3, 0, 8, 12, 2, 13, 7, 1, 4, 10, 5},
    {10, 2, 8, 4, 7, 6, 1, 5, 15, 11, 9, 14, 3, 12, 13, 0},
    {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
    {14, 10, 4, 8, 9, 15, 13, 6, 1, 12, 0, 2, 11, 7, 5, 3}
  };

  private final int digestLength;
  private final byte[] buffer;
  private byte[] key;
  private long[] h; // internal state
  private long t0; // counter's LSB
  private long t1; // counter's MSB
  private int c; // number of bytes in the buffer

  // Pre-allocated arrays for F function
  private final long[] v = new long[16];
  private final long[] m = new long[16];

  /**
   * Creates a new unkeyed {@code Blake2b} instance.
   *
   * @param digestLength the desired digest's length (in bytes).
   * @throws IllegalArgumentException if {@code digestLength} is not in the {@code [1, 64]} range.
   */
  public Blake2b(int digestLength) {
    this(digestLength, new byte[0]);
  }

  /**
   * Creates a new keyed {@code Blake2b} instance. If the key is empty, then the created instance is
   * equivalent to an unkeyed digest. The given key can be safely erased from memory after this
   * constructor has been called.
   *
   * @param digestLength the desired digest's length (in bytes).
   * @param key the key to use.
   * @throws NullPointerException if {@code key} is {@code null}.
   * @throws IllegalArgumentException if {@code key}'s length is greater than {@code 64} or if
   *     {@code digestLength} is not in the {@code [1, 64]} range.
   */
  public Blake2b(int digestLength, byte[] key) {
    Preconditions.checkArgument(key.length <= 64);
    Preconditions.checkArgument(digestLength >= 1 && digestLength <= 64);
    this.buffer = new byte[BLOCK_LENGTH];
    this.key = Arrays.copyOf(key, key.length);
    this.digestLength = digestLength;
    this.h = new long[IV.length];
    this.t0 = -1;
    this.t1 = -1;
    reset();
  }

  private Blake2b(Blake2b digest) {
    this.c = digest.c;
    this.buffer = Arrays.copyOf(digest.buffer, digest.buffer.length);
    this.key = Arrays.copyOf(digest.key, digest.key.length);
    this.digestLength = digest.digestLength;
    this.h = Arrays.copyOf(digest.h, digest.h.length);
    this.t0 = digest.t0;
    this.t1 = digest.t1;
  }

  @Override
  public String algorithm() {
    return "BLAKE2b";
  }

  @Override
  public int length() {
    return digestLength;
  }

  @Override
  public Blake2b copy() {
    return new Blake2b(this);
  }

  @Override
  public Blake2b reset() {
    // Already reset?
    if (t0 == 0 && t1 == 0) return this;

    t0 = 0L;
    t1 = 0L;

    h[0] = IV[0];
    h[1] = IV[1];
    h[2] = IV[2];
    h[3] = IV[3];
    h[4] = IV[4];
    h[5] = IV[5];
    h[6] = IV[6];
    h[7] = IV[7];
    h[0] ^= digestLength | (key.length << 8) | 0x01010000;

    if (key.length > 0) {
      System.arraycopy(key, 0, buffer, 0, key.length);
      Arrays.fill(buffer, key.length, BLOCK_LENGTH, (byte) 0);
      c = BLOCK_LENGTH;
    } else c = 0;

    return this;
  }

  @Override
  public Blake2b burn() {
    Arrays.fill(key, (byte) 0);
    Arrays.fill(buffer, (byte) 0); // buffer may contain the key...
    key = new byte[0];
    reset();
    return this;
  }

  @Override
  public Blake2b update(byte input) {
    if (c == BLOCK_LENGTH) processBuffer(false);

    buffer[c++] = input;
    return this;
  }

  @Override
  public Blake2b update(byte... input) {
    return update(input, 0, input.length);
  }

  @Override
  public Blake2b update(byte[] input, int off, int len) {
    Preconditions.checkBounds(input, off, len);

    int index = off;
    int remaining = len;

    // Optimization for small payloads
    if (c == 0 && remaining <= BLOCK_LENGTH && key.length == 0) {
      System.arraycopy(input, index, buffer, 0, remaining);
      c = remaining;
    } else {
      while (remaining > 0) {
        if (c == BLOCK_LENGTH) processBuffer(false);

        int cpLen = Math.min(BLOCK_LENGTH - c, remaining);
        System.arraycopy(input, index, buffer, c, cpLen);
        remaining -= cpLen;
        index += cpLen;
        c += cpLen;
      }
    }

    return this;
  }

  @Override
  public byte[] digest() {
    if (c <= BLOCK_LENGTH && t0 == 0 && t1 == 0) {
      // Single block case
      t0 = c;
      F(buffer, true);
    } else {
      // Multiple blocks case
      processBuffer(true);
    }

    byte[] out = new byte[digestLength];
    // Optimization for 32 byte hashes
    if (digestLength == 32) {
      LONG_HANDLE.set(out, 0, h[0]);
      LONG_HANDLE.set(out, 8, h[1]);
      LONG_HANDLE.set(out, 16, h[2]);
      LONG_HANDLE.set(out, 24, h[3]);
    } else {
      int i = 0;
      while (i * 8 < digestLength - 8) {
        LONG_HANDLE.set(out, i * 8, h[i]);
        i++;
      }

      byte[] last = new byte[8];
      LONG_HANDLE.set(last, i * 8, h[i]);
      System.arraycopy(last, 0, out, i * 8, digestLength - (i * 8));
    }

    reset();

    return out;
  }

  private void processBuffer(boolean lastBlock) {
    t0 += c;
    if (t0 == 0L && c > 0) { // bitwise overflow
      t1++;
      Preconditions.checkState(t1 != 0L);
    }

    F(buffer, lastBlock);
    c = 0;
  }

  private void F(byte[] input, boolean lastBlock) {
    v[0] = h[0];
    v[1] = h[1];
    v[2] = h[2];
    v[3] = h[3];
    v[4] = h[4];
    v[5] = h[5];
    v[6] = h[6];
    v[7] = h[7];
    v[8] = IV[0];
    v[9] = IV[1];
    v[10] = IV[2];
    v[11] = IV[3];
    v[12] = IV[4] ^ t0;
    v[13] = IV[5] ^ t1;
    v[14] = lastBlock ? ~IV[6] : IV[6];
    v[15] = IV[7];

    // Check if it's a partial block
    if (c <= BLOCK_LENGTH) {
      int fullLongs = c / 8;
      for (int i = 0; i < fullLongs; i++) m[i] = (long) LONG_HANDLE.get(input, i * 8);

      if (c % 8 != 0) {
        // Handle partial long at the end because we avoided the zero
        // padding buffer fill for single block payloads.
        long partialLong = 0;
        int start = fullLongs * 8;
        for (int i = 0; i < c % 8; i++) partialLong |= (long) (input[start + i] & 0xFF) << (8 * i);

        m[fullLongs] = partialLong;
      }

      // Zero-fill the rest of m
      for (int i = (c + 7) / 8; i < 16; i++) m[i] = 0;
    } else {
      m[0] = (long) LONG_HANDLE.get(input, 0);
      m[1] = (long) LONG_HANDLE.get(input, 8);
      m[2] = (long) LONG_HANDLE.get(input, 16);
      m[3] = (long) LONG_HANDLE.get(input, 24);
      m[4] = (long) LONG_HANDLE.get(input, 32);
      m[5] = (long) LONG_HANDLE.get(input, 40);
      m[6] = (long) LONG_HANDLE.get(input, 48);
      m[7] = (long) LONG_HANDLE.get(input, 56);
      m[8] = (long) LONG_HANDLE.get(input, 64);
      m[9] = (long) LONG_HANDLE.get(input, 72);
      m[10] = (long) LONG_HANDLE.get(input, 80);
      m[11] = (long) LONG_HANDLE.get(input, 88);
      m[12] = (long) LONG_HANDLE.get(input, 96);
      m[13] = (long) LONG_HANDLE.get(input, 104);
      m[14] = (long) LONG_HANDLE.get(input, 112);
      m[15] = (long) LONG_HANDLE.get(input, 120);
    }

    int[] SI;
    for (int i = 0; i < 12; i++) {
      SI = SIGMA[i];
      G(v, 0, 4, 8, 12, m[SI[0]], m[SI[1]]);
      G(v, 1, 5, 9, 13, m[SI[2]], m[SI[3]]);
      G(v, 2, 6, 10, 14, m[SI[4]], m[SI[5]]);
      G(v, 3, 7, 11, 15, m[SI[6]], m[SI[7]]);
      G(v, 0, 5, 10, 15, m[SI[8]], m[SI[9]]);
      G(v, 1, 6, 11, 12, m[SI[10]], m[SI[11]]);
      G(v, 2, 7, 8, 13, m[SI[12]], m[SI[13]]);
      G(v, 3, 4, 9, 14, m[SI[14]], m[SI[15]]);
    }

    h[0] ^= v[0] ^ v[8];
    h[1] ^= v[1] ^ v[9];
    h[2] ^= v[2] ^ v[10];
    h[3] ^= v[3] ^ v[11];
    h[4] ^= v[4] ^ v[12];
    h[5] ^= v[5] ^ v[13];
    h[6] ^= v[6] ^ v[14];
    h[7] ^= v[7] ^ v[15];
  }

  private void G(long[] v, int a, int b, int c, int d, long x, long y) {
    v[a] += v[b] + x;
    v[d] = Long.rotateRight(v[d] ^ v[a], 32);
    v[c] += v[d];
    v[b] = Long.rotateRight(v[b] ^ v[c], 24);
    v[a] += v[b] + y;
    v[d] = Long.rotateRight(v[d] ^ v[a], 16);
    v[c] += v[d];
    v[b] = Long.rotateRight(v[b] ^ v[c], 63);
  }
}
