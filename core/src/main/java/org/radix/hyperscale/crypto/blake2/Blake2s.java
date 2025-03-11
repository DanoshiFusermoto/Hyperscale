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

import java.util.Arrays;

/**
 * The BLAKE2s digest algorithm. This BLAKE2 flavor is optimized for 8 to
 * 32-bit platforms and produces digests of any size between 1 and 32 bytes.
 * This class implements the BLAKE2s algorithm as specified in RFC 7693. The
 * <a href="https://blake2.net/blake2.pdf">original paper</a> defines some
 * additional variants with features such as salting, personalization and tree
 * hashing. These features are considered optional and not covered by the RFC.
 * BLAKE2s can be directly keyed, making it functionally equivalent to a Message
 * Authentication Code (it does not require a special construction like HMAC).
 * Instances of this class are not thread safe.
 *
 * @see <a href="https://blake2.net">blake2.net</a>
 * @see <a href="https://tools.ietf.org/html/rfc7693">RFC 7693</a>
 *
 * @author Osman Koçak
 */
public final class Blake2s implements Blake2
{
	private static final int BLOCK_LENGTH = 64;
	private static final int IV[] = {
		0x6A09E667, 0XBB67AE85, 0X3C6EF372, 0xA54FF53A,
		0X510E527F, 0X9B05688C, 0x1F83D9AB, 0X5BE0CD19
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
		{10, 2, 8, 4, 7, 6, 1, 5, 15, 11, 9, 14, 3, 12, 13, 0}
	};

	private final int digestLength;
	private final byte[] buffer;
	private byte[] key;
	private int[] h; // internal state
	private int t0; // counter's LSB
	private int t1; // counter's MSB
	private int c; // number of bytes in the buffer

	/**
	 * Creates a new unkeyed {@code Blake2s} instance.
	 *
	 * @param digestLength the desired digest's length (in bytes).
	 *
	 * @throws IllegalArgumentException if {@code digestLength} is not in
	 *	the {@code [1, 32]} range.
	 */
	public Blake2s(int digestLength)
	{
		this(digestLength, new byte[0]);
	}

	/**
	 * Creates a new keyed {@code Blake2s} instance. If the key is empty,
	 * then the created instance is equivalent to an unkeyed digest. The
	 * given key can be safely erased from memory after this constructor has
	 * been called.
	 *
	 * @param digestLength the desired digest's length (in bytes).
	 * @param key the key to use.
	 *
	 * @throws NullPointerException if {@code key} is {@code null}.
	 * @throws IllegalArgumentException if {@code key}'s length is greater
	 *	than {@code 32} or if {@code digestLength} is not in the
	 *	{@code [1, 32]} range.
	 */
	public Blake2s(int digestLength, byte[] key)
	{
		Preconditions.checkArgument(key.length <= 32);
		Preconditions.checkArgument(digestLength >= 1 && digestLength <= 32);
		this.buffer = new byte[BLOCK_LENGTH];
		this.key = Arrays.copyOf(key, key.length);
		this.digestLength = digestLength;
		reset();
	}

	private Blake2s(Blake2s digest)
	{
		this.c = digest.c;
		this.buffer = Arrays.copyOf(digest.buffer, digest.buffer.length);
		this.key = Arrays.copyOf(digest.key, digest.key.length);
		this.digestLength = digest.digestLength;
		this.h = Arrays.copyOf(digest.h, digest.h.length);
		this.t0 = digest.t0;
		this.t1 = digest.t1;
	}

	@Override
	public String algorithm()
	{
		return "BLAKE2s";
	}

	@Override
	public int length()
	{
		return digestLength;
	}

	@Override
	public Blake2s copy()
	{
		return new Blake2s(this);
	}

	@Override
	public Blake2s reset()
	{
		t0 = 0;
		t1 = 0;
		h = Arrays.copyOf(IV, IV.length);
		h[0] ^= digestLength | (key.length << 8) | 0x01010000;
		if (key.length > 0) {
			System.arraycopy(key, 0, buffer, 0, key.length);
			Arrays.fill(buffer, key.length, BLOCK_LENGTH, (byte) 0);
			c = BLOCK_LENGTH;
		} else {
			c = 0;
		}
		return this;
	}

	@Override
	public Blake2s burn()
	{
		Arrays.fill(key, (byte) 0);
		Arrays.fill(buffer, (byte) 0); // buffer may contain the key...
		key = new byte[0];
		reset();
		return this;
	}

	@Override
	public Blake2s update(byte input)
	{
		if (c == BLOCK_LENGTH) {
			processBuffer(false);
		}
		buffer[c++] = input;
		return this;
	}

	@Override
	public Blake2s update(byte... input)
	{
		return update(input, 0, input.length);
	}

	@Override
	public Blake2s update(byte[] input, int off, int len)
	{
		Preconditions.checkBounds(input, off, len);
		int index = off;
		int remaining = len;
		while (remaining > 0) {
			if (c == BLOCK_LENGTH) {
				processBuffer(false);
			}
			int cpLen = Math.min(BLOCK_LENGTH - c, remaining);
			System.arraycopy(input, index, buffer, c, cpLen);
			remaining -= cpLen;
			index += cpLen;
			c += cpLen;
		}
		return this;
	}

	@Override
	public byte[] digest()
	{
		Arrays.fill(buffer, c, BLOCK_LENGTH, (byte) 0);
		processBuffer(true);
		byte[] out = new byte[digestLength];
		int i = 0;
		while (i * 4 < digestLength - 4) {
			LittleEndian.encode(h[i], out, i * 4);
			i++;
		}
		byte[] last = LittleEndian.encode(h[i]);
		System.arraycopy(last, 0, out, i * 4, digestLength - (i * 4));
		reset();
		return out;
	}

	private void processBuffer(boolean lastBlock)
	{
		t0 += c;
		if (t0 == 0 && c > 0) { // bitwise overflow
			t1++;
			Preconditions.checkState(t1 != 0);
		}
		c = 0;
		F(buffer, lastBlock);
	}

	private void F(byte[] input, boolean lastBlock)
	{
		int[] v = new int[16];
		System.arraycopy(h, 0, v, 0, h.length);
		System.arraycopy(IV, 0, v, h.length, IV.length);
		v[12] ^= t0;
		v[13] ^= t1;
		if (lastBlock) {
			v[14] = ~v[14];
		}
		int[] m = new int[16];
		for (int j = 0; j < 16; j++) {
			m[j] = LittleEndian.decodeInt(input, j * 4);
		}
		for (int i = 0; i < 10; i++) {
			G(v, 0, 4,  8, 12, m[SIGMA[i][ 0]], m[SIGMA[i][ 1]]);
			G(v, 1, 5,  9, 13, m[SIGMA[i][ 2]], m[SIGMA[i][ 3]]);
			G(v, 2, 6, 10, 14, m[SIGMA[i][ 4]], m[SIGMA[i][ 5]]);
			G(v, 3, 7, 11, 15, m[SIGMA[i][ 6]], m[SIGMA[i][ 7]]);
			G(v, 0, 5, 10, 15, m[SIGMA[i][ 8]], m[SIGMA[i][ 9]]);
			G(v, 1, 6, 11, 12, m[SIGMA[i][10]], m[SIGMA[i][11]]);
			G(v, 2, 7,  8, 13, m[SIGMA[i][12]], m[SIGMA[i][13]]);
			G(v, 3, 4,  9, 14, m[SIGMA[i][14]], m[SIGMA[i][15]]);
		}
		for (int i = 0; i < h.length; i++) {
			h[i] ^= v[i] ^ v[i + 8];
		}
	}

	private void G(int[] v, int a, int b, int c, int d, int x, int y)
	{
		v[a] += v[b] + x;
		v[d] = Integer.rotateRight(v[d] ^ v[a], 16);
		v[c] += v[d];
		v[b] = Integer.rotateRight(v[b] ^ v[c], 12);
		v[a] += v[b] + y;
		v[d] = Integer.rotateRight(v[d] ^ v[a], 8);
		v[c] += v[d];
		v[b] = Integer.rotateRight(v[b] ^ v[c], 7);
	}
}