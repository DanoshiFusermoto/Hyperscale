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

/**
 * Common interface for BLAKE2 flavors.
 *
 * @see Blake2b
 * @see Blake2s
 * @see <a href="https://blake2.net">blake2.net</a>
 * @see <a href="https://tools.ietf.org/html/rfc7693">RFC 7693</a>
 * @author Osman Koçak
 */
public interface Blake2 {
  /**
   * Returns the algorithm name.
   *
   * @return the algorithm name.
   */
  String algorithm();

  /**
   * Returns the digest's length (in bytes).
   *
   * @return the digest's length (in bytes).
   */
  int length();

  /**
   * Returns a copy of this object.
   *
   * @return a copy of this object.
   */
  Blake2 copy();

  /**
   * Resets the digest. Note that this does not erase the key.
   *
   * @return this object.
   */
  Blake2 reset();

  /**
   * Erases the key and resets the digest, thus making this instance functionally equivalent to a
   * newly created unkeyed digest. If this instance is already unkeyed, calling this method has no
   * other effect than resetting the digest. Please note that this method can only guarantee that
   * the active copy of the key is erased from memory. Indeed, in practice, an object may have
   * several copies of itself in physical RAM (garbage collection algorithms tend to regularly
   * "move" objects in memory).
   *
   * @return this object.
   */
  Blake2 burn();

  /**
   * Updates the digest using the given byte.
   *
   * @param input the byte with which to update the digest.
   * @return this object.
   */
  Blake2 update(byte input);

  /**
   * Updates the digest using the given array of bytes.
   *
   * @param input the array of bytes with which to update the digest.
   * @return this object.
   * @throws NullPointerException if {@code input} is {@code null}.
   */
  Blake2 update(byte... input);

  /**
   * Updates the digest using the specified number of bytes from the given array, starting at the
   * specified offset.
   *
   * @param input the array of bytes.
   * @param off the offset to start from in the array, inclusive.
   * @param len the number of bytes to use, starting at {@code off}.
   * @return this object.
   * @throws NullPointerException if {@code input} is {@code null}.
   * @throws IllegalArgumentException if {@code off < 0 || len < 0}.
   * @throws IndexOutOfBoundsException if {@code off + len > input.length}.
   */
  Blake2 update(byte[] input, int off, int len);

  /**
   * Performs a final update on the digest using the specified array of bytes, then completes the
   * digest computation. That is, this method first calls {@link #update(byte...)}, passing the
   * input array to the update method, then calls {@link #digest()}. Note that the digest is reset
   * after this call is made.
   *
   * @param input the array of bytes with which to update the digest before completing its
   *     computation.
   * @return the resulting digest.
   * @throws NullPointerException if {@code input} is {@code null}.
   */
  default byte[] digest(byte... input) {
    return digest(input, 0, input.length);
  }

  /**
   * Performs a final update on the digest using the specified data bytes, then completes the digest
   * computation. That is, this method first calls {@link #update(byte[], int, int)}, passing the
   * input array to the update method, then calls {@link #digest()}. Note that the digest is reset
   * after this call is made.
   *
   * @param input the array of bytes.
   * @param off the offset to start from in the array, inclusive.
   * @param len the number of bytes to use, starting at {@code off}.
   * @return the resulting digest.
   * @throws NullPointerException if {@code input} is {@code null}.
   * @throws IllegalArgumentException if {@code off < 0 || len < 0}.
   * @throws IndexOutOfBoundsException if {@code off + len > input.length}.
   */
  default byte[] digest(byte[] input, int off, int len) {
    return update(input, off, len).digest();
  }

  /**
   * Completes the digest computation. Note that this digest is reset after this call is made.
   *
   * @return the resulting digest.
   */
  byte[] digest();
}
