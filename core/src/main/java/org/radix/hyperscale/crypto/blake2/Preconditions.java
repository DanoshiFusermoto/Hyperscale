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
 * Static utility methods to check whether preconditions required for the
 * execution of a method are met.
 *
 * @author Osman Koçak
 */
final class Preconditions
{
	/**
	 * Checks the truth of the given condition checking arguments validity.
	 *
	 * @param condition the boolean condition to test.
	 *
	 * @throws IllegalArgumentException if the condition is {@code false}.
	 */
	static void checkArgument(boolean condition)
	{
		if (!condition) {
			throw new IllegalArgumentException();
		}
	}

	/**
	 * Checks that the specified range is valid and within the given array's
	 * bounds.
	 *
	 * @param buf the array.
	 * @param off the offset of the range in the array, inclusive.
	 * @param len the length of the range, starting at {@code off}.
	 *
	 * @throws IllegalArgumentException if {@code off < 0 || len < 0}.
	 * @throws IndexOutOfBoundsException if {@code off + len > buf.length}.
	 */
	static void checkBounds(byte[] buf, int off, int len)
	{
		checkArgument(off >= 0 && len >= 0);
		if (off + len > buf.length) {
			throw new IndexOutOfBoundsException();
		}
	}

	/**
	 * Checks the truth of the given condition checking the state of the
	 * calling instance.
	 *
	 * @param condition the boolean condition to test.
	 *
	 * @throws IllegalStateException if the condition is {@code false}.
	 */
	static void checkState(boolean condition)
	{
		if (!condition) {
			throw new IllegalStateException();
		}
	}

	private Preconditions()
	{
		// See "Effective Java" (Item 4)
		throw new AssertionError("Not meant to be instantiated");
	}
}