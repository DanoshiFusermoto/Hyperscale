package org.radix.hyperscale.utils;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;

/**
 * Utility class for manipulating primitive bytes.
 */
public class Bytes 
{
	private static final byte[] hexBytes = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };

	/**
	 * An empty array of bytes.
	 */
	public static final byte[] EMPTY_BYTES = new byte[0];

	private Bytes() 
	{
		throw new IllegalStateException("Can't construct");
	}

	/**
	 * Compare two byte array segments for equality.
	 *
	 * @param a1      The first array to compare
	 * @param offset1 The offset within {@code a1} to begin the comparison
	 * @param length1 The quantity of {@code a1} to compare
	 * @param a2      The second array to compare
	 * @param offset2 The offset within {@code a2} to begin the comparison
	 * @param length2 The quantity of {@code a2} to compare
	 * @return {@code true} iff {@code length1 == length2} and {@code a1[offset1 + i] == a2[offset2 + i]}
	 * 		for {@code i} &#x2208; {@code [0, length1)}.
	 */
	public static boolean arrayEquals(byte[] a1, int offset1, int length1, byte[] a2, int offset2, int length2) 
	{
        if (length1 != length2)
        	return false;

        for (int i = 0; i < length1; ++i) 
        {
            if (a1[offset1 + i] != a2[offset2 + i])
                return false;
        }

        return true;
	}

	/**
	 * Calculate hash code of a byte array segment.
	 *
	 * @param a      The array for which to calculate the hash code.
	 * @param offset The offset within the array to start the calculation.
	 * @param length The number of bytes for which to calculate the hash code.
	 * @return The hash code.
	 */
	public static int hashCode(byte[] a, int offset, int length) 
	{
		int i = length;
		int hc = i + 1;
		while (--i >= 0) 
		{
			hc *= 257;
			hc ^= a[offset + i];
		}
		return hc;
	}

	/**
	 * Convert a byte array into a {@link String} using the
	 * {@link RadixConstants#STANDARD_CHARSET} character set.
	 *
	 * @param bytes The bytes to convert.
	 * @return The string
	 */
	public static String toString(byte[] bytes) 
	{
		return new String(bytes, StandardCharsets.UTF_8);
	}

	/**
	 * Convert a byte into a two-digit hex string.
	 * <p>
	 * Note that digits a-f are output as lower case.
	 *
	 * @param b The byte to convert
	 * @return The converted string
	 */
	public static String toHexString(byte b) 
	{
		byte[] value = { toHexByte(b >> 4), toHexByte(b) };
		return new String(value);
	}

	/**
	 * Convert an array into a string of hex digits.
	 * <p>
	 * The output string will have length {@code 2*bytes.length}.
	 * Hex digits a-f are encoded as lower case.
	 *
	 * @param bytes The bytes to convert
	 * @return The converted string
	 */
	public static String toHexString(byte[] bytes) 
	{
		return toHexString(bytes, 0, bytes.length);
	}

	/**
	 * Convert a portion of an array into a string of hex digits.
	 * <p>
	 * The output string will have length {@code 2*length}.
	 * Hex digits a-f are encoded as lower case.
	 *
	 * @param bytes The bytes to convert
	 * @param offset The offset at which to start converting
	 * @param length The number of bytes to convert
	 * @return The converted string
	 */
	public static String toHexString(byte[] bytes, int offset, int length) 
	{
		byte[] hex = new byte[length * 2];
	    for (int i = 0; i < length; ++i) {
	        byte b = bytes[offset + i];
	        hex[i * 2] = hexBytes[(b >> 4) & 0xF];
	        hex[i * 2 + 1] = hexBytes[b & 0xF];
	    }
	    return new String(hex, StandardCharsets.US_ASCII);
	}

	/**
	 * Convert a string of hexadecimal digits to an array of bytes.
	 * <p>
	 * If the string length is odd, a leading '0' is assumed.
	 *
	 * @param s The string to convert to a byte array.
	 * @return The byte array corresponding to the converted string
	 * @throws IllegalArgumentException if any character in s is not a hex digit
	 */
	public static byte[] fromHexString(final String s) 
	{
		return fromHexString(s, 0);
	}

	/**
	 * Convert a string of hexadecimal digits starting at the offset, to an array of bytes.
	 * <p>
	 * If the string length is odd, a leading '0' is assumed.
	 *
	 * @param s The string to convert to a byte array.
	 * @param offset The offset into the string to start decoding.
	 * @return The byte array corresponding to the converted string
	 * @throws IllegalArgumentException if any character in s is not a hex digit
	 */
	public static byte[] fromHexString(final String s, final int offset) 
	{
		int inputLength = s.length() - offset;
		int byteCount = (inputLength + 1) / 2;
		byte[] bytes = new byte[byteCount];
		int i = offset;
		int b = 0;
		// If an odd number of chars, assume leading zero
		if ((inputLength & 1) != 0)
			bytes[b++] = fromHexNybble(s.charAt(i++));

		while (i < s.length()) 
		{
			byte msn = fromHexNybble(s.charAt(i++));
			byte lsn = fromHexNybble(s.charAt(i++));
			bytes[b++] = (byte) (((msn & 0xFF) << 4) | (lsn & 0xFF));
		}
		
		return bytes;
	}

	/**
	 * Convert an array of bytes into a Base-64 encoded using RFC 4648 rules.
	 *
	 * @param bytes The bytes to encode
	 * @return The base-64 encoded string
	 */
	public static String toBase64String(byte[] bytes) 
	{
		byte[] result = Base64.getEncoder().encode(bytes);
		return Strings.fromAsciiBytes(result, 0, result.length);
	}

	/**
	 * Convert a base-64 encoded string into an array of bytes using RFC 4648 rules.
	 *
	 * @param s The string to convert
	 * @return The decoded bytes
	 */
	public static byte[] fromBase64String(String s) 
	{
		return Base64.getDecoder().decode(s);
	}

	private static byte toHexByte(int value) 
	{
		return hexBytes[value & 0xF];
	}

	private static byte fromHexNybble(char value) 
	{
		char c = value;
		if (c >= '0' && c <= '9')
			return (byte) (c - '0');
		else
		{
			c = Character.toLowerCase(value);
			if (c >= 'a' && c <= 'f')
				return (byte) (10 + c - 'a');
		}
		
		throw new IllegalArgumentException("Unknown hex digit: " + value);
	}

	/**
	 * Trims any leading zero bytes from {@code bytes} until either no
	 * leading zero exists, or only a single zero byte exists.
	 *
	 * @param bytes the byte a
	 * @return @code bytes} with leading zeros removed, if any
	 */
	public static byte[] trimLeadingZeros(byte[] bytes) 
	{
		if (bytes == null || bytes.length <= 1 || bytes[0] != 0)
			return bytes;

		int trimLeadingZeros = 1;
		int maxTrim = bytes.length - 1;
		while (trimLeadingZeros < maxTrim && bytes[trimLeadingZeros] == 0)
			trimLeadingZeros += 1;

		return Arrays.copyOfRange(bytes, trimLeadingZeros, bytes.length);
	}
	
	public static boolean allZero(byte[] bytes)
	{
		return allZero(bytes, 0, bytes.length);
	}

	public static boolean allZero(byte[] bytes, int offset, int length)
	{
		for (int i = offset ; i < offset+length ; i++)
			if (bytes[i] != 0)
				return false;
		
		return true;
	}
	
	public static byte[] concatenate(byte[] first, byte[] second)
	{
		final byte[] concatenated = new byte[first.length+second.length];
		System.arraycopy(first, 0, concatenated, 0, first.length);
		System.arraycopy(second, 0, concatenated, first.length, second.length);
		return concatenated;
	}
}
