/*
 * Copyright 2011 Google Inc.
 * Copyright 2018 Andreas Schildbach
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.radix.hyperscale.utils;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Base58 is a way to encode Bitcoin addresses (or arbitrary data) as alphanumeric strings.
 * <p>
 * Note that this is not the same base58 as used by Flickr, which you may find referenced around the Internet.
 * <p>
 * You may want to consider working with {@link PrefixedChecksummedBytes} instead, which
 * adds support for testing the prefix and suffix bytes commonly found in addresses.
 * <p>
 * Satoshi explains: why base-58 instead of standard base-64 encoding?
 * <ul>
 * <li>Don't want 0OIl characters that look the same in some fonts and
 *     could be used to create visually identical looking account numbers.</li>
 * <li>A string with non-alphanumeric characters is not as easily accepted as an account number.</li>
 * <li>E-mail usually won't line-break if there's no punctuation to break at.</li>
 * <li>Doubleclicking selects the whole number as one word if it's all alphanumeric.</li>
 * </ul>
 * <p>
 * However, note that the encoding/decoding runs in O(n&sup2;) time, so it is not useful for large data.
 * <p>
 * The basic idea of the encoding is to treat the data bytes as a large number represented using
 * base-256 digits, convert the number to be represented using base-58 digits, preserve the exact
 * number of leading zeros (which are otherwise lost during the mathematical operations on the
 * numbers), and finally represent the resulting base-58 digits as alphanumeric ASCII characters.
 * <p>
 * Note: This code is subject to the license noted in the header comment and was taken from the bitcoinj
 * library available on
 * <a href="https://github.com/bitcoinj/bitcoinj/blob/master/core/src/main/java/org/bitcoinj/core/Base58.java">
 * Github</a>.
 */
public final class Base58 
{
	private static final int BUFFER_SIZE = 1024;
	private static final ThreadLocal<byte[]> encodedBuffer = ThreadLocal.withInitial(() -> new byte[BUFFER_SIZE]);
	private static final ThreadLocal<byte[]> decodedBuffer = ThreadLocal.withInitial(() -> new byte[BUFFER_SIZE]);
	
    private static final byte[] ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz".getBytes(StandardCharsets.US_ASCII);
    private static final byte ENCODED_ZERO = ALPHABET[0];
    private static final int[] INDEXES = new int[128];
    static 
    {
        Arrays.fill(INDEXES, -1);
        for (int i = 0; i < ALPHABET.length; i++)
            INDEXES[ALPHABET[i]] = i;
    }

    private Base58() 
    {
    	throw new IllegalArgumentException("Can't construct");
    }

    /**
     * Encodes the given bytes as a base58 string (no checksum is appended).
     *
     * @param input the bytes to encode
     * @return the base58-encoded string
     */
    public static String toBase58(byte[] input) 
    {
        if (input.length == 0)
            return "";

        // Count leading zeros.
        int zeros = 0;
        while (zeros < input.length && input[zeros] == 0)
            ++zeros;
        
        // Convert base-256 digits to base-58 digits (plus conversion to ASCII characters)
        final int decodedLength = input.length;
        final byte[] decoded = decodedLength <= BUFFER_SIZE ? Base58.decodedBuffer.get() : new byte[decodedLength];
        System.arraycopy(input, 0, decoded, 0, decodedLength);
        
        final int encodedLength = decodedLength * 2;
        final byte[] encoded = encodedLength <= BUFFER_SIZE ? Base58.encodedBuffer.get() : new byte[encodedLength];
        int outputStart = encodedLength;
        for (int inputStart = zeros; inputStart < decodedLength; ) 
        {
            encoded[--outputStart] = ALPHABET[divmod(decoded, decodedLength, inputStart, 256, 58)];
            if (decoded[inputStart] == 0)
                ++inputStart; // optimization - skip leading zeros
        }
        
        // Preserve exactly as many leading encoded zeros in output as there were leading zeros in input.
        while (outputStart < encodedLength && encoded[outputStart] == ENCODED_ZERO)
            ++outputStart;
        
        while (--zeros >= 0)
            encoded[--outputStart] = ENCODED_ZERO;
        
        // Return encoded string (including encoded leading zeros).
        return new String(encoded, outputStart, encodedLength - outputStart, StandardCharsets.US_ASCII);
    }

    /**
     * Decodes the given base58 string into the original data bytes.
     *
     * @param input the base58-encoded string to decode
     * @return the decoded data bytes
     * @throws IllegalArgumentException if the given string is not a valid base58 string
     */
    public static byte[] fromBase58(String input) 
    {
    	return fromBase58(input, 0);
    }
    
    /**
     * Decodes the given base58 string into the original data bytes.
     *
     * @param input the base58-encoded string to decode
     * @param offset into the base58-encoded input string to start decoding
     * @return the decoded data bytes
     * @throws IllegalArgumentException if the given string is not a valid base58 string
     */
    public static byte[] fromBase58(String input, int offset) 
    {
        if (input.length() == 0)
            return new byte[0];

        // Convert the base58-encoded ASCII chars to a base58 byte sequence (base58 digits).
        final int inputlength = input.length();
        final int encodedLength = inputlength-offset;
        final byte[] encoded = encodedLength <= BUFFER_SIZE ? Base58.encodedBuffer.get() : new byte[encodedLength];
        for (int i = offset; i < inputlength; ++i) 
        {
            char c = input.charAt(i);
            int digit = c < 128 ? INDEXES[c] : -1;
            if (digit < 0)
                throw new IllegalArgumentException("Invalid base58 character: " + c);

            encoded[i-offset] = (byte) digit;
        }
        
        // Count leading zeros.
        int zeros = 0;
        while (zeros < encodedLength && encoded[zeros] == 0)
            ++zeros;
        
        // Convert base-58 digits to base-256 digits.
        final int decodedLength = encodedLength;
        final byte[] decoded = decodedLength <= BUFFER_SIZE ? Base58.decodedBuffer.get() : new byte[decodedLength];
        int outputStart = decodedLength;
        for (int inputStart = zeros; inputStart < encodedLength; ) 
        {
            decoded[--outputStart] = divmod(encoded, encodedLength, inputStart, 58, 256);
            if (encoded[inputStart] == 0)
                ++inputStart; // optimization - skip leading zeros
        }

        // Ignore extra leading zeroes that were added during the calculation.
        while (outputStart < decodedLength && decoded[outputStart] == 0)
            ++outputStart;

        // Return decoded data (including original number of leading zeros).
        return Arrays.copyOfRange(decoded, outputStart - zeros, decodedLength);
    }

    /**
     * Divides a number, represented as an array of bytes each containing a single digit
     * in the specified base, by the given divisor. The given number is modified in-place
     * to contain the quotient, and the return value is the remainder.
     *
     * @param number the number to divide
     * @param length the number length in bytes
     * @param firstDigit the index within the array of the first non-zero digit
     *        (this is used for optimization by skipping the leading zeros)
     * @param base the base in which the number's digits are represented (up to 256)
     * @param divisor the number to divide by (up to 256)
     * @return the remainder of the division operation
     */
    private static byte divmod(byte[] number, int length, int firstDigit, int base, int divisor) 
    {
        // this is just long division which accounts for the base of the input digits
        int remainder = 0;
        for (int i = firstDigit; i < length; i++) 
        {
            int digit = number[i] & 0xFF;
            int temp = remainder * base + digit;
            number[i] = (byte) (temp / divisor);
            remainder = temp % divisor;
        }
        
        return (byte) remainder;
    }
}
