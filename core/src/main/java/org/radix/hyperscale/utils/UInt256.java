package org.radix.hyperscale.utils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.util.Arrays;
import java.util.Objects;

public final class UInt256 implements Comparable<UInt256> 
{
    public static final int SIZE = 256;
    public static final int BYTES = SIZE / 8;

    public static final UInt256 MIN_VALUE = new UInt256(0, 0, 0, 0);
    public static final UInt256 MAX_VALUE = new UInt256(-1L, -1L, -1L, -1L);
    public static final UInt256 HIGH_BIT = new UInt256(0x8000_0000_0000_0000L, 0, 0, 0);
    
    public static final UInt256 ZERO = MIN_VALUE;
    public static final UInt256 ONE = new UInt256(0, 0, 0, 1);
    public static final UInt256 TWO = new UInt256(0, 0, 0, 2);
    public static final UInt256 THREE = new UInt256(0, 0, 0, 3);
    public static final UInt256 FOUR = new UInt256(0, 0, 0, 4);
    public static final UInt256 FIVE = new UInt256(0, 0, 0, 5);
    public static final UInt256 SIX = new UInt256(0, 0, 0, 6);
    public static final UInt256 SEVEN = new UInt256(0, 0, 0, 7);
    public static final UInt256 EIGHT = new UInt256(0, 0, 0, 8);
    public static final UInt256 NINE = new UInt256(0, 0, 0, 9);
    public static final UInt256 TEN = new UInt256(0, 0, 0, 10);
    public static final UInt256 HUNDRED = new UInt256(0, 0, 0, 100);
    public static final UInt256 THOUSAND = new UInt256(0, 0, 0, 1000);
    public static final UInt256 MILLION = new UInt256(0, 0, 0, 1000000);

    private static final UInt256[] numbers = { ZERO, ONE, TWO, THREE, FOUR, FIVE, SIX, SEVEN, EIGHT, NINE, TEN };

    public static UInt256 from(short value) 
    {
        return from(value & 0xFFFFL);
    }

    public static UInt256 from(int value) 
    {
        return from(value & 0xFFFFFFFFL);
    }

    public static UInt256 from(long value) 
    {
    	if (value >= 0) 
    	{
            if (value <= 10)
                return numbers[(int) value];

            if (value == 100) return HUNDRED;
            if (value == 1000) return THOUSAND;
            if (value == 1000000) return MILLION;
        }
    	
        return new UInt256(0, 0, 0, value);
    }

    public static UInt256 from(UInt128 value) 
    {
        return new UInt256(0, 0, value.getHigh(), value.getLow());
    }

    public static UInt256 from(UInt128 highs, UInt128 lows) 
    {
        return new UInt256(highs.getHigh(), highs.getLow(), lows.getHigh(), lows.getLow());
    }

    public static UInt256 from(byte[] bytes) 
    {
        Objects.requireNonNull(bytes);
        if (bytes.length == 0)
            throw new IllegalArgumentException("bytes is 0 bytes long");
        
        byte[] newBytes = extend(bytes);
        return from(newBytes, 0);
    }

    public static UInt256 from(byte[] bytes, int offset) {
        if (bytes.length - offset < BYTES) {
            throw new IllegalArgumentException("Not enough bytes to construct a UInt256");
        }

        long v3 = ((long) bytes[offset + 31] & 0xFF) | ((long) (bytes[offset + 30] & 0xFF) << 8) |
                  ((long) (bytes[offset + 29] & 0xFF) << 16) | ((long) (bytes[offset + 28] & 0xFF) << 24) |
                  ((long) (bytes[offset + 27] & 0xFF) << 32) | ((long) (bytes[offset + 26] & 0xFF) << 40) |
                  ((long) (bytes[offset + 25] & 0xFF) << 48) | ((long) (bytes[offset + 24] & 0xFF) << 56);

        long v2 = ((long) bytes[offset + 23] & 0xFF) | ((long) (bytes[offset + 22] & 0xFF) << 8) |
                  ((long) (bytes[offset + 21] & 0xFF) << 16) | ((long) (bytes[offset + 20] & 0xFF) << 24) |
                  ((long) (bytes[offset + 19] & 0xFF) << 32) | ((long) (bytes[offset + 18] & 0xFF) << 40) |
                  ((long) (bytes[offset + 17] & 0xFF) << 48) | ((long) (bytes[offset + 16] & 0xFF) << 56);

        long v1 = ((long) bytes[offset + 15] & 0xFF) | ((long) (bytes[offset + 14] & 0xFF) << 8) |
                  ((long) (bytes[offset + 13] & 0xFF) << 16) | ((long) (bytes[offset + 12] & 0xFF) << 24) |
                  ((long) (bytes[offset + 11] & 0xFF) << 32) | ((long) (bytes[offset + 10] & 0xFF) << 40) |
                  ((long) (bytes[offset + 9] & 0xFF) << 48) | ((long) (bytes[offset + 8] & 0xFF) << 56);

        long v0 = ((long) bytes[offset + 7] & 0xFF) | ((long) (bytes[offset + 6] & 0xFF) << 8) |
                  ((long) (bytes[offset + 5] & 0xFF) << 16) | ((long) (bytes[offset + 4] & 0xFF) << 24) |
                  ((long) (bytes[offset + 3] & 0xFF) << 32) | ((long) (bytes[offset + 2] & 0xFF) << 40) |
                  ((long) (bytes[offset + 1] & 0xFF) << 48) | ((long) (bytes[offset + 0] & 0xFF) << 56);

        return new UInt256(v0, v1, v2, v3);
    }

    public static UInt256 from(String s) {
        return from(s, 0);
    }

    public static UInt256 from(String s, int offset) 
    {
    	Objects.requireNonNull(s);
        int len = s.length();
        if (len <= offset)
            throw new NumberFormatException(s);

        int i = offset;
        if (s.charAt(i) == '+') 
        {
            i++;
            if (i >= len)
                throw new NumberFormatException(s);
        }

        // Check for single digit numbers (0-9)
        if (len - i == 1) 
        {
            int digit = Character.digit(s.charAt(i), 10);
            if (digit >= 0 && digit <= 9)
                return numbers[digit];
            
            throw new NumberFormatException(s);
        }

        // Check for common constants
        if (s.charAt(i) == '1')
        {
        	// Count consecutive zeros
        	int zeros = 0;
        	int j = i + 1;
        	for (; j < s.length() && s.charAt(j) == '0'; j++)
        		zeros++;

        	if (j == s.length())
        	{
	        	switch (zeros) 
	        	{
	        		case 1: return TEN;
	        		case 2: return HUNDRED;
	        		case 3: return THOUSAND;
	        		case 6: return MILLION;
	        	}
        	}
        }
        
        // Parse larger numbers
        UInt256 result = ZERO;
        while (i < len) 
        {
            int digit = Character.digit(s.charAt(i++), 10);
            if (digit < 0)
                throw new NumberFormatException(s);
            
            result = result.multiply(TEN).add(numbers[digit]);
        }
        
        return result;
    }

    private final boolean internal;
    private long v0, v1, v2, v3; // v0 is most significant, v3 is least significant
    private byte[] cachedBytes = null;

    private UInt256(long v0, long v1, long v2, long v3) 
    {
    	this(v0, v1, v2, v3, false);
    }

    private UInt256(long v0, long v1, long v2, long v3, boolean internal) 
    {
    	this.internal = internal;
        this.v0 = v0;
        this.v1 = v1;
        this.v2 = v2;
        this.v3 = v3;
    }

    private static byte[] extend(byte[] bytes) 
    {
        if (bytes.length >= BYTES)
            return bytes;

        byte[] newBytes = new byte[BYTES];
        int newPos = BYTES - bytes.length;
        Arrays.fill(newBytes, 0, newPos, (byte) 0);
        System.arraycopy(bytes, 0, newBytes, newPos, bytes.length);
        return newBytes;
    }

    public synchronized byte[] toByteArray() 
    {
    	if (this.internal == true)
    		throw new UnsupportedOperationException("Internal UInt256");

    	if (this.cachedBytes != null)
    		return this.cachedBytes;

    	this.cachedBytes = new byte[BYTES];
    	return toByteArray(this.cachedBytes, 0);
    }

    public byte[] toByteArray(byte[] bytes, int offset) 
    {
    	if (this.internal == true)
    		throw new UnsupportedOperationException("Internal UInt256");

    	for (int i = 0; i < 8; i++) 
        {
            bytes[offset + i] = (byte) (this.v0 >>> (56 - i * 8));
            bytes[offset + i + 8] = (byte) (this.v1 >>> (56 - i * 8));
            bytes[offset + i + 16] = (byte) (this.v2 >>> (56 - i * 8));
            bytes[offset + i + 24] = (byte) (this.v3 >>> (56 - i * 8));
        }
        
        return bytes;
    }

    public BigInteger toBigInteger() 
    {
    	byte[] bytes = toByteArray();
        return new BigInteger(1, bytes);
    }

    public BigDecimal toBigDecimal(int scale) 
    {
    	return new BigDecimal(toBigInteger(), scale);
    }

    public BigDecimal toBigDecimal(int scale, MathContext mc) 
    {
    	return new BigDecimal(toBigInteger(), scale, mc);
    }

    public UInt256 add(UInt256 other) 
    {
        long sum3 = this.v3 + other.v3;
        long carry = (Long.compareUnsigned(sum3, this.v3) < 0) ? 1 : 0;

        long sum2 = this.v2 + other.v2 + carry;
        carry = (Long.compareUnsigned(sum2, this.v2) < 0 || (sum2 == this.v2 && carry == 1)) ? 1 : 0;

        long sum1 = this.v1 + other.v1 + carry;
        carry = (Long.compareUnsigned(sum1, this.v1) < 0 || (sum1 == this.v1 && carry == 1)) ? 1 : 0;

        long sum0 = this.v0 + other.v0 + carry;

        return new UInt256(sum0, sum1, sum2, sum3);
    }

    public UInt256 add(UInt128 other) 
    {
        long sum3 = this.v3 + other.getLow();
        long carry = (Long.compareUnsigned(sum3, this.v3) < 0) ? 1 : 0;

        long sum2 = this.v2 + other.getHigh() + carry;
        carry = (Long.compareUnsigned(sum2, this.v2) < 0 || (sum2 == this.v2 && carry == 1)) ? 1 : 0;

        long sum1 = this.v1 + carry;
        carry = (Long.compareUnsigned(sum1, this.v1) < 0) ? 1 : 0;

        long sum0 = this.v0 + carry;

        return new UInt256(sum0, sum1, sum2, sum3);
    }
    
    public UInt256 subtract(UInt256 other) 
    {
        long diff3 = this.v3 - other.v3;
        long borrow = (Long.compareUnsigned(this.v3, other.v3) < 0) ? 1 : 0;

        long diff2 = this.v2 - other.v2 - borrow;
        borrow = (Long.compareUnsigned(this.v2, other.v2) < 0 || (this.v2 == other.v2 && borrow == 1)) ? 1 : 0;

        long diff1 = this.v1 - other.v1 - borrow;
        borrow = (Long.compareUnsigned(this.v1, other.v1) < 0 || (this.v1 == other.v1 && borrow == 1)) ? 1 : 0;

        long diff0 = this.v0 - other.v0 - borrow;

        return new UInt256(diff0, diff1, diff2, diff3);
    }
    
    private void subtractInternal(UInt256 other) 
    {
    	if (this.internal == false)
    		throw new UnsupportedOperationException("Not an internal UInt256");

    	long diff3 = this.v3 - other.v3;
        long borrow = (Long.compareUnsigned(this.v3, other.v3) < 0) ? 1 : 0;

        long diff2 = this.v2 - other.v2 - borrow;
        borrow = (Long.compareUnsigned(this.v2, other.v2) < 0 || (this.v2 == other.v2 && borrow == 1)) ? 1 : 0;

        long diff1 = this.v1 - other.v1 - borrow;
        borrow = (Long.compareUnsigned(this.v1, other.v1) < 0 || (this.v1 == other.v1 && borrow == 1)) ? 1 : 0;

        long diff0 = this.v0 - other.v0 - borrow;
        
        this.v0 = diff0; this.v1 = diff1;
        this.v2 = diff2; this.v3 = diff3;
    }

    public UInt256 subtract(UInt128 other) 
    {
        long diff3 = this.v3 - other.getLow();
        long borrow = (Long.compareUnsigned(this.v3, other.getLow()) < 0) ? 1 : 0;

        long diff2 = this.v2 - other.getHigh() - borrow;
        borrow = (Long.compareUnsigned(this.v2, other.getHigh()) < 0 || (this.v2 == other.getHigh() && borrow == 1)) ? 1 : 0;

        long diff1 = this.v1 - borrow;
        borrow = (this.v1 == 0 && borrow == 1) ? 1 : 0;

        long diff0 = this.v0 - borrow;

        return new UInt256(diff0, diff1, diff2, diff3);
    }
    
    public UInt256 multiply(UInt256 other) 
    {
        long a0 = this.v3 & 0xFFFFFFFFL;
        long a1 = this.v3 >>> 32;
        long a2 = this.v2 & 0xFFFFFFFFL;
        long a3 = this.v2 >>> 32;
        long a4 = this.v1 & 0xFFFFFFFFL;
        long a5 = this.v1 >>> 32;
        long a6 = this.v0 & 0xFFFFFFFFL;
        long a7 = this.v0 >>> 32;

        long b0 = other.v3 & 0xFFFFFFFFL;
        long b1 = other.v3 >>> 32;
        long b2 = other.v2 & 0xFFFFFFFFL;
        long b3 = other.v2 >>> 32;
        long b4 = other.v1 & 0xFFFFFFFFL;
        long b5 = other.v1 >>> 32;
        long b6 = other.v0 & 0xFFFFFFFFL;
        long b7 = other.v0 >>> 32;

        long r0 = a0 * b0;
        long r1 = a1 * b0 + a0 * b1;
        long r2 = a2 * b0 + a1 * b1 + a0 * b2;
        long r3 = a3 * b0 + a2 * b1 + a1 * b2 + a0 * b3;
        long r4 = a4 * b0 + a3 * b1 + a2 * b2 + a1 * b3 + a0 * b4;
        long r5 = a5 * b0 + a4 * b1 + a3 * b2 + a2 * b3 + a1 * b4 + a0 * b5;
        long r6 = a6 * b0 + a5 * b1 + a4 * b2 + a3 * b3 + a2 * b4 + a1 * b5 + a0 * b6;
        long r7 = a7 * b0 + a6 * b1 + a5 * b2 + a4 * b3 + a3 * b4 + a2 * b5 + a1 * b6 + a0 * b7;

        long carry = 0;
        r0 += carry; carry = r0 >>> 32; r0 &= 0xFFFFFFFFL;
        r1 += carry; carry = r1 >>> 32; r1 &= 0xFFFFFFFFL;
        r2 += carry; carry = r2 >>> 32; r2 &= 0xFFFFFFFFL;
        r3 += carry; carry = r3 >>> 32; r3 &= 0xFFFFFFFFL;
        r4 += carry; carry = r4 >>> 32; r4 &= 0xFFFFFFFFL;
        r5 += carry; carry = r5 >>> 32; r5 &= 0xFFFFFFFFL;
        r6 += carry; carry = r6 >>> 32; r6 &= 0xFFFFFFFFL;
        r7 += carry;

        return new UInt256(
            (r7 << 32) | r6,
            (r5 << 32) | r4,
            (r3 << 32) | r2,
            (r1 << 32) | r0
        );
    }
    
    public UInt256 multiply(UInt128 other) 
    {
        long a0 = this.v3 & 0xFFFFFFFFL;
        long a1 = this.v3 >>> 32;
        long a2 = this.v2 & 0xFFFFFFFFL;
        long a3 = this.v2 >>> 32;
        long a4 = this.v1 & 0xFFFFFFFFL;
        long a5 = this.v1 >>> 32;
        long a6 = this.v0 & 0xFFFFFFFFL;
        long a7 = this.v0 >>> 32;

        long b0 = other.getLow() & 0xFFFFFFFFL;
        long b1 = other.getLow() >>> 32;
        long b2 = other.getHigh() & 0xFFFFFFFFL;
        long b3 = other.getHigh() >>> 32;

        long r0 = a0 * b0;
        long r1 = a1 * b0 + a0 * b1;
        long r2 = a2 * b0 + a1 * b1 + a0 * b2;
        long r3 = a3 * b0 + a2 * b1 + a1 * b2 + a0 * b3;
        long r4 = a4 * b0 + a3 * b1 + a2 * b2 + a1 * b3;
        long r5 = a5 * b0 + a4 * b1 + a3 * b2 + a2 * b3;
        long r6 = a6 * b0 + a5 * b1 + a4 * b2 + a3 * b3;
        long r7 = a7 * b0 + a6 * b1 + a5 * b2 + a4 * b3;

        long carry = 0;
        r0 += carry; carry = r0 >>> 32; r0 &= 0xFFFFFFFFL;
        r1 += carry; carry = r1 >>> 32; r1 &= 0xFFFFFFFFL;
        r2 += carry; carry = r2 >>> 32; r2 &= 0xFFFFFFFFL;
        r3 += carry; carry = r3 >>> 32; r3 &= 0xFFFFFFFFL;
        r4 += carry; carry = r4 >>> 32; r4 &= 0xFFFFFFFFL;
        r5 += carry; carry = r5 >>> 32; r5 &= 0xFFFFFFFFL;
        r6 += carry; carry = r6 >>> 32; r6 &= 0xFFFFFFFFL;
        r7 += carry;

        return new UInt256(
            (r7 << 32) | r6,
            (r5 << 32) | r4,
            (r3 << 32) | r2,
            (r1 << 32) | r0
        );
    }

    public UInt256 divide(UInt256 divisor) 
    {
        if (divisor.isZero())
            throw new ArithmeticException("Division by zero");
        
        if (this.compareTo(divisor) < 0)
            return ZERO;

        if (this.equals(divisor))
            return ONE;

        UInt256 quotient = new UInt256(0,0,0,0, true);
        UInt256 remainder = new UInt256(0,0,0,0, true);
        int leading = numberOfLeadingZeros();
        for (int i = SIZE - (leading + 1); i >= 0; i--) 
        {
        	remainder.shiftLeftInternal(1);
            if (getBit(i))
            	remainder.orInternal(ONE);
            
            if (divisor.compareTo(remainder) <= 0)
            {
            	remainder.subtractInternal(divisor);
                quotient.setBitInternal(i);
            }
        }
        
        return new UInt256(quotient.v0, quotient.v1, quotient.v2, quotient.v3);
    }
    
    private void divideInternal(UInt256 divisor) 
    {
    	if (this.internal == false)
    		throw new UnsupportedOperationException("Not an internal UInt256");

    	if (divisor.isZero())
            throw new ArithmeticException("Division by zero");
        
        if (compareTo(divisor) < 0)
        {
        	this.v0 = 0; this.v1 = 0; this.v2 = 0; this.v3 = 0;
        	return;
        }

        if (equals(divisor))
        {
        	this.v0 = 0; this.v1 = 0; this.v2 = 0; this.v3 = 1;
        	return;
        }

        UInt256 quotient = new UInt256(0,0,0,0, true);
        UInt256 remainder = new UInt256(0,0,0,0, true);
        int leading = numberOfLeadingZeros();
        for (int i = SIZE - (leading + 1); i >= 0; i--) 
        {
        	remainder.shiftLeftInternal(1);
            if (getBit(i))
            	remainder.orInternal(ONE);
            
            if (divisor.compareTo(remainder) <= 0)
            {
            	remainder.subtractInternal(divisor);
                quotient.setBitInternal(i);
            }
        }
        
        this.v0 = quotient.v0; this.v1 = quotient.v1;
        this.v2 = quotient.v2; this.v3 = quotient.v3;
    }

    public UInt256 divide(UInt128 divisor) 
    {
        if (divisor.isZero())
            throw new ArithmeticException("Division by zero");

        if (this.compareTo(new UInt256(0, 0, divisor.getHigh(), divisor.getLow())) < 0)
            return UInt256.ZERO;

        UInt256 quotient = UInt256.ZERO;
        UInt256 remainder = this;
        UInt256 divisor256 = new UInt256(0, 0, divisor.getHigh(), divisor.getLow());

        int shift = divisor.numberOfLeadingZeros() - this.numberOfLeadingZeros();
        divisor256 = divisor256.shiftLeft(shift);

        while (shift >= 0) 
        {
            if (remainder.compareTo(divisor256) >= 0) 
            {
                remainder = remainder.subtract(divisor256);
                quotient = quotient.setBit(shift);
            }
            
            divisor256 = divisor256.shiftRight(1);
            shift--;
        }

        return quotient;
    }

    public UInt256 remainder(UInt256 divisor) 
    {
        if (divisor.isZero())
            throw new ArithmeticException("Division by zero");
        
        UInt256 remainder = new UInt256(0,0,0,0, true);
        int leading = numberOfLeadingZeros();
        for (int i = SIZE - (leading + 1); i >= 0; i--) 
        {
        	remainder.shiftLeftInternal(1);
            if (getBit(i))
            	remainder.orInternal(ONE);
            
            if (divisor.compareTo(remainder) <= 0)
            	remainder.subtractInternal(divisor);
        }
        
        return new UInt256(remainder.v0, remainder.v1, remainder.v2, remainder.v3);
    }

    public UInt256 pow(int exponent) 
    {
    	if (exponent < 0)
            throw new IllegalArgumentException("Negative exponent");
        
        if (exponent == 0)
            return UInt256.ONE;
        
        if (exponent == 1 || this.equals(UInt256.ONE) || this.isZero())
            return this;

        UInt256 base = this;
        UInt256 result = UInt256.ONE;
        
        while (exponent > 0) 
        {
            if ((exponent & 1) == 1)
                result = result.multiply(base);
            
            base = base.multiply(base);
            exponent >>= 1;
        }

        return result;
    }

    public UInt256 isqrt() 
    {
        UInt256 bit = ONE.shiftLeft(SIZE - 2);
        UInt256 num = this;
        while (bit.compareTo(num) > 0) 
        {
            bit = bit.shiftRight(2);
        }
        
        UInt256 res = ZERO;
        while (bit.isZero() == false) 
        {
            UInt256 rab = res.add(bit);
            if (num.compareTo(rab) >= 0) 
            {
                num = num.subtract(rab);
                res = res.shiftRight(1).add(bit);
            } 
            else 
                res = res.shiftRight(1);

            bit = bit.shiftRight(2);
        }
        return res;
    }

    public UInt256 shiftLeft() 
    {
        return shiftLeft(1);
    }

    public UInt256 shiftLeft(int n) 
    {
        if (n == 0) {
            return this;
        }
        if (n >= 256) {
            return UInt256.ZERO;
        }
        
        if (n < 0)
        	return shiftRight(Math.abs(n));
        
        // Handle shifts in multiples of 64 first
        int longShifts = n / 64;
        long r0 = 0, r1 = 0, r2 = 0, r3 = 0;
        switch (longShifts) 
        {
            case 0:
                r0 = this.v0; r1 = this.v1; r2 = this.v2; r3 = this.v3;
                break;
            case 1:
                r0 = this.v1; r1 = this.v2; r2 = this.v3;
                break;
            case 2:
                r0 = this.v2; r1 = this.v3;
                break;
            case 3:
                r0 = this.v3;
                break;
        }
        
        // Then handle the remainder shift
        int remainderShift = n % 64;
        if (remainderShift != 0) {
            r0 = (r0 << remainderShift) | (r1 >>> (64 - remainderShift));
            r1 = (r1 << remainderShift) | (r2 >>> (64 - remainderShift));
            r2 = (r2 << remainderShift) | (r3 >>> (64 - remainderShift));
            r3 = r3 << remainderShift;
        }
        
        return new UInt256(r0, r1, r2, r3);
    }
    
    private void shiftLeftInternal(int n) 
    {
    	if (this.internal == false)
    		throw new UnsupportedOperationException("Not an internal UInt256");
    	
        // Handle shifts in multiples of 64 first
        int longShifts = n / 64;
        long r0 = 0, r1 = 0, r2 = 0, r3 = 0;
        switch (longShifts) 
        {
            case 0:
                r0 = this.v0; r1 = this.v1; r2 = this.v2; r3 = this.v3;
                break;
            case 1:
                r0 = this.v1; r1 = this.v2; r2 = this.v3;
                break;
            case 2:
                r0 = this.v2; r1 = this.v3;
                break;
            case 3:
                r0 = this.v3;
                break;
        }
        
        // Then handle the remainder shift
        int remainderShift = n % 64;
        if (remainderShift != 0) {
            r0 = (r0 << remainderShift) | (r1 >>> (64 - remainderShift));
            r1 = (r1 << remainderShift) | (r2 >>> (64 - remainderShift));
            r2 = (r2 << remainderShift) | (r3 >>> (64 - remainderShift));
            r3 = r3 << remainderShift;
        }
        
        this.v0 = r0; this.v1 = r1; this.v2 = r2; this.v3 = r3;
    }

    public UInt256 shiftRight() 
    {
        return shiftRight(1);
    }

    public UInt256 shiftRight(int n) 
    {
        if (n == 0)
            return this;
        
        if (n >= 256)
            return UInt256.ZERO;
        
        if (n < 0)
        	return shiftLeft(Math.abs(n));
        
        // Handle shifts in multiples of 64 first
        int longShifts = n / 64;
        long r0 = 0, r1 = 0, r2 = 0, r3 = 0;
        switch (longShifts) {
            case 0:
                r0 = this.v0; r1 = this.v1; r2 = this.v2; r3 = this.v3;
                break;
            case 1:
                r1 = this.v0; r2 = this.v1; r3 = this.v2;
                break;
            case 2:
                r2 = this.v0; r3 = this.v1;
                break;
            case 3:
                r3 = this.v0;
                break;
        }
        
        // Then handle the remainder shift
        int remainderShift = n % 64;
        if (remainderShift != 0) {
            r3 = (r3 >>> remainderShift) | (r2 << (64 - remainderShift));
            r2 = (r2 >>> remainderShift) | (r1 << (64 - remainderShift));
            r1 = (r1 >>> remainderShift) | (r0 << (64 - remainderShift));
            r0 = r0 >>> remainderShift;
        }

        return new UInt256(r0, r1, r2, r3);
    }
    
    public UInt256 invert() 
    {
        return new UInt256(~this.v0, ~this.v1, ~this.v2, ~this.v3);
    }

    public UInt256 or(UInt256 other) 
    {
        return new UInt256(this.v0 | other.v0, this.v1 | other.v1, this.v2 | other.v2, this.v3 | other.v3);
    }

    private void orInternal(UInt256 other) 
    {
    	if (this.internal == false)
    		throw new UnsupportedOperationException("Not an internal UInt256");

    	this.v0 = this.v0 | other.v0;
    	this.v1 = this.v1 | other.v1;
    	this.v2 = this.v2 | other.v2;
    	this.v3 = this.v3 | other.v3;
    }

    public UInt256 and(UInt256 other) 
    {
        return new UInt256(this.v0 & other.v0, this.v1 & other.v1, this.v2 & other.v2, this.v3 & other.v3);
    }

    public UInt256 xor(UInt256 other) 
    {
        return new UInt256(this.v0 ^ other.v0, this.v1 ^ other.v1, this.v2 ^ other.v2, this.v3 ^ other.v3);
    }

    public int numberOfLeadingZeros() 
    {
        if (this.v0 != 0)
            return Long.numberOfLeadingZeros(this.v0);
        
        if (this.v1 != 0)
            return 64 + Long.numberOfLeadingZeros(this.v1);
        
        if (this.v2 != 0)
            return 128 + Long.numberOfLeadingZeros(this.v2);
        
        return 192 + Long.numberOfLeadingZeros(this.v3);
    }

    public boolean isHighBitSet() 
    {
        return (this.v0 < 0);
    }

    public boolean isZero() 
    {
        return this.v0 == 0 && this.v1 == 0 && this.v2 == 0 && this.v3 == 0;
    }

    public boolean isEven() 
    {
        return (this.v3 & 1) == 0;
    }

    public boolean isOdd() 
    {
        return (this.v3 & 1) != 0;
    }

    @Override
    public int compareTo(UInt256 other) 
    {
        int cmp = Long.compareUnsigned(this.v0, other.v0);
        if (cmp != 0) return cmp;
        cmp = Long.compareUnsigned(this.v1, other.v1);
        if (cmp != 0) return cmp;
        cmp = Long.compareUnsigned(this.v2, other.v2);
        if (cmp != 0) return cmp;
        return Long.compareUnsigned(this.v3, other.v3);
    }

    @Override
    public boolean equals(Object obj) 
    {
    	if (obj == null)
    		return false;
    	
        if (this == obj) 
        	return true;
        
        if (obj instanceof UInt256 other)
        	return this.v0 == other.v0 && this.v1 == other.v1 && this.v2 == other.v2 && this.v3 == other.v3;
        
        return false;
    }

    @Override
    public int hashCode() 
    {
        return (int) (this.v0 ^ (this.v0 >>> 32) ^ this.v1 ^ (this.v1 >>> 32) ^ this.v2 ^ (this.v2 >>> 32) ^ this.v3 ^ (this.v3 >>> 32));
    }

    @Override
    public String toString() 
    {
    	return toString(10);
    }

    public String toString(int radix) 
    {
    	if (radix < Character.MIN_RADIX || radix > Character.MAX_RADIX)
   	      	throw new IllegalArgumentException("Illegal radix: " + radix);

    	if (isZero())
    		return "0";
    	
    	// Determine approximately the quantity of bits used to represent the UInt256 value
    	int bitSize;
    	if (this.v3 != 0) bitSize = 256;
    	else if (this.v2 != 0) bitSize = 192;
    	else if (this.v1 != 0) bitSize = 128;
    	else bitSize = 64;
    	    
    	// Allocate StringBuilder enough to accommodate half the maximum characters based on the bits used and the radix
    	int bufferSize = (int) Math.ceil(bitSize / Math.log(radix) * Math.log(2));
    	StringBuilder sb = new StringBuilder(bufferSize);
    	
   	    UInt256 n = new UInt256(this.v0, this.v1, this.v2, this.v3, true);
   	    UInt256 r = UInt256.from(radix);
   	    while (n.isZero() == false) 
   	    {
   	    	UInt256 digit = n.remainder(r);
   	    	sb.append(Character.forDigit((int)(digit.getLow() & 0xFFFFFFFF), radix));
    	    n.divideInternal(r);
    	}
   	    
    	return sb.reverse().toString();
    }

    public byte[] toTruncatedByteArray() 
    {
        byte[] bytes = toByteArray();
        int offset = 0;
        for (int i = 0; i < bytes.length; i++) 
        {
            if (bytes[i] != 0)
                break;
            
            offset++;
        }
        
        if (offset == bytes.length)
            offset--;
        
        byte[] truncated = new byte[bytes.length - offset];
        System.arraycopy(bytes, offset, truncated, 0, bytes.length - offset);
        return truncated;
    }

    private boolean getBit(int n) 
    {
        if (n < 0 || n >= SIZE)
            throw new IndexOutOfBoundsException();
        
        long word = (n < 64) ? this.v3 : (n < 128) ? this.v2 : (n < 192) ? this.v1 : this.v0;
        return ((word >>> (n % 64)) & 1) != 0;
    }

    private UInt256 setBit(int n) 
    {
        if (n < 0 || n >= SIZE)
            throw new IndexOutOfBoundsException();
        
        if (n < 64)
            return new UInt256(this.v0, this.v1, this.v2, this.v3 | (1L << n));
        else if (n < 128)
            return new UInt256(this.v0, this.v1, this.v2 | (1L << (n - 64)), this.v3);
        else if (n < 192)
            return new UInt256(this.v0, this.v1 | (1L << (n - 128)), this.v2, this.v3);
        else
            return new UInt256(this.v0 | (1L << (n - 192)), this.v1, this.v2, this.v3);
    }
    
    private void setBitInternal(int n)
    {
    	if (this.internal == false)
    		throw new UnsupportedOperationException("Not an internal UInt256");

    	if (n < 0 || n >= SIZE)
            throw new IndexOutOfBoundsException();
        
        if (n < 64)
            this.v3 = this.v3 | (1L << n);
        else if (n < 128)
            this.v2 = this.v2 | (1L << (n - 64));
        else if (n < 192)
            this.v1 = this.v1 | (1L << (n - 128));
        else
            this.v0 = this.v0 | (1L << (n - 192));
    }

    public UInt256 increment() 
    {
        long sum3 = this.v3 + 1;
        long carry = (sum3 == 0) ? 1 : 0;

        long sum2 = this.v2 + carry;
        carry = (sum2 == 0 && carry == 1) ? 1 : 0;

        long sum1 = this.v1 + carry;
        carry = (sum1 == 0 && carry == 1) ? 1 : 0;

        long sum0 = this.v0 + carry;

        return new UInt256(sum0, sum1, sum2, sum3);
    }

    public UInt256 decrement() 
    {
        long diff3 = this.v3 - 1;
        long borrow = (this.v3 == 0) ? 1 : 0;

        long diff2 = this.v2 - borrow;
        borrow = (this.v2 == 0 && borrow == 1) ? 1 : 0;

        long diff1 = this.v1 - borrow;
        borrow = (this.v1 == 0 && borrow == 1) ? 1 : 0;

        long diff0 = this.v0 - borrow;

        return new UInt256(diff0, diff1, diff2, diff3);
    }
    
    public long getHigh()
    {
    	return this.v0;
    }
    
    public long getMidHigh()
    {
    	return this.v1;
    }

    public long getMidLow()
    {
    	return this.v2;
    }

    public long getLow()
    {
    	return this.v3;
    }
}