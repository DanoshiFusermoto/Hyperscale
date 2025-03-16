package org.radix.hyperscale.utils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;

public final class Currency {
  public static final int DEFAULT_PRECISION = 9;
  public static final int MAX_PRECISION = 20;

  public static final UInt256 convert(final double decimal) {
    return convert(decimal, DEFAULT_PRECISION);
  }

  public static final UInt256 convert(final double decimal, final int precision) {
    Numbers.inRange(precision, 0, MAX_PRECISION, "Precision is out of range");
    Numbers.isNegative(decimal, "Decimal value is negative");

    return convert(BigDecimal.valueOf(decimal), DEFAULT_PRECISION);
  }

  public static final UInt256 convert(final BigDecimal decimal) {
    return convert(decimal, DEFAULT_PRECISION);
  }

  public static final UInt256 convert(final BigDecimal decimal, final int precision) {
    Numbers.inRange(precision, 0, MAX_PRECISION, "Precision is out of range");
    if (decimal.compareTo(BigDecimal.ZERO) < 0)
      throw new IllegalArgumentException("Decimal value is negative");

    BigDecimal shiftedDecimal = decimal.movePointRight(precision);
    BigInteger integer = shiftedDecimal.toBigInteger();
    return UInt256s.fromBigInteger(integer);
  }

  public static final BigDecimal convert(final UInt256 uint) {
    return convert(uint, DEFAULT_PRECISION);
  }

  public static final BigDecimal convert(final UInt256 uint, final int precision) {
    Numbers.inRange(precision, 0, MAX_PRECISION, "Precision is out of range");

    BigDecimal decimal = UInt256s.toBigDecimal(uint);
    BigDecimal shiftedDecimal = decimal.movePointLeft(precision);
    return shiftedDecimal;
  }

  public static final BigDecimal price(final UInt256 base, final UInt256 quote) {
    return price(base, DEFAULT_PRECISION, quote, DEFAULT_PRECISION);
  }

  public static final BigDecimal price(
      final UInt256 base, final int basePrecision, final UInt256 quote, final int quotePrecision) {
    BigDecimal baseBigD = convert(base, basePrecision);
    BigDecimal quoteBigD = convert(quote, quotePrecision);
    return quoteBigD.divide(baseBigD, MathContext.DECIMAL64);
  }

  private Currency() {
    throw new AssertionError("Not meant to be instantiated");
  }
}
