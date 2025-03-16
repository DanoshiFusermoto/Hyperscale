package org.radix.hyperscale.utils;

public final class Numbers {
  // INTS //
  public static final int inRange(int value, int low, int high, String message) {
    int rl = low;
    int rh = high;
    if (low > high) {
      rl = high;
      rh = low;
    }

    if (value >= rl && value <= rh) return value;

    if (message == null) throw new IllegalArgumentException();
    else throw new IllegalArgumentException(message);
  }

  public static final int isNegative(int value, String message) {
    if (value < 0) {
      if (message == null) throw new IllegalArgumentException();
      else throw new IllegalArgumentException(message);
    }

    return value;
  }

  public static final int isPositive(int value, String message) {
    if (value > 0) {
      if (message == null) throw new IllegalArgumentException();
      else throw new IllegalArgumentException(message);
    }

    return value;
  }

  public static final int isZero(int value, String message) {
    if (value == 0) {
      if (message == null) throw new IllegalArgumentException();
      else throw new IllegalArgumentException(message);
    }

    return value;
  }

  public static final int lessThan(int value, int bound, String message) {
    if (value < bound) {
      if (message == null) throw new IllegalArgumentException();
      else throw new IllegalArgumentException(message);
    }

    return value;
  }

  public static final int greaterThan(int value, int bound, String message) {
    if (value > bound) {
      if (message == null) throw new IllegalArgumentException();
      else throw new IllegalArgumentException(message);
    }

    return value;
  }

  public static final int equals(int value, int other, String message) {
    if (value != other) {
      if (message == null) throw new IllegalArgumentException();
      else throw new IllegalArgumentException(message);
    }

    return value;
  }

  // LONGS //
  public static final long inRange(long value, long low, long high, String message) {
    long rl = low;
    long rh = high;
    if (low > high) {
      rl = high;
      rh = low;
    }

    if (value >= rl && value <= rh) return value;

    if (message == null) throw new IllegalArgumentException();
    else throw new IllegalArgumentException(message);
  }

  public static final long isNegative(long value, String message) {
    if (value < 0) {
      if (message == null) throw new IllegalArgumentException();
      else throw new IllegalArgumentException(message);
    }

    return value;
  }

  public static final long isPositive(long value, String message) {
    if (value > 0) {
      if (message == null) throw new IllegalArgumentException();
      else throw new IllegalArgumentException(message);
    }

    return value;
  }

  public static final long isZero(long value, String message) {
    if (value == 0) {
      if (message == null) throw new IllegalArgumentException();
      else throw new IllegalArgumentException(message);
    }

    return value;
  }

  public static final long lessThan(long value, long bound, String message) {
    if (value < bound) {
      if (message == null) throw new IllegalArgumentException();
      else throw new IllegalArgumentException(message);
    }

    return value;
  }

  public static final long greaterThan(long value, long bound, String message) {
    if (value > bound) {
      if (message == null) throw new IllegalArgumentException();
      else throw new IllegalArgumentException(message);
    }

    return value;
  }

  public static final long equals(long value, long other, String message) {
    if (value != other) {
      if (message == null) throw new IllegalArgumentException();
      else throw new IllegalArgumentException(message);
    }

    return value;
  }

  // DOUBLES //
  public static final double inRange(double value, double low, double high, String message) {
    double rl = low;
    double rh = high;
    if (low > high) {
      rl = high;
      rh = low;
    }

    if (value >= rl && value <= rh) return value;

    if (message == null) throw new IllegalArgumentException();
    else throw new IllegalArgumentException(message);
  }

  public static final double isNegative(double value, String message) {
    if (value < 0) {
      if (message == null) throw new IllegalArgumentException();
      else throw new IllegalArgumentException(message);
    }

    return value;
  }

  public static final double isPositive(double value, String message) {
    if (value > 0) {
      if (message == null) throw new IllegalArgumentException();
      else throw new IllegalArgumentException(message);
    }

    return value;
  }

  public static final double isZero(double value, String message) {
    if (value == 0) {
      if (message == null) throw new IllegalArgumentException();
      else throw new IllegalArgumentException(message);
    }

    return value;
  }

  public static final double lessThan(double value, double bound, String message) {
    if (value < bound) {
      if (message == null) throw new IllegalArgumentException();
      else throw new IllegalArgumentException(message);
    }

    return value;
  }

  public static final double greaterThan(double value, double bound, String message) {
    if (value > bound) {
      if (message == null) throw new IllegalArgumentException();
      else throw new IllegalArgumentException(message);
    }

    return value;
  }

  public static final double equals(double value, double other, String message) {
    if (value != other) {
      if (message == null) throw new IllegalArgumentException();
      else throw new IllegalArgumentException(message);
    }

    return value;
  }

  private Numbers() {
    throw new AssertionError("Should not be instantiated");
  }
}
