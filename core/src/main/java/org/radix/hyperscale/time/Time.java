package org.radix.hyperscale.time;

import java.util.Objects;

public final class Time {
  public static final int MAXIMUM_DRIFT = 30;

  private static Time instance = null;

  public static Time getDefault() {
    if (instance == null) throw new RuntimeException("Time instance not set");

    return instance;
  }

  public static Time createAsDefault(final TimeProvider provider) {
    if (instance != null) throw new RuntimeException("Default time provider already set");

    instance = new Time(provider);

    return instance;
  }

  public static Time clearDefault() {
    Time time = getDefault();
    instance = null;
    return time;
  }

  public static long getLedgerTimeMS() {
    return getDefault().getProvider().getLedgerTimeMS();
  }

  public static long getLedgerTimeSeconds() {
    return getDefault().getProvider().getLedgerTimeSeconds();
  }

  public static long getSystemTime() {
    return getDefault().getProvider().getSystemTime();
  }

  private final TimeProvider provider;

  private Time(final TimeProvider provider) {
    Objects.requireNonNull(provider, "Time provider is null");
    this.provider = provider;
  }

  private TimeProvider getProvider() {
    return this.provider;
  }
}
