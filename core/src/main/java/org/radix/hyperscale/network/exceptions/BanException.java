package org.radix.hyperscale.network.exceptions;

@SuppressWarnings("serial")
public class BanException extends Exception {
  public BanException() {
    super();
  }

  public BanException(final String message, final Throwable throwable) {
    super(message, throwable);
  }

  public BanException(final String message) {
    super(message);
  }

  public BanException(final Throwable throwable) {
    super(throwable);
  }
}
