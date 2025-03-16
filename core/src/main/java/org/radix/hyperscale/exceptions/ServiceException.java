package org.radix.hyperscale.exceptions;

import java.util.Objects;

public class ServiceException extends Exception {

  /** */
  private static final long serialVersionUID = 954387361719375581L;

  private final Class<?> clazz;

  public ServiceException(final String message, final Throwable throwable) {
    super(message, throwable);

    this.clazz = null;
  }

  public ServiceException(final String message) {
    super(message);

    this.clazz = null;
  }

  public ServiceException(final Throwable throwable) {
    super(throwable);

    this.clazz = null;
  }

  public ServiceException(final Class<?> clazz) {
    super();

    this.clazz = Objects.requireNonNull(clazz, "Service class is null");
  }

  public ServiceException(final String message, final Throwable throwable, final Class<?> clazz) {
    super(message, throwable);

    this.clazz = Objects.requireNonNull(clazz, "Service class is null");
  }

  public ServiceException(final String message, final Class<?> clazz) {
    super(message);

    this.clazz = Objects.requireNonNull(clazz, "Service class is null");
  }

  public ServiceException(final Throwable throwable, final Class<?> clazz) {
    super(throwable);

    this.clazz = Objects.requireNonNull(clazz, "Startup class is null");
  }

  public Class<?> getClazz() {
    return this.clazz;
  }
}
