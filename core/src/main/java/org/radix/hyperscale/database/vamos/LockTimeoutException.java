package org.radix.hyperscale.database.vamos;

import org.radix.hyperscale.database.DatabaseException;

abstract class LockTimeoutException extends DatabaseException {
  /** */
  private static final long serialVersionUID = 7167609666702010330L;

  private final Object target;
  private final Transaction transaction;

  LockTimeoutException(String message, Object target, Transaction transaction) {
    super(message);

    this.target = target;
    this.transaction = transaction;
  }

  @SuppressWarnings("unchecked")
  <T> T getTarget() {
    return (T) this.target;
  }

  Transaction getTransaction() {
    return this.transaction;
  }
}
