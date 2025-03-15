package org.radix.hyperscale.database.vamos;

class LockInternalKeyTimeoutException extends LockTimeoutException {
  /** */
  private static final long serialVersionUID = 2480939955649960930L;

  private final Database database;

  LockInternalKeyTimeoutException(InternalKey key, Database database, Transaction transaction) {
    super(
        "Lock timeout for internal key " + key + " on database " + database.getName(),
        key,
        transaction);

    this.database = database;
  }

  InternalKey getKey() {
    return getTarget();
  }

  Database getDatabase() {
    return this.database;
  }
}
