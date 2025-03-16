package org.radix.hyperscale.ledger.exceptions;

public final class LedgerException extends Exception {
  /** */
  private static final long serialVersionUID = 9160884779162293740L;

  public LedgerException(String message, Exception thrown) {
    super(message, thrown);
  }

  public LedgerException(String message) {
    super(message);
  }

  public LedgerException(Exception thrown) {
    super(thrown);
  }
}
