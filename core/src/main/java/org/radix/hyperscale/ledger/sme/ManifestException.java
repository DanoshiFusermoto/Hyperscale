package org.radix.hyperscale.ledger.sme;

public class ManifestException extends Exception {
  /** */
  private static final long serialVersionUID = 1L;

  ManifestException(String message) {
    super(message);
  }

  ManifestException(String message, Throwable ex) {
    super(message, ex);
  }

  ManifestException(Throwable ex) {
    super(ex);
  }
}
