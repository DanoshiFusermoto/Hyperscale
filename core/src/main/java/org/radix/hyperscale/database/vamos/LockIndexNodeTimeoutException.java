package org.radix.hyperscale.database.vamos;

class LockIndexNodeTimeoutException extends LockTimeoutException {
  /** */
  private static final long serialVersionUID = -5220140524426082240L;

  LockIndexNodeTimeoutException(IndexNodeID indexNodeID, Transaction transaction) {
    super("Lock timeout for index node " + indexNodeID, indexNodeID, transaction);
  }

  IndexNodeID getIndexNodeID() {
    return getTarget();
  }
}
