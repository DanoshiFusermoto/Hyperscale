package org.radix.hyperscale.ledger.exceptions;

import org.radix.hyperscale.exceptions.ValidationException;
import org.radix.hyperscale.ledger.BlockHeader;

public class ProposalValidationException extends ValidationException {
  /** */
  private static final long serialVersionUID = 8923106899815104442L;

  private final BlockHeader header;

  public ProposalValidationException(final BlockHeader header, final String message) {
    super(message);

    this.header = header;
  }

  public ProposalValidationException(final BlockHeader header, final Exception ex) {
    super(ex);

    this.header = header;
  }

  public ProposalValidationException(
      final BlockHeader header, final String message, final Exception ex) {
    super(message, ex);

    this.header = header;
  }

  public BlockHeader getProposal() {
    return this.header;
  }
}
