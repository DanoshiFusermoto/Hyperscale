package org.radix.hyperscale.ledger.events;

import java.util.Objects;
import org.radix.hyperscale.ledger.PendingState;
import org.radix.hyperscale.ledger.primitives.StateCertificate;

public final class StateCertificateEvent extends CertificateEvent<StateCertificate> {
  private final PendingState pendingState;

  public StateCertificateEvent(
      final StateCertificate certificate, final PendingState pendingState) {
    super(certificate);

    Objects.requireNonNull(pendingState, "Pending state is null");

    if (certificate.getAddress().equals(pendingState.getAddress()) == false)
      throw new IllegalArgumentException(
          "State certificate "
              + certificate.getAddress()
              + " does not match pending state "
              + pendingState.getAddress());

    if (certificate.getBlock().equals(pendingState.getBlockHeader().getHash()) == false)
      throw new IllegalArgumentException(
          "State certificate block hash "
              + certificate.getHash()
              + " does not match pending state block hash "
              + pendingState.getBlockHeader().getHash());

    if (certificate.getAtom().equals(pendingState.getAtom().getHash()) == false)
      throw new IllegalArgumentException(
          "State certificate atom hash "
              + certificate.getHash()
              + " does not match pending state atom hash "
              + pendingState.getAtom().getHash());

    this.pendingState = pendingState;
  }

  public PendingState getPendingState() {
    return this.pendingState;
  }
}
