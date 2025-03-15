package org.radix.hyperscale.ledger.events;

import java.util.Objects;
import org.radix.hyperscale.ledger.PendingAtom;
import org.radix.hyperscale.ledger.primitives.AtomCertificate;

public class AtomCertificateEvent extends CertificateEvent<AtomCertificate> {
  private final PendingAtom pendingAtom;

  public AtomCertificateEvent(final AtomCertificate certificate, final PendingAtom pendingAtom) {
    super(certificate);

    Objects.requireNonNull(pendingAtom, "Pending atom is null");
    if (certificate.getAtom().equals(pendingAtom.getHash()) == false)
      throw new IllegalArgumentException(
          "Atom certificate hash "
              + certificate.getHash()
              + " does not match pending atom hash "
              + pendingAtom.getHash());

    this.pendingAtom = pendingAtom;
  }

  public PendingAtom getPendingAtom() {
    return this.pendingAtom;
  }
}
