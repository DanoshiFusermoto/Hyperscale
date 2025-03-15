package org.radix.hyperscale.ledger.events;

import java.util.Objects;
import org.radix.hyperscale.crypto.Certificate;
import org.radix.hyperscale.events.Event;

abstract class CertificateEvent<T extends Certificate> implements Event {
  private final T certificate;

  CertificateEvent(final T certificate) {
    this.certificate = Objects.requireNonNull(certificate, "Certificate is null");
  }

  public final T getCertificate() {
    return this.certificate;
  }
}
