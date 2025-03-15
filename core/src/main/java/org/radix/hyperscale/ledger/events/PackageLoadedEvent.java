package org.radix.hyperscale.ledger.events;

import java.util.Objects;
import org.radix.hyperscale.events.Event;
import org.radix.hyperscale.ledger.sme.Package;

public class PackageLoadedEvent implements Event {
  private final Package pakage;

  public PackageLoadedEvent(final Package pakage) {
    this.pakage = Objects.requireNonNull(pakage, "Package is null");
  }

  @SuppressWarnings("unchecked")
  public final <T extends Package> T getPackage() {
    return (T) this.pakage;
  }
}
