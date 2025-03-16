package org.radix.hyperscale.common;

import java.util.concurrent.TimeUnit;

public abstract class ExtendedObject extends BasicObject {
  private long witnessedAt;

  protected ExtendedObject() {
    super();

    this.witnessedAt = System.currentTimeMillis();
  }

  public long witnessedAt() {
    return this.witnessedAt;
  }

  public long getAge(TimeUnit unit) {
    long ms = System.currentTimeMillis() - this.witnessedAt;

    return unit.convert(ms, TimeUnit.MILLISECONDS);
  }
}
