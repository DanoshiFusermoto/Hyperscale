package org.radix.hyperscale.ledger;

import com.fasterxml.jackson.annotation.JsonValue;

public enum Isolation {
  COMMITTED,
  PENDING;

  @JsonValue
  @Override
  public String toString() {
    return this.name();
  }
}
