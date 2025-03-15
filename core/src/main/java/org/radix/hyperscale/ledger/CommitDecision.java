package org.radix.hyperscale.ledger;

import com.fasterxml.jackson.annotation.JsonValue;

public enum CommitDecision {
  ACCEPT,
  REJECT,
  ERROR,
  UNKNOWN;

  @JsonValue
  @Override
  public String toString() {
    return this.name();
  }
}
