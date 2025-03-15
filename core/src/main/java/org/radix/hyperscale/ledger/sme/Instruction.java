package org.radix.hyperscale.ledger.sme;

public interface Instruction {
  @Override
  int hashCode();

  @Override
  boolean equals(Object other);
}
