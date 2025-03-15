package org.radix.hyperscale.network;

import com.fasterxml.jackson.annotation.JsonValue;

// TODO shouldn't be an enum if there are "parameters"
public enum Protocol {
  TCP(false),
  UDP(true);

  private final boolean connectionless;

  Protocol(boolean connectionless) {
    this.connectionless = connectionless;
  }

  public boolean isConnectionless() {
    return this.connectionless;
  }

  @JsonValue
  @Override
  public String toString() {
    return name();
  }
}
