package org.radix.hyperscale.network;

public interface ConnectionFilter<T extends AbstractConnection> {
  boolean filter(final T connection);
}
