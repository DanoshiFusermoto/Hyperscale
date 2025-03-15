package org.radix.hyperscale.network;

import java.util.Collection;
import org.radix.hyperscale.common.Primitive;
import org.radix.hyperscale.crypto.Hash;

public interface GossipFetcher<T extends Primitive> {
  Collection<T> fetch(final Collection<Hash> items, final AbstractConnection connection)
      throws Throwable;
}
