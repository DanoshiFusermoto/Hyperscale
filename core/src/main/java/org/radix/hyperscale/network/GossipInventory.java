package org.radix.hyperscale.network;

import java.util.Collection;
import org.radix.hyperscale.common.Primitive;
import org.radix.hyperscale.crypto.Hash;

public interface GossipInventory {
  Collection<Hash> required(
      final Class<? extends Primitive> type,
      final Collection<Hash> items,
      final AbstractConnection connection)
      throws Throwable;
}
