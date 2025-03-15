package org.radix.hyperscale.network;

import java.util.Collection;
import org.radix.hyperscale.common.Primitive;

public interface GossipReceiver<T extends Primitive> {
  void receive(final Collection<T> objects, final AbstractConnection connection) throws Throwable;
}
