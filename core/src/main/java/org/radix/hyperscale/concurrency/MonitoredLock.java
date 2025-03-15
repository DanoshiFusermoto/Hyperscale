package org.radix.hyperscale.concurrency;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public interface MonitoredLock {
  Set<MonitoredLock> locks = Collections.synchronizedSet(new HashSet<MonitoredLock>());

  static List<MonitoredLock> getLocks() {
    synchronized (locks) {
      return Collections.unmodifiableList(new ArrayList<MonitoredLock>(MonitoredLock.locks));
    }
  }

  static boolean add(final MonitoredLock lock) {
    return locks.add(lock);
  }
}
