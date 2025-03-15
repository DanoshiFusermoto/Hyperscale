package org.radix.hyperscale;

import org.radix.hyperscale.exceptions.ServiceException;
import org.radix.hyperscale.exceptions.StartupException;
import org.radix.hyperscale.exceptions.TerminationException;

public interface Service {
  void start() throws StartupException;

  void stop() throws TerminationException;

  default void clean() throws ServiceException {
    // Does nothing by default
  }

  default String getName() {
    return this.getClass().getSimpleName();
  }
}
