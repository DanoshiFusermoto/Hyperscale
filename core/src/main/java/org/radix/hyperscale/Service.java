package org.radix.hyperscale;

import org.radix.hyperscale.exceptions.ServiceException;
import org.radix.hyperscale.exceptions.StartupException;
import org.radix.hyperscale.exceptions.TerminationException;

public interface Service
{
	public void start() throws StartupException;
	
	public void stop() throws TerminationException;
	
	public default void clean() throws ServiceException
	{
		// Does nothing by default
	}
	
	public default String getName()
	{
		return this.getClass().getSimpleName();
	}
}
