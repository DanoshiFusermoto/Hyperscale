package org.radix.hyperscale.network;

import java.io.IOException;

public interface ConnectionFilter<T extends AbstractConnection> 
{
	public boolean filter(final T connection);
}
