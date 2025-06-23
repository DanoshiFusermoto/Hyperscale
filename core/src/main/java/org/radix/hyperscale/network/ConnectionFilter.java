package org.radix.hyperscale.network;

public interface ConnectionFilter<T extends AbstractConnection> 
{
	public boolean filter(final T connection);
}
