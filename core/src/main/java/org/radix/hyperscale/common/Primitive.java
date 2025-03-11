package org.radix.hyperscale.common;

import org.radix.hyperscale.crypto.Hashable;

public interface Primitive extends Hashable
{
	default boolean isDeferredPersist()
	{
		return false;
	}
}
