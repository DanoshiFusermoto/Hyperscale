package org.radix.hyperscale.common;

import com.fasterxml.jackson.annotation.JsonValue;

public enum Order
{
	NATURAL,
	ASCENDING,
	DESCENDING;

	@JsonValue
	@Override
	public String toString() 
	{
		return this.name();
	}
}
