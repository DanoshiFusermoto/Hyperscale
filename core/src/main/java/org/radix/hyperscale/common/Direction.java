package org.radix.hyperscale.common;

import com.fasterxml.jackson.annotation.JsonValue;

public enum Direction 
{
	UNDEFINED,
	OUTBOUND,
	INBOUND;

	@JsonValue
	@Override
	public String toString() 
	{
		return this.name();
	}
}