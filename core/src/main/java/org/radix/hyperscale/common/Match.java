package org.radix.hyperscale.common;

import com.fasterxml.jackson.annotation.JsonValue;

public enum Match
{
	ANY,
	ALL;

	@JsonValue
	@Override
	public String toString() 
	{
		return this.name();
	}
}
