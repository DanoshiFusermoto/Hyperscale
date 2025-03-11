package org.radix.hyperscale.utils;

public class CustomInteger
{
	long value;
	long limit;
	
	public CustomInteger(long value, long limit)
	{
		this.value = value;
		this.limit = limit;
	}
	
	public long get()
	{
		return this.value;
	}
	
	public long increment()
	{
		if (this.value + 1 > this.limit-1)
			this.value = -this.limit;
		else
			this.value++;
		
		return this.value;
	}
	
	public long decrement()
	{
		if (this.value - 1 < -this.limit)
			this.value = this.limit - 1;
		else
			this.value--;
		
		return this.value;
	}
}

