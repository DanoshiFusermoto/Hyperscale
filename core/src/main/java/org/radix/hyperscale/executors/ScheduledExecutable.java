package org.radix.hyperscale.executors;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.radix.hyperscale.utils.Numbers;

public abstract class ScheduledExecutable extends Executable
{
	private final long delay;
//	private final long recurrentDelay;
	private final TimeUnit unit;

	protected ScheduledExecutable(final long delay, final TimeUnit unit)
	{
		super();
		
		Numbers.isNegative(delay, "Delay is negative");
		this.unit = Objects.requireNonNull(unit, "Time unit for scheduled executable is null");
		this.delay = delay;
	}

	public long getDelay() 
	{ 
		return this.delay; 
	}

	public TimeUnit getTimeUnit() 
	{ 
		return this.unit; 
	}
}

