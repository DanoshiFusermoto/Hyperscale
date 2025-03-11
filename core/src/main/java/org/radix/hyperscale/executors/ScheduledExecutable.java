package org.radix.hyperscale.executors;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.radix.hyperscale.utils.Numbers;

public abstract class ScheduledExecutable extends Executable
{
	private final long initialDelay;
	private final long recurrentDelay;
	private final TimeUnit unit;

	protected ScheduledExecutable(final long recurrentDelay, final TimeUnit unit)
	{
		this(0, recurrentDelay, unit);
	}
	
	protected ScheduledExecutable(final long initialDelay, final long recurrentDelay, final TimeUnit unit)
	{
		super();
		
		Numbers.isNegative(initialDelay, "Initial delay is negative");
		Numbers.isNegative(recurrentDelay, "Recurrent delay is negative");
		this.unit = Objects.requireNonNull(unit, "Time unit for scheduled executable is null");

		this.initialDelay = initialDelay;
		this.recurrentDelay = recurrentDelay;
	}

	public long getInitialDelay() 
	{ 
		return this.initialDelay; 
	}

	public long getRecurrentDelay() 
	{ 
		return this.recurrentDelay; 
	}
	
	public TimeUnit getTimeUnit() 
	{ 
		return this.unit; 
	}
}

