package org.radix.hyperscale.executors;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import org.radix.hyperscale.utils.Numbers;

public abstract class LatchedProcessor extends Executable
{
	private final long defaultDelay;
	private final TimeUnit timeUnit;
	private final boolean onSignalOnly;
	
	private volatile Thread thread;
	private volatile boolean signalled;
	private volatile long ephemeralDelay;
	private final AtomicReference<Thread> latch;

	protected LatchedProcessor()
	{
		this(true, 1, TimeUnit.SECONDS);
	}

	protected LatchedProcessor(final long defaultDelay, final TimeUnit timeUnit)
	{
		this(false, defaultDelay, timeUnit);
	}

	private LatchedProcessor(final boolean onSignalOnly, final long defaultDelay, final TimeUnit timeUnit)
	{
		super();
		
		Numbers.isNegative(defaultDelay, "Default delay is negative");
		Objects.requireNonNull(timeUnit, "Time unit for scheduled executable is null");
		
		this.timeUnit = timeUnit;
		this.defaultDelay = defaultDelay;
		this.latch = new AtomicReference<>();
		this.onSignalOnly = onSignalOnly;
	}

	public final long getDefaultDelay() 
	{ 
		return this.defaultDelay; 
	}

	public final TimeUnit getTimeUnit() 
	{ 
		return this.timeUnit; 
	}
	
	public final long getEphemeralDelay() 
	{ 
		return this.ephemeralDelay; 
	}

	public final void setEphemeralDelay(final long delay, final TimeUnit timeUnit) 
	{ 
		Numbers.isNegative(delay, "Ephemeral delay is negative");
		Objects.requireNonNull(timeUnit, "Ephemeral delay time unit is null");
		this.ephemeralDelay = this.timeUnit.convert(delay, timeUnit); 
	}

	@Override
	public final void execute() 
	{
		this.thread = Thread.currentThread();
		
		try 
		{
			while(isTerminate() == false)
			{
				if (this.signalled == false)
				{
					this.latch.set(this.thread);
					
					long delay = this.ephemeralDelay;
					if (delay == 0)
						delay = this.defaultDelay;
					this.ephemeralDelay = 0;
					
	                LockSupport.parkNanos(this.timeUnit.toNanos(delay));
	                if (this.onSignalOnly && this.latch.compareAndSet(this.thread, null))
                		continue;
				}
				this.signalled = false;
	                
                process();
			}
		}
		catch (Throwable throwable)
		{
			onError(throwable);
		}
		finally
		{
			onTerminated();
		}
	}
	
	public abstract void process();
	
	public abstract void onError(Throwable thrown);
	
	public abstract void onTerminated();

	public final void signal()
	{
		this.signalled = true;
	    Thread t = this.latch.getAndSet(null);
	    if (t != null)
	        LockSupport.unpark(t);
	}
}