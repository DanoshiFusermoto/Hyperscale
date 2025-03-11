package org.radix.hyperscale.executors;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import org.radix.hyperscale.utils.Numbers;

public abstract class LatchedProcessor extends Executable
{
	private final long delay;
	private final TimeUnit timeUnit;
	private final boolean onSignalOnly;
	
	private volatile Thread thread;
	private volatile boolean signalled;
	private final AtomicReference<Thread> latch;

	protected LatchedProcessor()
	{
		this(true, 1, TimeUnit.SECONDS);
	}

	protected LatchedProcessor(final long delay, final TimeUnit timeUnit)
	{
		this(false, delay, timeUnit);
	}

	private LatchedProcessor(final boolean onSignalOnly, final long delay, final TimeUnit timeUnit)
	{
		super();
		
		Numbers.isNegative(delay, "Initial delay is negative");
		Objects.requireNonNull(timeUnit, "Time unit for scheduled executable is null");
		
		this.timeUnit = timeUnit;
		this.delay = delay;
		this.latch = new AtomicReference<>();
		this.onSignalOnly = onSignalOnly;
	}

	public final long getDelay() 
	{ 
		return this.delay; 
	}

	public final TimeUnit getTimeUnit() 
	{ 
		return this.timeUnit; 
	}

	@Override
	public final void execute() 
	{
		this.thread = Thread.currentThread();
		
		try 
		{
			while(isTerminated() == false)
			{
				if (this.signalled == false)
				{
					this.latch.set(this.thread);
	                LockSupport.parkNanos(this.timeUnit.toNanos(this.delay));
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