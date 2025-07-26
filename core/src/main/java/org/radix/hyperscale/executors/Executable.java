package org.radix.hyperscale.executors;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public abstract class Executable implements Runnable
{
	private static final AtomicLong IDIncrementer = new AtomicLong(0);

	private enum State { 
        PENDING,      
        RUNNING,      
        COMPLETED,    
        CANCELLED,    
        FAILED        
    }
	
	private final long id = IDIncrementer.incrementAndGet();

	private final AtomicReference<Thread> thread = new AtomicReference<Thread>();
	private CountDownLatch completionLatch;
	
    private final AtomicReference<State> state = new AtomicReference<>(State.PENDING);
	private volatile Throwable thrown = null;
	private volatile boolean terminate = false;

	public final boolean isTerminate()
	{
		return this.terminate;
	}

	// TODO need to be able to await completion
	public final void terminate()
	{
		this.terminate = true;
	}

	public final boolean isRunning() 
	{
        return this.state.get() == State.RUNNING;
    }
	
	public final boolean isCancelled()
	{
		 return this.state.get() == State.CANCELLED;
	}

	public final boolean isDone()
	{
		State current = this.state.get();
        return current == State.COMPLETED || current == State.CANCELLED || current == State.FAILED;
    }
	
	public abstract void execute();

	protected void onCancelled()
	{
		// Stub function for abstract class
	}

	protected void onThrown(final Throwable thrown)
	{
		// Stub function for abstract class
	}

	protected void onCompleted()
	{
		// Stub function for abstract class
	}

	@Override
	public final void run()
	{
		if (this.state.compareAndSet(State.PENDING, State.RUNNING) == false) 
		{
            final State currentState = this.state.get();
            if (currentState == State.CANCELLED)
                return; // Already cancelled, don't run
            else
                throw new IllegalStateException("Executable "+this+" is in state "+currentState);
        }
		
		if (this.thread.compareAndSet(null, Thread.currentThread()) == false)
			throw new IllegalStateException("Executable "+this+" is running");

		try
		{
			this.completionLatch = new CountDownLatch(1);

			execute();
			
			if (this.state.compareAndSet(State.RUNNING, State.COMPLETED) == true)
                onCompleted();
            else
            	onCancelled();
		}
		// TODO check this isnt needed
		catch (Throwable t)
		{
			this.thrown = t;
			if (this.state.compareAndSet(State.RUNNING, State.FAILED) == true)
                onThrown(this.thrown);
            else
                onCancelled();
		}
		finally
		{
			// Always clean up execution state
            this.thread.set(null);
            if (this.completionLatch != null)
            	this.completionLatch.countDown();
		}
	}
	
	public final boolean cancel()
	{
		return cancel(false);
	}
	
	public final boolean cancel(boolean force)
	{
		final State currentState = this.state.get();
        switch (currentState) 
        {
            case COMPLETED:
            case FAILED:
                return false;
                
            case CANCELLED:
                throw new IllegalStateException("Executable " + this + " is already cancelled");
                
            case PENDING:
                if (this.state.compareAndSet(State.PENDING, State.CANCELLED) == true) 
                {
                    onCancelled();
                    return true;
                } 
                else
                    return cancel(force);
            case RUNNING:
                if (this.state.compareAndSet(State.RUNNING, State.CANCELLED) == true) 
                {
                	final Thread runningThread = this.thread.get();
                    if (runningThread != null) 
                    {
                        if (force == false) 
                        {
                        	this.terminate = true;
                            if (this.completionLatch != null)
                            {
								try
								{
									this.completionLatch.await();
								} 
								catch (InterruptedException e)
								{
									Thread.currentThread().interrupt();
									cancel(true);
								}
                            }
                        } 
                        else
                            // Note: finalization will happen in the finally block of run()
                            runningThread.interrupt();
                    }
                    return true;
                } 
                else
                    return false;
            default:
                throw new IllegalStateException("Unknown state: " + currentState);
        }
	}
	
	public final long getID()
	{
		return this.id;
	}

	@Override
	public boolean equals(final Object other)
	{
		if (other == null)
			return false;

		if (other == this)
			return true;

		if (other instanceof Executable executable)
			return this.id == executable.id;

		return false;
	}

	@Override
	public int hashCode()
	{
		return (int) (this.id & 0xFFFFFFFF);
	}

	@Override
	public String toString()
	{
		return "ID: "+this.id;
	}
}
