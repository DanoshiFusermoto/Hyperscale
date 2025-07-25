package org.radix.hyperscale.database.vamos;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;

final class Lock<T> 
{
	private static final Logger locksLog = Logging.getLogger("locks");
	private static final long	DEFAULT_LOCK_WARN_THRESHOLD_MS = 250;

	private final Object monitor = new Object();
    private final AtomicInteger signals = new AtomicInteger(0);
    
    private volatile T key;
    private volatile boolean stale;
    private volatile Transaction owner;
    
    Lock() 
    {
    }
    
    boolean isStale()
    {
    	return this.stale;
    }

    boolean signal(final Transaction transaction)
    {
        synchronized(this.monitor) 
        {
        	this.signals.incrementAndGet();
            return true;
        }
    }

    boolean hasLock(final Transaction transaction)
    {
        synchronized(this.monitor) 
        {
        	if (this.owner == transaction)
        		return true;

            return false;
        }
    }
    
    boolean tryLock(final Transaction transaction, final long timeout, final TimeUnit unit) throws InterruptedException 
    {
        synchronized(this.monitor) 
        {
        	long startTime = System.nanoTime();
            try 
            {
            	if (this.owner == transaction) 
                    return true;

            	long remainingNanos = unit.toNanos(timeout);
                long deadline = startTime + remainingNanos;

                while (true) 
                {
                    if (this.owner == null) 
                    {
                        this.owner = transaction;
                        return true;
                    }

                    if (timeout == 0)
                        return false;

                    if (remainingNanos <= 0)
                        return false;

                    this.monitor.wait(remainingNanos / 1_000_000, (int) (remainingNanos % 1_000_000));
                    remainingNanos = deadline - System.nanoTime();
                }
            }
            catch(Exception e)
            {
                throw e;
            }
            finally
            {
                if (this.signals.decrementAndGet() < 0)
                    throw new IllegalStateException("Signals negative for lock on transaction "+transaction);
                
                if (this.owner == transaction)
                {
                    long lockTime = (System.nanoTime() - startTime) / 1_000_000; // Convert to ms
                    if (lockTime > Lock.DEFAULT_LOCK_WARN_THRESHOLD_MS) 
                        locksLog.warn("Lock acquisition took "+lockTime+"ms at:", new Exception("Lock acquisition stack trace"));
                }
            }
        }
    }

    void unlock(final Transaction transaction) 
    {
        synchronized(this.monitor) 
        {
            if (transaction != this.owner)
                throw new IllegalStateException("Unlock "+this.key+" is not assigned to transaction "+transaction);

            this.owner = null;

            int remainingSignals = this.signals.get();
            if (remainingSignals == 0)
            	this.stale = true;
            else if (remainingSignals < 0)
            	throw new IllegalStateException("Signals negative for unlocked lock on transaction "+transaction);

            this.monitor.notifyAll();
        }
    }

    void reset(final T key) 
    {
        synchronized(this.monitor) 
        {
            this.key = key;
            this.owner = null;
            this.stale = false;
            this.signals.set(0);
       }
    }
}