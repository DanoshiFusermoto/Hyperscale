package org.radix.hyperscale.concurrency;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;

@SuppressWarnings("serial")
public class MonitoredReadWriteLock extends ReentrantReadWriteLock implements MonitoredLock
{
    private static final Logger locksLog = Logging.getLogger("locks");
    private static final long DEFAULT_LOCK_WARN_THRESHOLD_MS = 10;

    // Tracking metrics
    private final AtomicLong readLocksCount = new AtomicLong(0);
    private final AtomicLong writeLocksCount = new AtomicLong(0);
    private final AtomicLong totalReadLockTime = new AtomicLong(0);
    private final AtomicLong totalWriteLockTime = new AtomicLong(0);
    private final AtomicLong totalReadLockWaitTime = new AtomicLong(0);
    private final AtomicLong totalWriteLockWaitTime = new AtomicLong(0);

    private final String label;
    private final long warningThresholdMS;
    private final MonitoredReadLock monitoredReadLock;
    private final MonitoredWriteLock monitoredWriteLock;

    public MonitoredReadWriteLock(final String label) 
    {
        this(label, DEFAULT_LOCK_WARN_THRESHOLD_MS);
    }
    
    public MonitoredReadWriteLock(final String label, boolean fair) 
    {
        super(fair);
        
        this.label = label;
        this.warningThresholdMS = DEFAULT_LOCK_WARN_THRESHOLD_MS;
        this.monitoredReadLock = new MonitoredReadLock(this);
        this.monitoredWriteLock = new MonitoredWriteLock(this);

        MonitoredLock.add(this);
    }

    public MonitoredReadWriteLock(final String label, long warningThresholdMS) 
    {
    	super();
    	
        this.label = label;
        this.warningThresholdMS = warningThresholdMS;
        this.monitoredReadLock = new MonitoredReadLock(this);
        this.monitoredWriteLock = new MonitoredWriteLock(this);
        
        MonitoredLock.add(this);
    }

    @Override
    public ReadLock readLock() 
    {
        return this.monitoredReadLock;
    }

    @Override
    public WriteLock writeLock() 
    {
        return this.monitoredWriteLock;
    }

    // Metrics retrieval methods
    public long getTotalReadLockWaitTime() 
    {
        return this.totalReadLockWaitTime.get() / 1_000_000;
    }

    public long getTotalWriteLockWaitTime() 
    {
        return this.totalWriteLockWaitTime.get() / 1_000_000;
    }

    public long getReadLocksCount() 
    {
        return this.readLocksCount.get();
    }

    public long getWriteLocksCount() 
    {
        return this.writeLocksCount.get();
    }

    public double getAverageReadLockWaitTime() 
    {
        long count = this.readLocksCount.get();
        return count > 0 ? (double) (this.totalReadLockTime.get() / 1_000_000) / count : 0;
    }

    public double getAverageWriteLockWaitTime() 
    {
        long count = this.writeLocksCount.get();
        return count > 0 ? (double) (this.totalWriteLockTime.get() / 1_000_000) / count : 0;
    }

    private class MonitoredReadLock extends ReadLock 
    {
        MonitoredReadLock(ReentrantReadWriteLock parent) 
        {
            super(parent);
        }

        @Override
        public void lock() 
        {
            // Check if the current thread already holds the lock
            boolean isFirstAcquisition = getReadHoldCount() == 0;
            
            long startWaitTime = System.nanoTime();
            long startLockTime = startWaitTime;
            
            try {
                super.lock();
                
                // Only track metrics for the first lock acquisition by this thread
                if (isFirstAcquisition) 
                {
                    // Record wait time
                    long waitTime = (System.nanoTime() - startWaitTime);
                    MonitoredReadWriteLock.this.totalReadLockWaitTime.addAndGet(waitTime);
                    
                    // Increment distinct lock count
                    MonitoredReadWriteLock.this.readLocksCount.incrementAndGet();
                }
                
                // Track total lock time
                startLockTime = System.nanoTime();
            } finally {
                // Ensure lock time is tracked even if an exception occurs
                long lockTime = (System.nanoTime() - startLockTime);
                MonitoredReadWriteLock.this.totalReadLockTime.addAndGet(lockTime);
                
                // Log warning if lock acquisition took too long
                long lockTimeMS = lockTime / 1_000_000; 
                if (lockTimeMS > MonitoredReadWriteLock.this.warningThresholdMS) 
                    locksLog.warn("Read lock acquisition took "+lockTimeMS+"ms ( "+getReadLocksCount()+" / "+getAverageReadLockWaitTime()+"ms / "+getTotalReadLockWaitTime()+"ms )", new Exception("Read lock acquisition stack trace"));
            }
        }
    }

    private class MonitoredWriteLock extends WriteLock 
    {
        MonitoredWriteLock(ReentrantReadWriteLock parent) 
        {
            super(parent);
        }

        @Override
        public void lock() 
        {
            // Check if the current thread already holds the lock
            boolean isFirstAcquisition = getWriteHoldCount() == 0;
            
            long startWaitTime = System.nanoTime();
            long startLockTime = startWaitTime;
            
            try 
            {
                super.lock();
                
                // Only track metrics for the first lock acquisition by this thread
                if (isFirstAcquisition) 
                {
                    // Record wait time
                    long waitTime = (System.nanoTime() - startWaitTime);
                    MonitoredReadWriteLock.this.totalWriteLockWaitTime.addAndGet(waitTime);
                    
                    // Increment distinct lock count
                    MonitoredReadWriteLock.this.writeLocksCount.incrementAndGet();
                }
                
                // Track total lock time
                startLockTime = System.nanoTime();
            } 
            finally 
            {
                // Ensure lock time is tracked even if an exception occurs
                long lockTime = (System.nanoTime() - startLockTime);
                MonitoredReadWriteLock.this.totalWriteLockTime.addAndGet(lockTime);
                
                // Log warning if lock acquisition took too long
                long lockTimeMS = lockTime / 1_000_000; 
                if (lockTimeMS > MonitoredReadWriteLock.this.warningThresholdMS) 
                    locksLog.warn("Write lock acquisition took "+lockTimeMS+"ms ( "+getWriteLocksCount()+" / "+getAverageWriteLockWaitTime()+"ms / "+getTotalWriteLockWaitTime()+"ms )", new Exception("Write lock acquisition stack trace"));
            }
        }
    }
    
    @Override
    public String toString()
    {
    	return this.label+" "+getClass().getSimpleName()+" [READS] "+getReadLocksCount()+" / "+getAverageReadLockWaitTime()+"ms | "+getTotalReadLockWaitTime()+"ms [WRITES] "+getWriteLocksCount()+" / "+getAverageWriteLockWaitTime()+"ms | "+getTotalWriteLockWaitTime()+"ms";
    }
}