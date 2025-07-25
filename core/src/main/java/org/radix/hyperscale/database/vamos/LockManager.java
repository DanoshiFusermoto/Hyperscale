package org.radix.hyperscale.database.vamos;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.radix.hyperscale.collections.SimpleObjectPool;
import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;

public class LockManager 
{
	private static final Logger vamosLog = Logging.getLogger("vamos");
	
	private static final int NUM_LOCK_MUTEXS = 256;
	
	private final Environment environment;
	
	private final SimpleObjectPool<Lock<InternalKey>> keyLockPool;
	private final Map<InternalKey, Lock<InternalKey>> keyLocks;
	private final SimpleObjectPool<Lock<IndexNodeID>> indexLockPool;
	private final Map<IndexNodeID, Lock<IndexNodeID>> indexLocks;
	private final Object[] lockMutexs;
	
    // Tracking metrics
    private final AtomicLong keyLockCount = new AtomicLong(0);
    private final AtomicLong nodeLockCount = new AtomicLong(0);
    private final AtomicLong totalKeyLockWaitTime = new AtomicLong(0);
    private final AtomicLong totalNodeLockWaitTime = new AtomicLong(0);
	
	LockManager(final Environment environment)
	{
		this.environment = environment;
		
		this.keyLockPool = new SimpleObjectPool<Lock<InternalKey>>("Vamos Internal Key Lock", 1<<16, Lock::new, (l) -> true);
        this.indexLockPool = new SimpleObjectPool<Lock<IndexNodeID>>("Vamos Index Node Lock", 1<<16, Lock::new, (l) -> true);
        
        this.keyLocks = new ConcurrentHashMap<InternalKey, Lock<InternalKey>>(1<<8);
        this.indexLocks = new ConcurrentHashMap<IndexNodeID, Lock<IndexNodeID>>(1<<8);
        
        this.lockMutexs = new Object[NUM_LOCK_MUTEXS];
        for(int m = 0 ; m < NUM_LOCK_MUTEXS ; m++) this.lockMutexs[m] = new Object();
    }

    // Metrics retrieval methods
    public long getTotalKeyLockWaitTime() 
    {
        return this.totalKeyLockWaitTime.get() / 1_000_000;
    }

    public long getKeyLockCount() 
    {
        return this.keyLockCount.get();
    }

    public double getAverageKeyLockWaitTime() 
    {
        long count = this.keyLockCount.get();
        return count > 0 ? (double) (this.totalKeyLockWaitTime.get() / 1_000_000) / count : 0;
    }
	
    public long getTotalNodeLockWaitTime() 
    {
        return this.totalNodeLockWaitTime.get() / 1_000_000;
    }

    public long getNodeLockCount() 
    {
        return this.nodeLockCount.get();
    }

    public double getAverageNodeLockWaitTime() 
    {
        long count = this.nodeLockCount.get();
        return count > 0 ? (double) (this.totalNodeLockWaitTime.get() / 1_000_000) / count : 0;
    }

    private Object mutex(final Object key) 
    {
        int lockMutexIndex = (key.hashCode() & Integer.MAX_VALUE) % NUM_LOCK_MUTEXS;
        return this.lockMutexs[lockMutexIndex];
    }

    boolean isLocked(final InternalKey key)
    {
        final Object mutex = mutex(key);
        synchronized(mutex) 
        {
        	return this.keyLocks.containsKey(key);
        }
    }

    boolean isLockedBy(final InternalKey key, final Transaction transaction)
    {
        final Object mutex = mutex(key);
        synchronized(mutex) 
        {
	    	final Lock<InternalKey> lock = this.keyLocks.get(key);
	        if (lock == null)
	        	return false;
	            
	        return lock.hasLock(transaction);
        }
    }
    
    void lock(final InternalKey key, final Transaction transaction, final long time, final TimeUnit unit) throws InterruptedException, LockInternalKeyTimeoutException
    {
        Lock<InternalKey> lock;
        final Object mutex = mutex(key);

        final long lockStartTime = System.nanoTime();
        try
        {
	        synchronized(mutex) 
	        {
	        	lock = this.keyLocks.get(key);
	            if (lock == null || lock.isStale())
	            {
	            	if (vamosLog.hasLevel(Logging.DEBUG))
	                {
	            		if (lock == null)
	                        vamosLog.debug("Created new lock for "+key+" on database '"+this.environment.getDatabase(key.getDatabaseID()).getName()+"'");
	            		else if(lock.isStale())
	            			vamosLog.debug("Replacing stale lock for "+key+" on database '"+this.environment.getDatabase(key.getDatabaseID()).getName()+"'");
	                }
	
	                lock = this.keyLockPool.acquire();
	                lock.reset(key);
	                this.keyLocks.put(key, lock);
	            }
	            
	            lock.signal(transaction);
	        }
	
	        if (lock.tryLock(transaction, time, unit) == false)
	            throw new LockInternalKeyTimeoutException(key, this.environment.getDatabase(key.getDatabaseID()), transaction);
	        
	        if (vamosLog.hasLevel(Logging.DEBUG))
	            vamosLog.debug("Locked index key "+key+" on database '"+this.environment.getDatabase(key.getDatabaseID()).getName()+"' to transaction "+transaction);
        }
        finally
        {
        	this.keyLockCount.incrementAndGet();
        	this.totalKeyLockWaitTime.addAndGet(System.nanoTime()-lockStartTime);
        }        
    }
    
    void unlock(final InternalKey key, final Transaction transaction)
    {
        final Object mutex = mutex(key);
        synchronized(mutex) 
        {
            Lock<InternalKey> lock = this.keyLocks.get(key);
            if (lock == null)
            	throw new IllegalStateException("Lock for key "+key+" on database '"+this.environment.getDatabase(key.getDatabaseID()).getName()+"' is null");

            lock.unlock(transaction);

            if (lock.isStale())
            {
            	if (vamosLog.hasLevel(Logging.DEBUG))
            		vamosLog.debug("Removed stale lock "+key+" on database '"+this.environment.getDatabase(key.getDatabaseID()).getName()+"' in transaction "+transaction);

            	this.keyLockPool.release(lock);
            	this.keyLocks.remove(key);
            }
        }
        
        if (vamosLog.hasLevel(Logging.DEBUG))
            vamosLog.debug("Unlocked key "+key+" on database '"+this.environment.getDatabase(key.getDatabaseID()).getName()+"' in transaction "+transaction);
    }
    
    boolean isLocked(final IndexNodeID indexNodeID)
    {
        final Object mutex = mutex(indexNodeID);
        synchronized(mutex) 
        {
        	return this.indexLocks.containsKey(indexNodeID);
        }
    }

    boolean isLockedBy(final IndexNodeID indexNodeID, final Transaction transaction)
    {
        final Object mutex = mutex(indexNodeID);
        synchronized(mutex) 
        {
	    	Lock<IndexNodeID> lock = this.indexLocks.get(indexNodeID);
	        if (lock == null)
	        	return false;
	            
	        return lock.hasLock(transaction);
        }
    }
    
    boolean lock(final IndexNodeID indexNodeID, final Transaction transaction, final long time, final TimeUnit unit) throws InterruptedException, LockIndexNodeTimeoutException
    {
        Lock<IndexNodeID> lock;
        Object mutex = mutex(indexNodeID);

        final long lockStartTime = System.nanoTime();
        try
        {
	        synchronized(mutex) 
	        {
	            lock = this.indexLocks.get(indexNodeID);
	            if (lock == null || lock.isStale())
	            {
	            	if (vamosLog.hasLevel(Logging.DEBUG))
	                {
	            		if (lock == null)
	                        vamosLog.debug("Created new lock for index node "+indexNodeID);
	            		else if (lock.isStale())
	            			vamosLog.debug("Replacing stale lock for index node "+indexNodeID);
	                }
	
	                lock = this.indexLockPool.acquire();
	                lock.reset(indexNodeID);
	                this.indexLocks.put(indexNodeID, lock);
	            }
	
	            lock.signal(transaction);
	        }
	        
	        if (lock.tryLock(transaction, time, unit) == false)
	        {
	            if (time != 0)
	                throw new LockIndexNodeTimeoutException(indexNodeID, transaction);
	            
	            return false;
	        }
	
	        if (vamosLog.hasLevel(Logging.DEBUG))
	            vamosLog.debug("Locked index node "+indexNodeID+" in transaction "+transaction);
	
	        return true;
        }
        finally
        {
        	this.nodeLockCount.incrementAndGet();
        	this.totalNodeLockWaitTime.addAndGet(System.nanoTime()-lockStartTime);
        }
    }
    
    void unlock(final IndexNodeID indexNodeID, final Transaction transaction)
    {
        final Object mutex = mutex(indexNodeID);
        synchronized(mutex) 
        {
            Lock<IndexNodeID> lock = this.indexLocks.get(indexNodeID);
            if (lock == null)
            	throw new IllegalStateException("Lock for index node "+indexNodeID+" is null");
        
            lock.unlock(transaction);
                
            if (lock.isStale())
            {
            	if (vamosLog.hasLevel(Logging.DEBUG))
            		vamosLog.debug("Removed stale lock for index node "+indexNodeID+" in transaction "+transaction);

            	this.indexLockPool.release(lock);
            	this.indexLocks.remove(indexNodeID);
            }
        }
        
        if (vamosLog.hasLevel(Logging.DEBUG))
            vamosLog.debug("Unlocked index node "+indexNodeID+" in transaction "+transaction);
    }
}

