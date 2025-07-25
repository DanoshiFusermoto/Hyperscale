package org.radix.hyperscale.collections;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.function.Supplier;

public final class SimpleObjectPool<T> extends AbstractObjectPool<T>
{
    private final Deque<T> pool;
    private final AtomicInteger poolCount;

    public SimpleObjectPool(final String label, final int capacity, final Supplier<T> supplier, final Predicate<T> recycler) 
    {
    	super(label, capacity, supplier, recycler);

        this.pool = new ArrayDeque<T>(capacity);
        this.poolCount = new AtomicInteger(0);
    }
	    
	/**
     * Acquire an object from the pool
     * 
     * @return an object of type T
     */
    public T acquire() 
    {
        T object;
        synchronized(this.pool)
        {
        	object = this.pool.poll();
        }
        
        if (object != null) 
        {
        	this.poolCount.decrementAndGet();
        	incrementRecycled();
        }
        else
	        object = instantiate();

        incrementAssigned();
        return object;
    }
    
    public PoolReturnStatus release(final T object)
    {
    	final PoolReturnStatus status = revoke(object);
    	switch(status)
    	{
    	case PoolReturnStatus.DISCARDED:
    	case PoolReturnStatus.FULL:
    		incrementDiscarded();
    	case PoolReturnStatus.RETURNED:
    		incrementReleased();
    		break;
    	default:
    		break;
    	}
       	return status;
    }

    private PoolReturnStatus revoke(final T object) 
    {
        final int bcCheck = this.poolCount.incrementAndGet();
        final boolean recycle = recyclable(object);
        
        if (bcCheck <= capacity() && recycle == true) 
        {
            synchronized(this.pool)
            {
            	this.pool.offer(object);
            }
            return PoolReturnStatus.RETURNED;
        } 
        else
        {
            this.poolCount.decrementAndGet();

            if (recycle == false)
            	return PoolReturnStatus.DISCARDED;
            else
            	return PoolReturnStatus.FULL;
        }
    }
    
    /**
     * Clear all buffers from the pool
     */
    public void clear() 
    {
        this.pool.clear();
        this.poolCount.set(0);
    }
    
    @Override
    public String toString() 
    {
    	return String.format("ObjectPool{%s-%d, pooled=%d, allocated=%d, assigned=%d, released=%d, recycled=%d, discarded=%d, expired=%d}", 
    							getLabel(), hashCode(), this.poolCount.get(), 
    							totalAllocated(), totalAssigned(), totalReleased(), 
    							totalRecycled(), totalDiscarded(), totalExpired());
    }
}