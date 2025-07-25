package org.radix.hyperscale.collections;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.function.Supplier;

abstract class AbstractObjectPool<T> extends ObjectPool<T>
{
    private final int capacity;
    private final Supplier<T> supplier;
    private final Predicate<T> recycler;
    
    private final AtomicLong allocated;
    private final AtomicLong recycled;
    private final AtomicLong discarded;
    private final AtomicLong expired;
    private final AtomicLong assigned;
    private final AtomicLong released;

    AbstractObjectPool(final String label, final int capacity, final Supplier<T> supplier, final Predicate<T> recycler)
	{
		super(label);
		
    	this.capacity = capacity;
    	this.supplier = supplier;
    	this.recycler = recycler;
    	
        this.allocated = new AtomicLong(0);
        this.recycled = new AtomicLong(0);
        this.discarded = new AtomicLong(0);
        this.expired = new AtomicLong(0);
        this.assigned = new AtomicLong(0);
        this.released = new AtomicLong(0);
	}
    
    final public int capacity()
    {
    	return this.capacity;
    }
    
    final long incrementAllocated()
    {
    	return this.allocated.incrementAndGet();
    }
    
    final long totalAllocated()
    {
    	return this.allocated.get();
    }

    final long incrementRecycled()
    {
    	return this.recycled.incrementAndGet();
    }

    final long totalRecycled()
    {
    	return this.recycled.get();
    }

    final long incrementDiscarded()
    {
    	return this.discarded.incrementAndGet();
    }

    final long totalDiscarded()
    {
    	return this.discarded.get();
    }

    final long incrementExpired()
    {
    	return this.expired.incrementAndGet();
    }

    final long totalExpired()
    {
    	return this.expired.get();
    }

    final long incrementAssigned()
    {
    	return this.assigned.incrementAndGet();
    }

    final long totalAssigned()
    {
    	return this.assigned.get();
    }

    final long incrementReleased()
    {
    	return this.released.incrementAndGet();
    }

    final long totalReleased()
    {
    	return this.released.get();
    }

    final T instantiate() 
    {
    	try
    	{
    		return this.supplier.get();
    	}
    	finally
    	{
    		incrementAllocated();
    	}
    }

    final boolean recyclable(final T object)
    {
    	Objects.requireNonNull(object, "Object to recycle is null");
    	
    	if (this.recycler == null)
    		return true;
    	
    	return this.recycler.test(object);
    }
}
