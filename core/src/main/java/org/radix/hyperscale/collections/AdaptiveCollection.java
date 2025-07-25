package org.radix.hyperscale.collections;

import java.util.Collection;

/**
 * A collection that can transition from mutable to immutable state, unsynchronised to synchronised, and 
 * a number of other useful properties for improved collection utility.
 */
public interface AdaptiveCollection<E> extends Collection<E> 
{
    /**
     * Freezes this collection, making it immutable.
     * This is a one-way operation that cannot be reversed.
     * 
     * @return this collection for method chaining
     * @throws IllegalStateException if already frozen (optional)
     */
	default AdaptiveCollection<E> freeze()
	{
		throw new UnsupportedOperationException("Freezing is not supported");
	}
    
    /**
     * @return true if this collection has been frozen and is immutable
     */
    default boolean isFrozen()
	{
    	return false;
	}
    
    /**
     * Fixes the capacity of this collection.  Collections which have 
     * been fixed will throw an exception rather than growing to accommodate 
     * new items.
     * This is a one-way operation that cannot be reversed.
     * 
     * @return this collection for method chaining
     * @throws IllegalStateException if already frozen (optional)
     */
	default AdaptiveCollection<E> fixed()
	{
		throw new UnsupportedOperationException("Capacity fixing is not supported");
	}
    
    /**
     * @return true if this collection has a fixed capacity
     */
    default boolean isFixed()
	{
    	return false;
	}

    /**
     * Synchronises this collection, making all future accesses synchronized.
     * This is a one-way operation that cannot be reversed.
     * 
     * @return this collection for method chaining
     * @throws IllegalStateException if already synchronized (optional)
     */
	default AdaptiveCollection<E> sync()
	{
		throw new UnsupportedOperationException("Synchronization is not supported");
	}
    
    /**
     * @return true if this collection has been synchronized
     */
    default boolean isSync()
	{
    	return false;
	}
    
    /**
     * Default implementation for checking if mutations are allowed.
     * Implementations should call this before any mutating operation.
     * 
     * @throws UnsupportedOperationException if the collection is frozen
     */
    default void checkMutable() 
    {
        if (isFrozen())
            throw new UnsupportedOperationException("Collection is frozen and cannot be modified");
    }
    
    /**
     * Default implementation for checking if collection growth is allowed.
     * Implementations should call this before any growth operation.
     * 
     * @throws UnsupportedOperationException if the collection is fixed
     */
    default void checkGrowable() 
    {
        if (isFixed())
            throw new UnsupportedOperationException("Collection is fixed and cannot be grown");
    }
}