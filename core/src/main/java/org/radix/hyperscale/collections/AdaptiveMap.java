package org.radix.hyperscale.collections;

import java.util.Map;

/**
 * A map that can transition from mutable to immutable state, unsynchronised to synchronised, and 
 * a number of other useful properties for improved collection utility.
 */
public interface AdaptiveMap<K, E> extends Map<K, E> 
{
    /**
     * Freezes this map, making it immutable.
     * This is a one-way operation that cannot be reversed.
     * 
     * @return this map for method chaining
     * @throws IllegalStateException if already frozen (optional)
     */
	AdaptiveMap<K, E> freeze();
    
    /**
     * @return true if this map has been frozen and is immutable
     */
    boolean isFrozen();
    
    /**
     * Fixes the capacity of this map.  Maps which have 
     * been fixed will throw an exception rather than growing to accommodate 
     * new items.
     * This is a one-way operation that cannot be reversed.
     * 
     * @return this map for method chaining
     * @throws IllegalStateException if already frozen (optional)
     */
    AdaptiveMap<K, E> fixed();
    
    /**
     * @return true if this map has a fixed capacity
     */
    boolean isFixed();

    /**
     * Synchronises this map, making all future accesses synchronised.
     * This is a one-way operation that cannot be reversed.
     * 
     * @return this map for method chaining
     * @throws IllegalStateException if already synchronised (optional)
     */
    AdaptiveMap<K, E> synchronised();
    
    /**
     * @return true if this collection has been synchronised
     */
    boolean isSynchronised();

    
    /**
     * Default implementation for checking if mutations are allowed.
     * Implementations should call this before any mutating operation.
     * 
     * @throws UnsupportedOperationException if the map is frozen
     */
    default void checkMutable() 
    {
        if (isFrozen())
            throw new UnsupportedOperationException("Map is frozen and cannot be modified");
    }
}