package org.radix.hyperscale.collections;

import java.util.AbstractMap;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.radix.hyperscale.utils.Numbers;

public class MappedBlockingQueue<K, V> 
{
	private final Map<K, V> map;
	private final Deque<K> keys;
	private final int capacity;

	/*
     * Concurrency control uses the classic two-condition algorithm
     * found in any textbook.
     */

    /** Main lock guarding all access */
    final ReentrantReadWriteLock lock;
    /** Condition for waiting takes */
    private final Condition notEmpty;
    /** Condition for waiting puts */
    private final Condition notFull;

	private volatile int count;
	
	public MappedBlockingQueue(int capacity) 
	{
		Numbers.isZero(capacity, "Capacity is zero");
		
		this.map = new HashMap<K, V>(capacity);
		this.keys = new ArrayDeque<K>(capacity);
		this.lock = new ReentrantReadWriteLock(true);
		this.notEmpty = this.lock.writeLock().newCondition();
		this.notFull =  this.lock.writeLock().newCondition();
		this.count = 0;
		this.capacity = capacity;
	}

	public int capacity() 
	{
		return this.capacity;
	}
	
	/**
     * Atomically removes all of the elements from this queue.
     * The queue will be empty after this call returns.
     */
    public void clear() 
    {
        this.lock.writeLock().lock();
        try 
        {
        	this.map.clear();
        	this.keys.clear();
        	this.count = 0;
            this.notFull.signalAll();
        } 
        finally 
        {
            this.lock.writeLock().unlock();
        }
    }

    public boolean isEmpty() 
    {
        return this.count == 0;
    }

    public int size() 
    {
        return this.count;
    }
	
    public int drainTo(final Collection<? super V> collection)
    {
        Objects.requireNonNull(collection, "Collection to drain to is null");
        if (collection == this)
            throw new IllegalArgumentException("Can not drain to self");
        
        this.lock.writeLock().lock();
        try
        {
            int drained = this.count;
            collection.addAll(this.map.values());
            clear();
            return drained;
        }
        finally
        {
            this.lock.writeLock().unlock();
        }
    }

    public int drainTo(final Collection<? super V> collection, final int maxElements)
    {
        Objects.requireNonNull(collection, "Collection to drain to is null");
        if (collection == this)
            throw new IllegalArgumentException("Can not drain to self");
        
        Numbers.isZero(maxElements, "Max elements is zero");

        this.lock.writeLock().lock();
        try
        {
            int drained = Math.min(maxElements, this.count);
            final Iterator<K> keyIterator = this.keys.iterator();
            for (int i = 0; i < drained; i++)
            {
            	final K key = keyIterator.next();
                keyIterator.remove();
                final V value = this.map.remove(key);
                collection.add(value);
            }

            this.count -= drained;
            this.notFull.signalAll();
            return drained;
        }
        finally
        {
            this.lock.writeLock().unlock();
        }
    }
    
	public int drainTo(final Map<? super K, ? super V> collection)
	{
		Objects.requireNonNull(collection, "Map to drain to is null");
		if (collection == this)
            throw new IllegalArgumentException("Can not drain to self");
		
        this.lock.writeLock().lock();
		try
		{
			int drained = 0;

			final Iterator<K> keyIterator = this.keys.iterator();
			while(keyIterator.hasNext())
			{
				final K key = keyIterator.next();
				keyIterator.remove();

				final V value = this.map.remove(key);
				if (value == null)
					throw new IllegalStateException("Key "+key+" does not have matching value");

				collection.put(key, value);
				drained++;
			}
	
			if (drained > 0)
			{
				this.count -= drained;
				this.notFull.signalAll();
			}

			return drained;
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}

	public int drainTo(final Map<? super K, ? super V> collection, final int maxElements)
	{
		Objects.requireNonNull(collection, "Map to drain to is null");
		if (collection == this)
            throw new IllegalArgumentException("Can not drain to self");
		
		Numbers.isZero(maxElements, "Max elements is zero");

		this.lock.writeLock().lock();
		try
		{
			int drained = 0;
			final Iterator<K> keyIterator = this.keys.iterator();
			while(drained < maxElements && keyIterator.hasNext())
			{
				final K key = keyIterator.next();
				keyIterator.remove();

				final V value = this.map.remove(key);
				if (value == null)
					throw new IllegalStateException("Key "+key+" does not have matching value");

				collection.put(key, value);
				drained++;
			}

			if (drained > 0)
			{
				this.count -= drained;
				this.notFull.signalAll();
			}

			return drained;
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}

	public void put(final K key, final V value)
	{
		Objects.requireNonNull(key, "Key to put is null");
		Objects.requireNonNull(value, "Value to put is null");
		
		this.lock.writeLock().lock();
		try
		{
			while(this.count >= this.capacity)
			{
				if (this.count > this.capacity)
					throw new IllegalStateException("Count "+this.count+" should never be greater than capacity "+this.capacity);
				
				this.notFull.awaitUninterruptibly();
			}
			
			if (this.map.put(key, value) == null)
            {
                this.keys.add(key);
                this.count++;
                this.notEmpty.signal();
            }
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}
	
	public V putIfAbsent(final K key, final V value)
	{
		Objects.requireNonNull(key, "Key to put is null");
		Objects.requireNonNull(value, "Value to put is null");
		
		this.lock.writeLock().lock();
		try
		{
			final V current = this.map.get(key);
			if (current != null)
				return current;
			
			while(this.count >= this.capacity)
			{
				if (this.count > this.capacity)
					throw new IllegalStateException("Count "+this.count+" should never be greater than capacity "+this.capacity);
				
				this.notFull.awaitUninterruptibly();
			}
			
			if (this.map.put(key, value) == null)
			{
				this.keys.add(key);
				this.count++;
				this.notEmpty.signal();
				return null;
			}
			
			throw new IllegalStateException("Expected key "+key+" not to be assigned");
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}

	public Collection<K> putAll(final Map<K, V> values)
	{
		Objects.requireNonNull(values, "Map to put is null");
		if (values.isEmpty())
			return Collections.emptyList();

		for (final Entry<K, V> entry : values.entrySet())
		{
			Objects.requireNonNull(entry.getKey(), "Key to put is null");
			Objects.requireNonNull(entry.getValue(), "Value to put is null");
		}

		this.lock.writeLock().lock();
		try
		{
			final AdaptiveHashSet<K> puts = new AdaptiveHashSet<K>(values.size());
			for (final Entry<K, V> entry : values.entrySet())
			{
				while(this.count >= this.capacity)
				{
					if (this.count > this.capacity)
						throw new IllegalStateException("Count "+this.count+" should never be greater than capacity "+this.capacity);
					
					this.notFull.awaitUninterruptibly();
				}

				if (this.map.put(entry.getKey(), entry.getValue()) == null)
				{
					this.keys.add(entry.getKey());
					this.count ++;
				}
				
				puts.add(entry.getKey());
			}

			if (puts.isEmpty() == false)
				this.notEmpty.signalAll();

			return puts.freeze();
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}
	
	public Collection<V> putAll(Collection<V> values, final Function<V, K> mapping) 
	{
		Objects.requireNonNull(values, "Values to put is null");
		Objects.requireNonNull(mapping, "Mapping function is null");
		if (values.isEmpty())
			return Collections.emptyList();
		
		for (final V value : values)
			Objects.requireNonNull(value, "Value is null");

		this.lock.writeLock().lock();
		try
		{
			final AdaptiveHashSet<V> puts = new AdaptiveHashSet<V>(values.size());
			for (final V value : values)
			{
				while(this.count >= this.capacity)
				{
					if (this.count > this.capacity)
						throw new IllegalStateException("Count "+this.count+" should never be greater than capacity "+this.capacity);
					
					this.notFull.awaitUninterruptibly();
				}

				final K key = mapping.apply(value);
				if (this.map.put(key, value) == null)
				{
					this.keys.add(key);
					this.count ++;
				}

				puts.add(value);
			}

			if (puts.isEmpty() == false)
				this.notEmpty.signalAll();

			return puts.freeze();
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}

	public boolean offer(final K key, final V value) 
	{
		return offerInternal(key, value, false);
	}

	public boolean offer(final K key, final V value, final long timeout, final TimeUnit unit) throws InterruptedException
	{
		return offerInternal(key, value, false, timeout, unit);
	}

	public boolean offerFirst(final K key, final V value) 
	{
		return offerInternal(key, value, true);
	}

	public boolean offerFirst(final K key, final V value, final long timeout, final TimeUnit unit) throws InterruptedException
	{
		return offerInternal(key, value, true, timeout, unit);
	}
	
	public boolean offerLast(final K key, final V value) 
	{
		return offerInternal(key, value, false);
	}

	public boolean offerLast(final K key, final V value, final long timeout, final TimeUnit unit) throws InterruptedException
	{
		return offerInternal(key, value, false, timeout, unit);
	}

	private boolean offerInternal(final K key, final V value, boolean first) 
	{
		Objects.requireNonNull(key, "Key to offer is null");
		Objects.requireNonNull(value, "Value to offer is null");

		this.lock.writeLock().lock();
		try
		{
			if (this.count > this.capacity)
				throw new IllegalStateException("Count "+this.count+" should never be greater than capacity "+this.capacity);
			
			if (this.count == this.capacity)
				return false;
			
			if (this.map.put(key, value) == null)
			{
				if (first)
					this.keys.addFirst(key);
				else
					this.keys.addLast(key);
				
				this.count++;
				this.notEmpty.signal();
			}
			
			return true;
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}

	private boolean offerInternal(final K key, final V value, final boolean first, final long timeout, final TimeUnit unit) throws InterruptedException
	{
		Objects.requireNonNull(key, "Key to offer is null");
		Objects.requireNonNull(value, "Value to offer is null");
		Objects.requireNonNull(unit, "Time unit is null");
		Numbers.lessThan(timeout, 1, "Time out is less than 1");
		
		long nanos = unit.toNanos(timeout);
		
		this.lock.writeLock().lockInterruptibly();
		try
		{
			while(this.count >= this.capacity)
			{
				if (this.count > this.capacity)
					throw new IllegalStateException("Count "+this.count+" should never be greater than capacity "+this.capacity);
				
                if (nanos <= 0)
                    return false;
                
                nanos = this.notFull.awaitNanos(nanos);
            }
			
			if (this.map.put(key, value) == null)
			{
				if (first)
					this.keys.addFirst(key);
				else
					this.keys.addLast(key);

				this.count++;
				this.notEmpty.signal();
			}
			
			return true;
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}


	public Entry<K,V> peek() 
    {
	    this.lock.readLock().lock();
	    try 
	    {
	    	if (this.count == 0)
	    		return null;
		        
	    	final K key = this.keys.getFirst();
		    if (key == null)
		    	return null;
		        
		    final V value = this.map.get(key);
		    if (value == null)
		    	throw new IllegalStateException("Key " + key + " does not have matching value");
		            
		    return new AbstractMap.SimpleEntry<>(key, value);
	    } 
	    finally 
	    {
	    	this.lock.readLock().unlock();
	    }
    }
	
	public Entry<K,V> peek(final long timeout, final TimeUnit unit) throws InterruptedException
	{
		Objects.requireNonNull(unit, "Time unit is null");
		Numbers.lessThan(timeout, 1, "Time out is less than 1");

        long nanos = unit.toNanos(timeout);
		
		// Need writelock as using writelock.conditions
		this.lock.writeLock().lockInterruptibly();
		try
		{
			while (this.count == 0) 
			{
                if (nanos <= 0)
                    return null;

                nanos = this.notEmpty.awaitNanos(nanos);
            }
			
			final K key = this.keys.getFirst();
			if (key == null)
				return null;
			
			final V value = this.map.get(key);
			if (value == null)
				throw new IllegalStateException("Key "+key+" does not have matching value");
				
			return new AbstractMap.SimpleEntry<K, V>(key, value);
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}

	public Entry<K,V> poll()
    {
        this.lock.writeLock().lock();
        try
        {
            if (this.count == 0)
                return null;
            
            final K key = this.keys.removeFirst();
            if (key == null)
                throw new IllegalStateException("Expected key as count is positive");

            final V value = this.map.remove(key);
            if (value == null)
                throw new IllegalStateException("Key "+key+" does not have matching value");

            this.count--;
            this.notFull.signal();
            return new AbstractMap.SimpleEntry<K,V>(key, value);
        }
        finally
        {
            this.lock.writeLock().unlock();
        }
    }
	public Entry<K,V> poll(final long timeout, final TimeUnit unit) throws InterruptedException
	{
		Objects.requireNonNull(unit, "Time unit is null");
		Numbers.lessThan(timeout, 1, "Time out is less than 1");

		long nanos = unit.toNanos(timeout);
		this.lock.writeLock().lockInterruptibly();
		try
		{
			while (this.count == 0) 
			{
                if (nanos <= 0)
                    return null;

                nanos = this.notEmpty.awaitNanos(nanos);
            }
			
			final K key = this.keys.removeFirst();
			if (key == null)
				throw new IllegalStateException("Expected key as count is positive");

			final V value = this.map.remove(key);
			if (value == null)
				throw new IllegalStateException("Key "+key+" does not have matching value");
		
			this.count--;
			this.notFull.signal();
			return new AbstractMap.SimpleEntry<K,V>(key, value);
		}
		catch (Exception ex)
		{
			throw ex;
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}

	public void forEach(final BiConsumer<? super K, ? super V> action)
	{
		Objects.requireNonNull(action, "Biconsumer foreach is null");
		
		this.lock.readLock().lock();
		try
		{
			for (final K key : this.keys)
			{
				final V value = this.map.get(key);
				if (value == null)
					throw new IllegalStateException("Key "+key+" integrity failure");
				
				action.accept(key, value);
			}
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}

	public boolean contains(final K key)
	{
		Objects.requireNonNull(key, "Key to test contains is null");
		
		this.lock.readLock().lock();
		try			
		{
			return this.map.containsKey(key);
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}
	
	public Set<K> contains(final Collection<K> keys)
	{
		Objects.requireNonNull(keys, "Keys to test contains is null");
		
		final AdaptiveHashSet<K> contains = new AdaptiveHashSet<K>(keys.size());
		this.lock.readLock().lock();
		try			
		{
			for (final K key : keys)
			{
				if (this.map.containsKey(key))
					contains.add(key);
			}
		}
		finally
		{
			this.lock.readLock().unlock();
		}

		return contains.freeze();
	}

	public Set<V> contains(final Collection<V> items, Function<V,K> keyFunction)
	{
		Objects.requireNonNull(items, "Items to test contains is null");
		
		final Map<K, V> map = new HashMap<K, V>();
		for (final V item : items)
			map.put(keyFunction.apply(item), item);
		
		final AdaptiveHashSet<V> contains = new AdaptiveHashSet<V>(map.size());
		this.lock.readLock().lock();
		try			
		{
			for (final Entry<K, V> mapping : map.entrySet())
			{
				if (this.map.containsKey(mapping.getKey()))
					contains.add(mapping.getValue());
			}
		}
		finally
		{
			this.lock.readLock().unlock();
		}

		return contains.freeze();
	}

	public V get(final K key)
	{
		Objects.requireNonNull(key, "Key to get is null");

		this.lock.readLock().lock();
		try			
		{
			return this.map.get(key);
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}

	public List<V> getMany(final int limit)
	{
		return getMany(limit, null);
	}

	public List<V> getMany(final int limit, final Comparator<V> comparator)
	{
		final List<V> target = new ArrayList<V>(limit);
		getMany(target, limit, comparator);
		return target;
	}
	
	public void getMany(final List<V> target, final int limit)
	{
		getMany(target, limit, null);
	}
	
	public void getMany(final List<V> target, final int limit, final Comparator<V> comparator)
	{
		Objects.requireNonNull(target, "Target list is null");
		Numbers.isZero(limit, "Limit is zero");
		
		this.lock.readLock().lock();
		try			
		{
			for (final K key : this.keys)
			{
				final V value = this.map.get(key);
				if (value == null)
					throw new IllegalStateException("Key "+key+" integrity failure");
				
				target.add(value);
				
				if (target.size() == limit)
					break;
			}
			
			if (comparator != null)
				target.sort(comparator);
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}

	public List<V> getAll()
	{
		this.lock.readLock().lock();
		try			
		{
			final List<V> list = new ArrayList<V>(this.keys.size());
			for (final K key : this.keys)
			{
				final V value = this.map.get(key);
				if (value == null)
					throw new IllegalStateException("Key "+key+" integrity failure");
				
				list.add(value);
			}

			return Collections.unmodifiableList(list);
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}

	public List<V> getAll(final Comparator<V> comparator)
	{
		this.lock.readLock().lock();
		try			
		{
			final List<V> list = new ArrayList<V>(this.keys.size());
			for (final K key : this.keys)
			{
				final V value = this.map.get(key);
				if (value == null)
					throw new IllegalStateException("Key "+key+" integrity failure");
				
				list.add(value);
			}
			
			list.sort(comparator);

			return Collections.unmodifiableList(list);
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}

	public Collection<V> getAll(final Collection<K> keys)
	{
		return getAll(keys, null);
	}
	
	public Collection<V> getAll(final Collection<K> keys, final BiConsumer<K, V> consumer)
	{
		Objects.requireNonNull(keys, "Keys to get is null");

		final Map<K,V> results = new HashMap<K,V>(keys.size()+1, 1.0f);
		this.lock.readLock().lock();
		try			
		{
			for (final K key : keys)
			{
				final V value = this.map.get(key);
				if (value == null)
					continue;
				
				results.put(key, value);
			}
		}
		finally
		{
			this.lock.readLock().unlock();
		}
		
		if (consumer != null)
			results.forEach(consumer);
		
		return results.values();
	}

	public V remove(final K key) 
    {
        Objects.requireNonNull(key, "Key to remove is null");
        
        this.lock.writeLock().lock();
        try
        {
        	final V value = this.map.remove(key);
            if (value == null)
                return value;
            
            if (this.keys.remove(key) == false)
                throw new IllegalStateException("Key "+key+" integrity failure");
            
            this.count--;
            this.notFull.signal();
            return value;
        }
        finally
        {
            this.lock.writeLock().unlock();
        }
    }
	
	public boolean remove(final K key, final V value)
	{
		Objects.requireNonNull(key, "Key to remove is null");
		Objects.requireNonNull(value, "Value to remove is null");
		
		this.lock.writeLock().lock();
		try
		{
			boolean removed = this.map.remove(key, value);
			if (removed)
			{
				if (this.keys.remove(key) == false)
					throw new IllegalStateException("Key "+key+" integrity failure");

				this.count--;
				this.notFull.signal();
			}
			return removed;
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}

	public Map<K, V> removeAll(final Map<K, V> removals) 
	{
		return removeAll(removals.keySet());
	}

	public Map<K, V> removeAll(final Collection<K> keys) 
	{
		Objects.requireNonNull(keys, "Key collection to remove is null");
		if (keys.isEmpty())
			return Collections.emptyMap();
		
		final Map<K,V> removed = new HashMap<K,V>(keys.size()+1, 1.0f);
		this.lock.writeLock().lock();
		try
		{
			for (final K key : keys)
			{
				final V value = this.map.remove(key);
				if (value != null)
				{
					if (this.keys.remove(key) == false)
						throw new IllegalStateException("Key "+key+" integrity failure");

					removed.put(key, value);
				}
			}

			if (removed.size() > 0)
			{
				this.count -= removed.size();
				this.notFull.signalAll();
			}
		}
		finally
		{
			this.lock.writeLock().unlock();
		}

		return removed;
	}
}
