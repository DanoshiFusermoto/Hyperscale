package org.radix.hyperscale.collections;

import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

public class AdaptiveHashSet<E> extends AbstractSet<E> implements AdaptiveCollection<E>, java.io.Serializable
{
	private static final long serialVersionUID = 7449746608855880597L;

	private static final int MINIMUM_SET_SIZE = 2;
	private static final int DEFAULT_SET_SIZE = 8;

    private volatile Object[] elements;
    private volatile int size;
    private volatile boolean frozen;
    private volatile boolean fixed;
    
    protected volatile transient int modCount = 0;

    public AdaptiveHashSet()
    {
    	this(DEFAULT_SET_SIZE);
    }
    
    public AdaptiveHashSet(int capacity)
    {
    	super();
    	
   		this.elements = new Object[Math.max(capacity, MINIMUM_SET_SIZE)];
    	this.size = 0;
    	this.fixed = false;
    	this.frozen = false;
    }
    
    /**
     * Constructs a list containing the elements of the specified
     * collection, in the order they are returned by the collection's
     * iterator.
     *
     * @param c the collection whose elements are to be placed into this list
     * @throws NullPointerException if the specified collection is null
     */
    public AdaptiveHashSet(Collection<? extends E> c) 
    {
    	super();
        
        if (c instanceof AdaptiveArrayList adaptiveArrayList) 
        {
        	this.elements = new Object[Math.max(adaptiveArrayList.size(), MINIMUM_SET_SIZE)];
        	for (int i = 0 ; i < this.elements.length ; i++)
           		add((E) adaptiveArrayList.get(i));
        }
        else if (c instanceof ArrayList arrayList) 
        {
        	this.elements = new Object[Math.max(arrayList.size(), MINIMUM_SET_SIZE)];
        	for (int i = 0 ; i < this.elements.length ; i++)
        		add((E) arrayList.get(i));
        }
        else
        {
        	final Object[] array = c.toArray();
        	this.elements = new Object[Math.max(array.length, MINIMUM_SET_SIZE)];
        	for (int i = 0 ; i < array.length ; i++)
        		add((E) array[i]);
        }
        
    	this.size = c.size();
    	this.frozen = false;
    	this.fixed = false;
    }
    
    /**
     * Constructs a list containing the elements of the specified array.  The array must
     * not contain null values.
     *
     * @param array the array whose elements are to be placed into this list
     * @throws NullPointerException if the specified collection is null
     */
    public AdaptiveHashSet(E ... array) 
    {
    	super();
        
       	this.elements = new Object[Math.max(array.length, MINIMUM_SET_SIZE)];
       	for (int i = 0 ; i < this.elements.length ; i++)
       	{
       		E element = array[i];
       		if (element == null)
       			throw new IllegalArgumentException("Element "+i+" is null");
       		
       		add(array[i]);
       	}

       	this.size = array.length;
    	this.frozen = false;
    	this.fixed = false;
    }

    @Override
    public void clear()
    {
    	checkMutable();
    	this.modCount++;
    	Arrays.fill(this.elements, null);
    	this.size = 0;
    }

    @Override
	public int size()
	{
		return this.size;
	}
    
	/**
     * Appends the specified element to the end of this list.
     *
     * @param e element to be appended to this list
     * @return {@code true} (as specified by {@link Collection#add})
     */
    public boolean add(E element) 
    {
        return add(element, this.size);
    }
    
    private boolean add(E element, int s) 
    {
    	checkMutable();
        if (s == this.elements.length)
        	grow();
        
        final int slot = slot(element.hashCode());
        if (mergeItem(element, slot) == false)
        	return false;
        
       	this.modCount++;
       	this.size = s + 1;
        return true;
    }
    
	@Override
	public boolean contains(Object object)
	{
        final int slot = slot(object.hashCode());
        return containsItem(object, slot);
	}

	@Override
	public boolean remove(Object object)
	{
    	checkMutable();
        final int slot = slot(object.hashCode());
        final E removed = removeItem(object, slot);
        if (removed != null) 
        {
        	if (isSlotVoid(slot))
        		this.elements[slot] = null;
        	
        	this.size--;
            this.modCount++;
            return true;
		}
        
        return false;
	}

	@Override
	public Iterator<E> iterator()
	{
		return new Itr();
	}
	
	@Override
	public Object[] toArray() 
	{
	    final Object[] result = new Object[this.size];
	    int index = 0;

	    for (final Object entry : this.elements) 
	    {
	        if (entry == null) 
	        	continue;

	        if (entry instanceof Object[] bucket) 
	        {
	            for (Object obj : bucket) 
	            {
	                if (obj != null)
	                    result[index++] = obj;
	            }
	        } 
	        else
	            result[index++] = entry;
	    }

	    return result;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T[] toArray(T[] a) 
	{
	    int actualSize = this.size;
	    final T[] result = (a.length >= actualSize) ? a : (T[]) java.lang.reflect.Array.newInstance(a.getClass().getComponentType(), actualSize);

	    int index = 0;
	    for (final Object entry : this.elements) 
	    {
	        if (entry == null) 
	        	continue;

	        if (entry instanceof Object[] bucket) 
	        {
	            for (Object obj : bucket) 
	            {
	                if (obj != null)
	                    result[index++] = (T) obj;
	            }
	        } 
	        else 
	            result[index++] = (T) entry;
	    }

	    if (result.length > actualSize)
	        result[actualSize] = null;

	    return result;
	}

    /**
     * Increases the capacity to ensure that it can hold at least the
     * number of elements specified by the minimum capacity argument.
     *
     * @throws OutOfMemoryError if minCapacity is less than zero
     */
    private void grow() 
    {
    	checkGrowable();
    	Object[] oldElements = this.elements;
   		this.elements = new Object[this.elements.length * 2];
   		for (int i = 0 ; i < oldElements.length ; i++)
   		{
   			final Object entry = oldElements[i];
   			if (entry == null)
   				continue;
   			
   			if (entry instanceof Object[] bucket)
   			{
   				for (int c = 0 ; c < bucket.length ; c++)
   				{
   					if (bucket[c] == null)
   						continue;
   					
   					final int slot = slot(bucket[c].hashCode());
   					mergeItem((E)bucket[c], slot);
   				}
   			}
   			else
   			{
   				final int slot = slot(entry.hashCode());
				mergeItem((E)entry, slot);
   			}
   		}
    }
    
    private int slot(final int hash) 
    {
        return Math.abs(hash % this.elements.length);
    }
    
    private boolean isSlotVoid(int slot)
    {
    	if (this.elements[slot] == null)
    		return true;
    	
    	final Object entry = this.elements[slot];
    	if (entry instanceof Object[] bucket)
    	{
    		for (int i = 0 ; i < bucket.length ; i++)
    		{
    			if (bucket[i] != null)
    				return false;
    		}
    	}
    	
    	return true;
    }

    private boolean containsItem(final Object element, final int slot) 
    {
    	final Object entry = this.elements[slot];
        if (entry instanceof Object[] bucket) 
        {
        	for (int i = 0 ; i < bucket.length ; i++)
            {
        		if (bucket[i] == null)
        			continue;
        		
                if (element.equals(bucket[i])) 
                  	return true;
            }
        }
        
        return element.equals(entry);
    }

    private E removeItem(final Object element, final int slot) 
    {
    	final Object entry = this.elements[slot];
        if (entry instanceof Object[] bucket) 
        {
            for (int i = 0; i < bucket.length; i++) 
            {
            	if (bucket[i] == null)
            		continue;
            	
            	if (element.equals(bucket[i])) 
                {
            		bucket[i] = null;
                    return (E) element;
                }
            }
        }
        else if (element.equals(entry))
        {
        	this.elements[slot] = null;
        	return (E) element;
        }
        
        return null;
    }

    private boolean mergeItem(final E element, final int slot) 
    {
    	final Object entry = this.elements[slot];
        if (entry instanceof Object[] bucket) 
        {
            for (int i = 0; i < bucket.length; i++) 
            {
            	if (bucket[i] == null)
            		continue;
            	
                if (element.equals(bucket[i])) 
                    return false;
            }
            
            for (int i = 0; i < bucket.length; i++) 
            {
                if (bucket[i] == null) 
                {
                    bucket[i] = element;
                    return true;
                }
            }
            
            final Object[] expanded = Arrays.copyOf(bucket, bucket.length * 2);
            expanded[bucket.length] = element;
            this.elements[slot] = expanded;
            return true;
        }
        else if (element.equals(entry) == false)
        {
        	if (entry == null)
            	this.elements[slot] = element;
        	else
        		this.elements[slot] = new Object[]{entry, element};
        	
            return true;
        }
        
        return false;
    }
    
	@Override
	public AdaptiveHashSet<E> freeze()
	{
		this.frozen = true;
		return this;
	}

	@Override
	public boolean isFrozen()
	{
		return this.frozen;
	}
	
	@Override
	public AdaptiveHashSet<E> fixed()
	{
		this.fixed = true;
		return this;
	}

	@Override
	public boolean isFixed()
	{
		return this.fixed;
	}
	
    private class Itr implements Iterator<E> 
    {
        private final int expectedModCount;
        private int index = 0;
        private int bucketIndex = -1;
        private E nextElement = null;
        private E lastReturned = null;

        Itr()
        {
        	this.expectedModCount = AdaptiveHashSet.this.modCount;
            advance();
        }

        private void checkForConcurrentModification() 
        {
            if (AdaptiveHashSet.this.modCount != this.expectedModCount)
                throw new java.util.ConcurrentModificationException();
        }

        private void advance() 
        {
            this.nextElement = null;
            while (this.index < AdaptiveHashSet.this.elements.length) 
            {
                Object entry = AdaptiveHashSet.this.elements[this.index];

                if (entry == null) 
                {
                	this.index++;
                	this.bucketIndex = -1;
                    continue;
                }

                if (entry instanceof Object[] bucket) 
                {
                	this.bucketIndex++;
                    while (this.bucketIndex < bucket.length) 
                    {
                        Object candidate = bucket[this.bucketIndex];
                        if (candidate != null) 
                        {
                        	this.nextElement = (E) candidate;
                            return;
                        }
                        this.bucketIndex++;
                    }
                    this.index++;
                    this.bucketIndex = -1;
                } 
                else 
                {
                	this.nextElement = (E) entry;
                	this.index++;
                	this.bucketIndex = -1;
                    return;
                }
            }
        }

        @Override
        public boolean hasNext() 
        {
            return this.nextElement != null;
        }

        @Override
        public E next() 
        {
            checkForConcurrentModification();
            if (this.nextElement == null)
                throw new java.util.NoSuchElementException();

            this.lastReturned = this.nextElement;
            advance();
            return this.lastReturned;
        }

        @Override
        public void remove() 
        {
            checkForConcurrentModification();
            if (this.lastReturned == null)
                throw new IllegalStateException();
            
            final int slot = slot(this.lastReturned.hashCode());
            final E removed = removeItem(this.lastReturned, slot);
            if (removed != null) 
            {
            	if (isSlotVoid(slot))
            		AdaptiveHashSet.this.elements[slot] = null;
            	
            	AdaptiveHashSet.this.size--;
            }
            this.lastReturned = null;
        }
    }
}
