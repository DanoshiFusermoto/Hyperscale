package org.radix.hyperscale.collections;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.RandomAccess;

public class AdaptiveArrayList<E> extends AbstractList<E> implements RandomAccess, AdaptiveCollection<E>, java.io.Serializable
{
	private static final long serialVersionUID = -7000810147461243037L;
	
	private static final int DEFAULT_ARRAY_SIZE = 8;

    private volatile Object[] elements;
    private volatile int size;
    private volatile boolean frozen;
    private volatile boolean fixed;

    public AdaptiveArrayList()
    {
    	this(DEFAULT_ARRAY_SIZE);
    }
    
    public AdaptiveArrayList(int capacity)
    {
    	super();
    	
   		this.elements = new Object[capacity];
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
    public AdaptiveArrayList(Collection<? extends E> c) 
    {
    	super();
        
        if (c instanceof AdaptiveArrayList adaptiveArrayList) 
        {
        	this.elements = new Object[adaptiveArrayList.size()];
        	for (int i = 0 ; i < this.elements.length ; i++)
        		this.elements[i] = adaptiveArrayList.get(i);
        }
        else if (c instanceof ArrayList arrayList) 
        {
        	this.elements = new Object[arrayList.size()];
        	for (int i = 0 ; i < this.elements.length ; i++)
        		this.elements[i] = arrayList.get(i);
        }
        else
        	this.elements = c.toArray();
        
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
    public AdaptiveArrayList(E ... array) 
    {
    	super();
        
       	this.elements = new Object[array.length];
       	for (int i = 0 ; i < this.elements.length ; i++)
       	{
       		E element = array[i];
       		if (element == null)
       			throw new IllegalArgumentException("Array element "+i+" is null");
       		
       		this.elements[i] = array[i];
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
    public boolean add(E e) 
    {
    	checkMutable();
    	this.modCount++;
        add(e, this.size);
        return true;
    }
    
    private void add(E e, int s) 
    {
        if (s == this.elements.length)
        	grow();
    
        this.elements[s] = e;
        this.size = s + 1;
    }
    
    /**
     * Returns the element at the specified position in this list.
     *
     * @param  index index of the element to return
     * @return the element at the specified position in this list
     * @throws IndexOutOfBoundsException {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
	public E get(int index) 
    {
        Objects.checkIndex(index, size);
        return (E) this.elements[index];
    }
    
    /**
     * Replaces the element at the specified position in this list with
     * the specified element.
     *
     * @param index index of the element to replace
     * @param element element to be stored at the specified position
     * @return the element previously at the specified position
     * @throws IndexOutOfBoundsException {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
	public E set(int index, E element) 
    {
        Objects.checkIndex(index, size);
    	checkMutable();
        
        E oldValue = (E) this.elements[index];
        this.elements[index] = element;
        return oldValue;
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
   		Object[] newElements = Arrays.copyOf(this.elements, this.elements.length * 2);
   		this.elements = newElements;
    		
    }

	@Override
	public AdaptiveArrayList<E> freeze()
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
	public AdaptiveArrayList<E> fixed()
	{
		this.fixed = true;
		return this;
	}

	@Override
	public boolean isFixed()
	{
		return this.fixed;
	}
}
