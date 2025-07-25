package org.radix.hyperscale.collections;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import org.radix.hyperscale.utils.Numbers;

public abstract class ObjectPool<T>
{
    final static List<WeakReference<ObjectPool<?>>> instances = Collections.synchronizedList(new LinkedList<WeakReference<ObjectPool<?>>>());
    public final static Iterator<WeakReference<ObjectPool<?>>> instances()
    {
    	synchronized(instances)
    	{
    		final List<WeakReference<ObjectPool<?>>> live = new ArrayList<WeakReference<ObjectPool<?>>>(instances.size());
    		final Iterator<WeakReference<ObjectPool<?>>> instanceIterator = instances.iterator();
    		while(instanceIterator.hasNext())
    		{
    			final WeakReference<ObjectPool<?>> instanceRef = instanceIterator.next();
    			final ObjectPool<?> instance = instanceRef.get();
    			if (instance == null)
    			{
    				instanceIterator.remove();
    				continue;
    			}
    			
    			live.add(instanceRef);
    		}
    		
    		return live.iterator();
    	}
    }
    
    static enum PoolReturnStatus 
    {
    	UNKNOWN, RETURNED, STALE, DISCARDED, FULL
    }
    
    private final String label;

    ObjectPool(final String label)
    {
    	Objects.requireNonNull(label, "Label is null");
    	Numbers.inRange(label.length(), 3, 32, "Pool label is invalid length");
    	this.label = label;
    	
    	ObjectPool.instances.add(new WeakReference<ObjectPool<?>>(this));
    }
    
    public String getLabel()
    {
    	return this.label;
    }

    public abstract void clear();
}
