package org.radix.hyperscale.database.vamos;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import org.radix.hyperscale.collections.PoolBorrower;
import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;

class IndexNode implements PoolBorrower
{
    private static final Logger vamosLog = Logging.getLogger("vamos");
    private static final AtomicInteger incrementer = new AtomicInteger(1);
    private static final ThreadLocal<ByteBuffer> byteBuffer = ThreadLocal.withInitial(() -> ByteBuffer.allocate(65535));

    private final Environment environment;
    private final int 	instance;
    private final short capacity;
    private final short length;
    private final int[] keys;
    private final Object[] entries;

    private volatile IndexNodeID id;
    private transient long modifiedAt = -1;
    private transient int modifications = 0;
    private transient int size = 0;

    IndexNode(final Environment environment)
    {
		Objects.requireNonNull(environment, "Environment is null");

        this.environment = environment;
        this.instance = IndexNode.incrementer.getAndIncrement();
        this.length = (short) environment.getConfig().getIndexNodeSize();
        this.capacity = (short) (this.length / (Integer.BYTES + IndexItem.BYTES));

        this.keys = new int[this.capacity];
        this.entries = new Object[this.capacity];
        Arrays.fill(this.keys, -1);
    }
    
    void reset(final ByteBuffer buffer) throws IOException
    {
        reset(IndexNodeID.from(buffer.getInt()));

        if (this.length != buffer.getShort())
        	throw new IOException("Length mismatch");
        
        if (this.capacity != buffer.getShort())
        	throw new IOException("Capacity mismatch");

        this.size = buffer.getShort();
        Arrays.fill(this.keys, -1);
        Arrays.fill(this.entries, null);

        for (short i = 0; i < this.size; i++) 
        {
            int hash = buffer.getInt();
            byte[] entry = this.environment.getIndex().getIndexItemBytesPool().acquire();
            buffer.get(entry);
            load(hash, entry);
        }
    }

    void reset(final IndexNodeID id)
    {
    	this.id = id;
	    this.modifiedAt = -1;
	    this.modifications = 0;
	    this.size = 0;
	    
        Arrays.fill(this.keys, -1);
        Arrays.fill(this.entries, null);
    }

	@Override
	public void release()
	{
		if (this.modifications != 0)
			throw new IllegalStateException("Index node not flushed before release: "+this);
		
		for (int i = 0 ; i < this.entries.length ; i++)
		{
			if (this.entries[i] instanceof Object[] chain)
			{
				for (int c = 0 ; c < chain.length ; c++)
				{
					if (chain[c] instanceof byte[] bytes)
						release(bytes);
				}
			}
			else if (this.entries[i] instanceof byte[] bytes)
				release(bytes);
		}
		
		this.id = null;
	    this.modifiedAt = -1;
	    this.modifications = 0;
	    this.size = 0;
	    
        Arrays.fill(this.keys, -1);
        Arrays.fill(this.entries, null);

        this.environment.getIndex().getIndexNodePool().release(this);
	}
	
	private void release(final byte[] bytes)
	{
		this.environment.getIndex().getIndexItemBytesPool().release(bytes);
	}

	IndexNodeID getID() 
    { 
    	return this.id; 
    }
    
    int length() 
    { 
    	return this.length; 
    }
    
    int capacity() 
    { 
    	return this.capacity; 
    }
    
    int size() 
    { 
    	synchronized (this) 
    	{ 
    		return this.size; 
    	} 
    }
    
    int modifications() 
    { 
    	return this.modifications; 
    }
    
    long modifiedAt() 
    { 
    	return this.modifiedAt; 
    }

    private void modified() 
    {
        synchronized (this) 
        {
        	this. modifications++;
            if (this.modifiedAt == -1) 
            	this.modifiedAt = System.currentTimeMillis();
        }
    }

    IndexItem get(final InternalKey key) throws IOException 
    {
        synchronized (this) 
        {
            final int slot = slot(key.hashCode());
            if (this.keys[slot] == -1) 
            	return null;

            return retrieveItem(key, slot);
        }
    }

    IndexItem getOrDefault(final InternalKey key, final IndexItem _default) throws IOException 
    {
        final IndexItem result = get(key);
        return result != null ? result : _default;
    }

    IndexItem delete(final InternalKey key) throws IOException 
    {
        synchronized (this) 
        {
            final int slot = slot(key.hashCode());
            if (this.keys[slot] == -1) 
            	return null;

            final IndexItem removed = removeItem(key, slot);
            if (removed != null) 
            {
                modified();

                if (isSlotVoid(slot))
            	{
            		this.keys[slot] = -1;
            		this.entries[slot] = null;
            	}
            	
            	this.size--;
            }
            
            return removed;
        }
    }

    void put(final IndexItem item) throws IOException 
    {
        synchronized (this) 
        {
            if (this.size == this.capacity) throw new IndexNodeCapacityException(this);

            if (this.size == (this.capacity * 3 / 4)) vamosLog.warn("IndexNodeID "+this.id.value()+" is at 75% of capacity");
            if (this.size == (this.capacity * 9 / 10)) vamosLog.warn("IndexNodeID "+this.id.value()+" is at 90% of capacity");

            modified();

            final int slot = slot(item.getKey().hashCode());
            if (mergeItem(item, slot) == true)
            {
            	this.size++;
            	if (vamosLog.hasLevel(Logging.DEBUG))
            		vamosLog.debug("Inserted index item: "+item+" node="+this);
            }
            else if (vamosLog.hasLevel(Logging.DEBUG))
            	vamosLog.debug("Updated index item: "+item+" node="+this);
        }
    }

    void flushed() 
    {
        synchronized (this) 
        {
        	this.modifications = 0;
        	this.modifiedAt = -1;
        }
    }

    byte[] toByteArray() throws IOException 
    {
        ByteBuffer buffer = byteBuffer.get();
        buffer.clear();
        
        write(buffer, false);
        buffer.flip();
        
        byte[] bytes = new byte[buffer.limit()];
        buffer.get(bytes);
        return bytes;
    }

    private int slot(final int hash) 
    {
        return Math.abs(hash % this.capacity);
    }
    
    private boolean isSlotVoid(final int slot)
    {
    	if (this.keys[slot] == -1)
    		return true;
    	
    	// Entry should never be null if key is set; indicates internal inconsistency
    	if (this.entries[slot] == null)
    		throw new IllegalStateException("Entry is null for slot "+slot);
    	
    	final Object entry = this.entries[slot];
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

    private IndexItem retrieveItem(final InternalKey key, final int slot) throws IOException 
    {
    	final Object entry = this.entries[slot];
        if (entry instanceof byte[] bytes) 
        {
            final IndexItem indexItem = IndexItem.from(bytes);
            this.entries[slot] = indexItem;
            release(bytes);
            return key.equals(indexItem.getKey()) ? indexItem : null;
        } 
        else if (entry instanceof IndexItem indexItem) 
        {
            return key.equals(indexItem.getKey()) ? indexItem : null;
        } 
        else if (entry instanceof Object[] bucket) 
        {
        	for (int i = 0 ; i < bucket.length ; i++)
            {
                if (bucket[i] instanceof byte[] bytes) 
                {
                    final IndexItem indexItem = IndexItem.from(bytes);
                    bucket[i] = indexItem;
                    release(bytes);
                    if (key.equals(indexItem.getKey())) 
                    	return indexItem;
                } 
                else if (bucket[i] instanceof IndexItem indexItem) 
                {
                    if (key.equals(indexItem.getKey())) 
                    	return indexItem;
                }
            }
        }
        return null;
    }

    private IndexItem removeItem(final InternalKey key, final int slot) throws IOException 
    {
    	final Object entry = this.entries[slot];
        if (entry instanceof byte[] bytes) 
        {
            IndexItem indexItem = IndexItem.from(bytes);
            this.entries[slot] = indexItem;
            release(bytes);
            return key.equals(indexItem.getKey()) ? indexItem : null;
        } 
        else if (entry instanceof IndexItem indexItem) 
        {
            return key.equals(indexItem.getKey()) ? indexItem : null;
        } 
        else if (entry instanceof Object[] bucket) 
        {
            for (int i = 0; i < bucket.length; i++) 
            {
                if (bucket[i] instanceof byte[] bytes) 
                {
                    IndexItem indexItem = IndexItem.from(bytes);
                    bucket[i] = indexItem;
                    release(bytes);
                    if (key.equals(indexItem.getKey())) 
                    {
                        bucket[i] = null;
                        return indexItem;
                    }
                } 
                else if (bucket[i] instanceof IndexItem indexItem) 
                {
                    if (key.equals(indexItem.getKey())) 
                    {
                        bucket[i] = null;
                        return indexItem;
                    }
                }
            }
        }
        
        return null;
    }

    private boolean mergeItem(final IndexItem item, final int slot) throws IOException 
    {
    	final Object entry = this.entries[slot];
    	if (entry == null)
    	{
    		this.keys[slot] = item.getKey().hashCode();
    		this.entries[slot] = item;
    		return true;
    	}
    	else if (entry instanceof IndexItem existing) 
        {
            if (item.getKey().equals(existing.getKey())) 
            {
                existing.update(item);
                return false;
            } 
            else 
            {
            	this.entries[slot] = new Object[]{existing, item};
            	return true;
            }
        } 
        else if (entry instanceof byte[] bytes) 
        {
        	final IndexItem existing = IndexItem.from(bytes);
            this.entries[slot] = existing;
            release(bytes);
            if (item.getKey().equals(existing.getKey())) 
            {
                existing.update(item);
                return false;
            } 
            else 
            {
            	this.entries[slot] = new Object[]{existing, item};
            	return true;
            }
        } 
        else if (entry instanceof Object[] bucket) 
        {
            for (int i = 0; i < bucket.length; i++) 
            {
                if (bucket[i] instanceof IndexItem existing) 
                {
                	if (item.getKey().equals(existing.getKey()))
          			{
                		existing.update(item);
                		return false;
          			}
                } 
                else if (bucket[i] instanceof byte[] bytes) 
                {
                	final IndexItem existing = IndexItem.from(bytes);
                    bucket[i] = existing;
                    release(bytes);
                    if (item.getKey().equals(existing.getKey())) 
                    {
                    	existing.update(item);
                        return false;
                    }
                }
            }
            
            for (int i = 0; i < bucket.length; i++) 
            {
                if (bucket[i] == null) 
                {
                    bucket[i] = item;
                    return true;
                }
            }
            
            final Object[] expanded = Arrays.copyOf(bucket, bucket.length * 2);
            expanded[bucket.length] = item;
            this.entries[slot] = expanded;
            return true;
        }
        
        throw new IllegalArgumentException("Unsupported entry type: " + entry.getClass());
    }

    private void load(final int hash, final byte[] entry) 
    {
        final int slot = slot(hash);
        if (this.keys[slot] == -1) 
        {
        	this.keys[slot] = hash;
        	this.entries[slot] = entry;
            return;
        }

        if (this.entries[slot] instanceof Object[] bucket) 
        {
            for (int i = 0; i < bucket.length; i++) 
            {
                if (bucket[i] == null) 
                {
                    bucket[i] = entry;
                    return;
                }
            }
            
            final Object[] expanded = Arrays.copyOf(bucket, bucket.length * 2);
            expanded[bucket.length] = entry;
            this.entries[slot] = expanded;
        }
        else if (this.entries[slot] instanceof byte[] bytes) 
        	this.entries[slot] = new Object[]{bytes, entry};
    }

    void write(final ByteBuffer output) throws IOException 
    {
        write(output, true);
    }

    private void write(final ByteBuffer output, final boolean padding) throws IOException 
    {
        int startPos = output.position();

        output.putInt(this.id.value());
        output.putShort(this.length);
        output.putShort(this.capacity);
        synchronized (this) 
        {
            output.putShort((short) this.size);
            for (int i = 0; i < this.entries.length; i++) 
            {
                if (this.keys[i] != -1) 
                    writeEntry(this.keys[i], this.entries[i], output);
            }
        }
        
        if (padding) 
        	output.position(startPos + this.length);
    }

    private void writeEntry(final int hash, final Object entry, final ByteBuffer output) throws IOException 
    {
        if (entry instanceof Object[] bucket) 
        {
            for (Object e : bucket) 
            {
                if (e != null) 
                	writeEntry(hash, e, output);
            }
        } 
        else if (entry instanceof byte[] bytes) 
        {
            output.putInt(hash);
            output.put(bytes);
        } 
        else if (entry instanceof IndexItem indexItem) 
        {
            output.putInt(hash);
            indexItem.write(output);
        } 
        else 
            throw new IllegalArgumentException("Unsupported entry type: " + entry.getClass());
    }

    @Override
    public String toString() 
    {
        return "[inst="+this.instance+" id="+this.id.value()+" size="+this.length+" capacity="+this.capacity+" items="+this.size+" mods="+this.modifications+"]";
    }
}
