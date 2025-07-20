package org.radix.hyperscale.database.vamos;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;
import org.radix.hyperscale.utils.Numbers;

class IndexNode 
{
    private static final Logger vamosLog = Logging.getLogger("vamos");
    private static final ThreadLocal<ByteBuffer> byteBuffer = ThreadLocal.withInitial(() -> ByteBuffer.allocate(65535));

    private final IndexNodeID id;
    private final short capacity;
    private final short length;
    private final int[] keys;
    private final Object[] entries;

    private transient long modifiedAt = -1;
    private transient int modifications = 0;
    private transient int size = 0;

    IndexNode(final IndexNodeID id, final int length) 
    {
        Objects.requireNonNull(id, "Index node ID is null");
        Numbers.lessThan(length, 1024, "Max index node length is less than 1kb");
        Numbers.greaterThan(length, 65535, "Max index node length is greater than 64kb");

        this.id = id;
        this.length = (short) length;
        this.capacity = (short) (length / (Integer.BYTES + IndexItem.BYTES));

        this.keys = new int[this.capacity];
        this.entries = new Object[this.capacity];
        Arrays.fill(this.keys, -1);
    }

    IndexNode(final ByteBuffer buffer) throws IOException 
    {
        Objects.requireNonNull(buffer, "Index node constructor buffer is null");

        this.id = IndexNodeID.from(buffer.getInt());
        this.length = buffer.getShort();
        this.capacity = buffer.getShort();
        this.size = buffer.getShort();

        this.keys = new int[this.capacity];
        this.entries = new Object[this.capacity];
        Arrays.fill(this.keys, -1);

        for (short i = 0; i < this.size; i++) 
        {
            int hash = buffer.getInt();
            byte[] entry = new byte[IndexItem.BYTES];
            buffer.get(entry);
            load(hash, entry);
        }
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

            final Object entry = this.entries[slot];
            return retrieveItem(key, entry);
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

            final Object entry = this.entries[slot];
            final IndexItem removed = removeItem(key, entry);
            if (removed != null) 
            {
            	if (isKeyVoid(key))
            	{
            		this.keys[slot] = -1;
            		this.entries[slot] = null;
            	}
            	
            	this.size--;
                modified();
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
            if (this.keys[slot] == -1) 
            {
            	this.keys[slot] = item.getKey().hashCode();
            	this.entries[slot] = item;
            	this.size++;
                return;
            }

            this.entries[slot] = mergeItem(item, this.entries[slot]);
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
    
    private boolean isKeyVoid(final InternalKey key)
    {
    	final int slot = slot(key.hashCode());
    	if (this.keys[slot] == -1)
    		return true;
    	
    	// Entry should never be null if key is set; indicates internal inconsistency
    	if (this.entries[slot] == null)
    		throw new IllegalStateException("Entry is null for key "+key.hashCode()+" in slot "+slot);
    	
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

    private IndexItem retrieveItem(final InternalKey key, final Object entry) throws IOException 
    {
        if (entry instanceof byte[] bytes) 
        {
            final IndexItem indexItem = IndexItem.from(bytes);
            return key.equals(indexItem.getKey()) ? indexItem : null;
        } 
        else if (entry instanceof IndexItem indexItem) 
        {
            return key.equals(indexItem.getKey()) ? indexItem : null;
        } 
        else if (entry instanceof Object[] bucket) 
        {
            for (final Object e : bucket) 
            {
                if (e instanceof byte[] b) 
                {
                    IndexItem ii = IndexItem.from(b);
                    if (key.equals(ii.getKey())) 
                    	return ii;
                } 
                else if (e instanceof IndexItem ii) 
                {
                    if (key.equals(ii.getKey())) 
                    	return ii;
                }
            }
        }
        return null;
    }

    private IndexItem removeItem(final InternalKey key, final Object entry) throws IOException 
    {
        if (entry instanceof byte[] bytes) 
        {
            IndexItem indexItem = IndexItem.from(bytes);
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
                if (bucket[i] instanceof byte[] b) 
                {
                    IndexItem indexItem = IndexItem.from(b);
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

    private Object mergeItem(final IndexItem item, final Object entry) throws IOException 
    {
        if (entry instanceof IndexItem existing) 
        {
            if (item.getKey().equals(existing.getKey())) 
            {
                existing.update(item);
                return existing;
            } 
            else 
                return new Object[]{existing, item};
        } 
        else if (entry instanceof byte[] bytes) 
        {
        	final IndexItem existing = IndexItem.from(bytes);
            if (item.getKey().equals(existing.getKey())) 
            {
                existing.update(item);
                return existing;
            } 
            else 
                return new Object[]{existing, item};
        } 
        else if (entry instanceof Object[] bucket) 
        {
            for (int i = 0; i < bucket.length; i++) 
            {
                if (bucket[i] instanceof IndexItem indexItem && item.getKey().equals(indexItem.getKey())) 
                {
                    indexItem.update(item);
                    return bucket;
                } 
                else if (bucket[i] instanceof byte[] bytes) 
                {
                	final IndexItem indexItem = IndexItem.from(bytes);
                    if (item.getKey().equals(indexItem.getKey())) 
                    {
                    	indexItem.update(item);
                        bucket[i] = indexItem;
                        return bucket;
                    }
                }
            }
            
            for (int i = 0; i < bucket.length; i++) 
            {
                if (bucket[i] == null) 
                {
                    bucket[i] = item;
                    return bucket;
                }
            }
            
            final Object[] expanded = Arrays.copyOf(bucket, bucket.length * 2);
            expanded[bucket.length] = item;
            return expanded;
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

    private void writeEntry(final int key, final Object entry, final ByteBuffer output) throws IOException 
    {
        if (entry instanceof Object[] bucket) 
        {
            for (Object e : bucket) 
            {
                if (e != null) writeEntry(key, e, output);
            }
        } 
        else if (entry instanceof byte[] b) 
        {
            output.putInt(key);
            output.put(b);
        } 
        else if (entry instanceof IndexItem ii) 
        {
            output.putInt(key);
            ii.write(output);
        } 
        else 
        {
            throw new IllegalArgumentException("Unsupported entry type: " + entry.getClass());
        }
    }

    @Override
    public String toString() 
    {
        return "[id="+this.id.value()+" size="+this.length+" capacity="+this.capacity+" items="+this.size+"]";
    }

    void write(final ByteBuffer output) throws IOException 
    {
        write(output, true);
    }
}
