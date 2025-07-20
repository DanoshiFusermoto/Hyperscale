package org.radix.hyperscale.database.vamos;

import org.radix.hyperscale.crypto.SipHash24;
import org.radix.hyperscale.utils.Ints;
import org.radix.hyperscale.utils.Longs;

import com.sleepycat.je.DatabaseEntry;

final class InternalKey 
{
	private static final InternalKey[][] cache = new InternalKey[8192][16];
	private static final InternalKey cache(final int databaseID, final byte[] key)
	{
		final long hash = hash(databaseID, key);
		return cache(databaseID, hash);
	}
	
	private static final InternalKey cache(final int databaseID, final long hash)
	{
		final int bucketIndex = (int) Math.abs((hash % cache.length));
		final InternalKey[] bucket = cache[bucketIndex];
		synchronized(bucket)
		{
			final int itemIndex = (int) Math.abs((hash % bucket.length));
			InternalKey cacheKey = bucket[itemIndex];
			if (cacheKey != null)
			{
				if (cacheKey.databaseID == databaseID && cacheKey.value == hash)
					return cacheKey;
			}
			
			cacheKey = new InternalKey(databaseID, hash);
			bucket[itemIndex] = cacheKey;
			return cacheKey;
		}
	}
	
	public final static InternalKey from(final Database database, final DatabaseEntry key)
	{
		return cache(database.getID(), key.getData());
	}

	public final static InternalKey from(final int databaseID, final byte[] key)
	{
		return cache(databaseID, key);
	}

	final static InternalKey from(final int databaseID, final long value)
	{
		return cache(databaseID, value);
	}

	final static InternalKey from(final byte[] bytes, int offset)
	{
		int databaseID = Ints.fromByteArray(bytes, offset);
		long value = Longs.fromByteArray(bytes, offset+Integer.BYTES);
		return cache(databaseID, value);
	}

	private static long hash(final int databaseID, final byte[] key)
	{
		return SipHash24.hash24(databaseID, 0, key);
	}
	
	static final int BYTES = Integer.BYTES + Long.BYTES;
	
	private final int	databaseID;
	private final long 	value;
	private final int 	hashCode;
	
	private InternalKey(final int databaseID, final long value)
	{
		this.databaseID = databaseID;
		this.value = value;
		
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (this.value ^ (this.value >>> 32));
		result = prime * result + this.databaseID;
		this.hashCode = murmur3Hash(result);
	}
	
    private int murmur3Hash(final int input)
    {
        int hash = input;
        hash ^= hash >>> 16;
        hash *= 0x85ebca6b;
        hash ^= hash >>> 13;
        hash *= 0xc2b2ae35;
        hash ^= hash >>> 16;
        return hash;
    }
	
	long value()
	{
		return this.value;
	}
	
	int getDatabaseID()
	{
		return this.databaseID;
	}
	
	@Override
	public int hashCode() 
	{
		return this.hashCode;
	}

	@Override
	public boolean equals(final Object obj) 
	{
		if (this == obj)
			return true;
		
		if (obj == null)
			return false;
		
		if (obj instanceof InternalKey other)
		{
			if (this.databaseID != other.databaseID)
				return false;
			
			if (this.value != other.value)
				return false;
			
			return true;
		}
		
		return false;
	}

	@Override
	public String toString() 
	{
		return "[dbid="+ this.databaseID + " value=" + this.value + "]";
	}
}
