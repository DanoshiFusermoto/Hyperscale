package org.radix.hyperscale.database.vamos;

import org.radix.hyperscale.utils.Numbers;

final class IndexNodeID 
{
	/*
	 * Basic memory light index node ID cache.  Frequent identical index node IDs are generated. 
	 * Re-using these created instances where possible, even if minimal, reduces load on the garbage collector "significantly".  
	 * 
	 * TODO improvements?
	 */
	private static final int INDEX_NODE_ID_CACHE_SLICES = 1<<14;
	private static final int INDEX_NODE_ID_CACHE_SIZE = 1<<20;
	private static final IndexNodeID[][] cache = new IndexNodeID[INDEX_NODE_ID_CACHE_SLICES][INDEX_NODE_ID_CACHE_SIZE / INDEX_NODE_ID_CACHE_SLICES];
	private static final IndexNodeID compute(int ID)
	{
		final int slice = ID % INDEX_NODE_ID_CACHE_SLICES;
		final int index = ID % (INDEX_NODE_ID_CACHE_SIZE / INDEX_NODE_ID_CACHE_SLICES);
		
		final IndexNodeID[] cacheSlice = cache[slice];
		synchronized(cacheSlice)
		{
			IndexNodeID in = cacheSlice[index];
			if (in == null)
			{
				in = new IndexNodeID(ID);
				cacheSlice[index] = in;
			}
			else
			{
				if (in.ID == ID)
					return in;
				else
				{
					in = new IndexNodeID(ID);
					cacheSlice[index] = in;
				}
			}
			
			return in;
		}
	}
	
	public final static IndexNodeID from(int ID)
	{
		return compute(ID);
	}

	private final int ID;
	private final int hashCode;
	
	private IndexNodeID(int ID)
	{
		Numbers.isNegative(ID, "ID is negative");
		this.ID = ID;

		final int prime = 31;
		int result = 1;
		result = prime * result + this.ID;
		this.hashCode = result;
	}
	
	int value()
	{
		return this.ID;
	}

	@Override
	public int hashCode() 
	{
		return this.hashCode;
	}

	@Override
	public boolean equals(Object obj) 
	{
		if (this == obj)
			return true;
		
		if (obj == null)
			return false;
		
		if (obj instanceof IndexNodeID other) 
		{
			if (this.ID != other.ID)
				return false;
			
			return true;
		}
		
		return false;
	}

	@Override
	public String toString() 
	{
		return "[id=" + this.ID + "]";
	}
}
