package org.radix.hyperscale.crypto;

import java.util.Arrays;
import java.util.Objects;

import org.radix.hyperscale.utils.Base58;
import org.radix.hyperscale.utils.Numbers;

import com.google.common.primitives.SignedBytes;

public abstract class Key implements Comparable<Key>, Hashable
{
	private volatile long hashCode = Long.MAX_VALUE;
	private volatile Hash hash;
	private volatile String cachedBase58;
	
	public abstract boolean canSign();

	public abstract boolean canVerify();
	
	public abstract Identity getIdentity();

	public final int length() 
    {
        return toByteArray().length;
    }
	
	public abstract byte[] toByteArray();
	
	@Override
	public final synchronized int hashCode() 
	{
		if (this.hashCode == Long.MAX_VALUE)
			this.hashCode = computeHashCode();
		
		return (int) this.hashCode;
	}
	
	int computeHashCode()
	{
		return Arrays.hashCode(toByteArray());
	}
	
	@Override
	public final boolean equals(Object object) 
	{
		if (object == null)
			return false;
		
		if (object == this)
			return true;

		return computeEqual(object);
	}
	
	boolean computeEqual(Object object)
	{
		if (object instanceof Key key) 
			return Arrays.equals(key.toByteArray(), toByteArray());
		
		return false;
	}

	@Override
    public final synchronized Hash getHash()
	{
    	if (this.hash == null)
    		this.hash = computeHash();
    	
    	return this.hash;
	}
    
    Hash computeHash()
    {
		return Hash.hash(toByteArray());
    }

	@Override
	public final String toString() 
	{
		return toString(-1);
	}

	public final synchronized String toString(int truncateTo) 
	{
		if (truncateTo > -1)
			Numbers.lessThan(truncateTo, 4, "Key truncation must be at least 4");
		
		if (this.cachedBase58 == null)
			this.cachedBase58 = Base58.toBase58(toByteArray());
		
		if (truncateTo >= 4)
		{
			if (this.cachedBase58.length() > truncateTo)
			{
				final StringBuilder truncatedBuilder = new StringBuilder(truncateTo+3);
				int halfTruncate = truncateTo / 2;
				truncatedBuilder.append(this.cachedBase58, 0, halfTruncate);
				truncatedBuilder.append("...");
				truncatedBuilder.append(this.cachedBase58, this.cachedBase58.length()-halfTruncate, this.cachedBase58.length());
				return truncatedBuilder.toString();
			}
		}
		
		return this.cachedBase58;
	}

	@Override
	public final int compareTo(final Key other)
	{
		Objects.requireNonNull(other, "Key for compare is null");
		
		return SignedBytes.lexicographicalComparator().compare(toByteArray(), other.toByteArray());
	}
}
