package org.radix.hyperscale.ledger;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import org.radix.hyperscale.collections.LRUCacheMap;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.crypto.Hashable;
import org.radix.hyperscale.crypto.Identity;
import org.radix.hyperscale.utils.Numbers;
import org.radix.hyperscale.utils.Strings;

public final class StateAddress implements Comparable<StateAddress>, Hashable
{
	// Contexts are frequently the same, so cache the most popular to save an actual digest computation when generating a hash 
	private static final LRUCacheMap<String, Hash> contextHashCache = new LRUCacheMap<String, Hash>(1<<10);
	
	private static final Hash checkCache(final String context)
	{
		synchronized(contextHashCache)
		{
			// IMPORTANT context should ALWAYS be lower case here!
			Hash hash = contextHashCache.get(context);
			if (hash != null)
				return hash;
			
			hash = Hash.hash(context);
			contextHashCache.put(context, hash);
			return hash;
		}
	}
	
	// Simple fixed-size String cache using array for minimal overhead for context strings
    private static final int CONTEXT_CACHE_SIZE = 64;  // Small size since contexts are likely limited
    private static final String[] stringCache = new String[CONTEXT_CACHE_SIZE];
    private static final byte[][] bytesCache = new byte[CONTEXT_CACHE_SIZE][]; 
    private static int cacheIndex = 0;
    
    private static final String checkCache(final byte[] bytes, int contextOffset, int contextLength)
    {
    	String context = null;
    	
    	// Cache only short contexts
    	if (contextLength > 12)
    		return new String(bytes, contextOffset, contextLength, StandardCharsets.UTF_8);
        
    	synchronized(stringCache)
    	{
	        // Simple array-based cache lookup
	        for (int i = 0; i < CONTEXT_CACHE_SIZE && stringCache[i] != null; i++) 
	        {
	            if (contextLength == bytesCache[i].length)
	            {
	            	boolean equals = true;
	            	for (int b = 0; b < contextLength; b++) 
	            	{
	                    if (bytes[contextOffset + b] != bytesCache[i][b])
	                    {
	                    	equals = false;
	                    	break;
	                    }
	                }
	            	
	            	if (equals == true)
	            	{
	            		context = stringCache[i];
	            		break;
	            	}
	            }
	        }
	        
	        if (context == null) 
	        {
	            context = new String(bytes, contextOffset, contextLength, StandardCharsets.UTF_8);
	            // Cache the new string and its bytes
	            byte[] contextBytes = new byte[contextLength];
	            System.arraycopy(bytes, contextOffset, contextBytes, 0, contextLength);
	            stringCache[cacheIndex] = context;
	            bytesCache[cacheIndex] = contextBytes;
	            cacheIndex = (cacheIndex + 1) % CONTEXT_CACHE_SIZE;
	        }
    	}
        
        return context;
    }
	
    private static final byte[] getCacheBytes(final String context)
    {
    	final int contextLength = context.length();

    	// Only short contexts are cached
    	if (contextLength > 12)
    		return context.getBytes(StandardCharsets.UTF_8);
        
    	synchronized(stringCache)
    	{
	        // Simple array-based cache lookup
	        for (int i = 0; i < CONTEXT_CACHE_SIZE && stringCache[i] != null; i++) 
	        {
	            if (contextLength == stringCache[i].length())
	            {
	            	if (context.equals(stringCache[i]))
	            		return bytesCache[i];
	            }
	        }
	        
            // Cache the new string and its bytes
            byte[] contextBytes = context.getBytes(StandardCharsets.UTF_8);
            stringCache[cacheIndex] = context;
            bytesCache[cacheIndex] = contextBytes;
            cacheIndex = (cacheIndex + 1) % CONTEXT_CACHE_SIZE;
            
            return contextBytes;
    	}
    }

    public static final StateAddress from(final Class<?> context, final String scope)
	{
		return from(context, Hash.valueOf(Strings.toLowerCase(Objects.requireNonNull(scope, "Scope is null"))));
	}

	public static final StateAddress from(final Class<?> context, final Identity identity)
	{
		return from(Objects.requireNonNull(context, "Class for context is null").getAnnotation(StateContext.class).value(), Objects.requireNonNull(identity, "Identity is null"));
	}

	public static final StateAddress from(final Class<?> context, final Hash scope)
	{
		return new StateAddress(Objects.requireNonNull(context, "Class for context is null").getAnnotation(StateContext.class).value(), scope);
	}

	public static final StateAddress from(final String context, final String scope)
	{
		return new StateAddress(context, Hash.valueOf(Strings.toLowerCase(Objects.requireNonNull(scope, "Scope is null"))));
	}
	
	public static final StateAddress from(final String context, final Hash scope)
	{
		return new StateAddress(context, scope);
	}

	public static final StateAddress from(final String context, final Identity identity)
	{
		return new StateAddress(context, Hash.valueOf(Objects.requireNonNull(identity, "Identity is null")));
	}
	
	public static final StateAddress from(final String value, final int offset) 
	{
		Objects.requireNonNull(value, "String is null");
		
		final int separator = value.indexOf(':', offset);
	    final String context = value.substring(offset, separator);
	    final Hash scope = Hash.hash(value, separator + 1, value.length() - (separator + 1));
	    return StateAddress.from(context, scope);
	}
	
	public static StateAddress from(final byte[] bytes)
	{
		return from(bytes, 0);
	}

	public static StateAddress from(final byte[] bytes, final int offset)
	{
		int p = offset;
		final int contextLength = bytes[p++];
		final String context = checkCache(bytes, p, contextLength);p+=contextLength;
		final Hash scope = Hash.from(bytes, p);
		return new StateAddress(context, scope);
	}

	private final String context;
	private final Hash scope;

	private volatile ShardID shardID;
	private volatile Hash cachedHash;
	
	private StateAddress(final String context, final Hash scope)
	{
		Objects.requireNonNull(context, "Context is null");
		Numbers.lessThan(context.length(), 3, "Context is too short");
		Objects.requireNonNull(scope, "Scope is null");
		
		this.context = Strings.toLowerCase(context);
		this.scope = scope;
	}

	@Override
	public synchronized Hash getHash()
	{
		if (this.cachedHash == null)
			this.cachedHash = Hash.hash(checkCache(this.context).toByteArray(), this.scope.toByteArray());

		return this.cachedHash;
	}

	public synchronized ShardID getShardID()
	{
		if (this.shardID == null)
			this.shardID = ShardID.from(scope);

		return this.shardID;
	}

	public String context()
	{
		return this.context;
	}

	public Hash scope()
	{
		return this.scope;
	}

	@Override
	public int hashCode() 
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + this.context.hashCode();
		result = prime * result + this.scope.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj) 
	{
		if (this == obj)
			return true;
		
		if (obj == null)
			return false;
		
		if (obj instanceof StateAddress other)
		{
			if (this.scope.equals(other.scope) == false)
				return false;
	
			if (this.context.equals(other.context) == false)
				return false;
			
			return true;
		}
		
		return false;
	}

	@Override
	public final String toString()
	{
		return getHash()+" "+this.context+":"+this.scope;
	}
	
	public byte[] toByteArray()
	{
		final byte[] contextBytes = this.context.getBytes(StandardCharsets.UTF_8);
		final byte[] scopeBytes = this.scope.toByteArray();
		
		final byte[] bytes = new byte[contextBytes.length+scopeBytes.length+1];
		bytes[0] = (byte) contextBytes.length;
		System.arraycopy(contextBytes, 1, bytes, 0, contextBytes.length);
		System.arraycopy(scopeBytes, 1, bytes, contextBytes.length, scopeBytes.length);
		return bytes;
	}
	
	public void write(final ByteBuffer buffer)
	{
		final byte[] contextBytes = getCacheBytes(this.context);
		final byte[] scopeBytes = this.scope.toByteArray();
		
		buffer.put((byte)contextBytes.length);
		buffer.put(contextBytes);
		buffer.put(scopeBytes);
	}

	@Override
	public int compareTo(final StateAddress other) 
	{
		int result = this.context.compareToIgnoreCase(other.context);
		if (result != 0)
			return result;
		
		return this.scope.compareTo(other.scope);
	}
}
