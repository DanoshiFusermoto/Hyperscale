package org.radix.hyperscale.crypto;

import java.util.Arrays;
import java.util.Objects;

import org.radix.hyperscale.utils.Base58;
import org.radix.hyperscale.utils.Numbers;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public final class ComputeKey extends Key
{
	public static final int	BYTES = Hash.BYTES;
	public static final ComputeKey NULL = ComputeKey.from(Hash.ZERO.toByteArray(), 0);
	
	@JsonValue
	private final byte[] bytes;

	private transient Identity identity;

	@JsonCreator
	public static ComputeKey from(final byte[] bytes)
	{
	    return new ComputeKey(bytes, 0);
	}

	public static ComputeKey from(final byte[] bytes, final int offset)
	{
	    return new ComputeKey(bytes, offset);
	}
	
	public static ComputeKey from(final Hash key)
	{
	    return new ComputeKey(key.toByteArray(), 0);
	}

	public static ComputeKey from(final String key)
	{
	    return new ComputeKey(key);
	}

	private ComputeKey(final String key)  
	{
		Objects.requireNonNull(key, "Key string is null");
		byte[] bytes = Base58.fromBase58(key);
		Numbers.equals(bytes.length, BYTES, "Invalid compute key size");
		this.bytes = bytes;
	}
	
	private ComputeKey(final byte[] key, final int offset)  
	{
		Objects.requireNonNull(key, "Key bytes is null");
		Numbers.equals(key.length-offset, BYTES, "Invalid compute key size");
		this.bytes = Arrays.copyOfRange(key, offset, key.length);
	}

	public synchronized Identity getIdentity()
	{
		if (this.identity == null)
			this.identity = new Identity(Identity.COMPUTE, this);
		
		return this.identity;
	}

	@Override
	Hash computeHash()
	{
		return Hash.from(this.bytes);
	}
	
	@Override
	public boolean canSign() 
	{
		return false;
	}

	@Override
	public boolean canVerify() 
	{
		return false;
	}

	public byte[] toByteArray() 
	{
		return this.bytes;
	}
}
