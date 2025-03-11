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
	public static ComputeKey from(byte[] bytes)
	{
	    return new ComputeKey(bytes, 0);
	}

	public static ComputeKey from(byte[] bytes, int offset)
	{
	    return new ComputeKey(bytes, offset);
	}
	
	public static ComputeKey from(Hash key)
	{
	    return new ComputeKey(key.toByteArray(), 0);
	}

	public static ComputeKey from(String key)
	{
		byte[] bytes = Base58.fromBase58(Objects.requireNonNull(key, "Key string is null"));
	    return new ComputeKey(bytes, 0);
	}

	private ComputeKey(byte[] key, int offset)  
	{
		Objects.requireNonNull(key, "Key bytes is null");
		Numbers.equals(key.length-offset, BYTES, "Invalid compute key size "+(key.length-offset));
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
