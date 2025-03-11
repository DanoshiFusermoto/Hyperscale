package org.radix.hyperscale.crypto;

import java.util.Arrays;
import java.util.Objects;

import org.apache.commons.lang3.ArrayUtils;
import org.radix.hyperscale.crypto.bls12381.BLSPublicKey;
import org.radix.hyperscale.crypto.ed25519.EDPublicKey;
import org.radix.hyperscale.utils.Base58;
import org.radix.hyperscale.utils.Bytes;
import org.radix.hyperscale.utils.UInt128;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public final class Identity extends Key
{
	public static final byte EC = (byte)0x01;
	public static final byte BLS = (byte)0x02;
	public static final byte COMPUTE = (byte)0XFF;
	
	public static final Identity NULL = new Identity();
	
	public static Identity from(final String identity) 
	{
		return from(identity, 0);
	}
	
	public static Identity from(final String identity, final int offset) 
	{
		byte[] bytes = Base58.fromBase58(Objects.requireNonNull(identity, "Identity string is null"), offset);
	    return from(bytes, 0);
	}

	@JsonCreator
	public static Identity from(final byte[] bytes) 
	{
	    return from(bytes, 0);
	}
	
	public static Identity from(final byte[] bytes, final int offset) 
	{
	    return new Identity(bytes, offset);
	}

	private byte 	prefix; 
	private Key		key;
	
	private volatile byte[] cachedBytes;

	private Identity()
	{
		this.prefix = 0x00;
		this.key = ComputeKey.NULL;
	}

	private Identity(byte[] bytes, int offset) 
	{
		Objects.requireNonNull(bytes, "Identity bytes is null");
		
		this.prefix = bytes[offset];
		if (this.prefix == 0x00 && Bytes.allZero(bytes, offset, bytes.length-offset))
			this.key = ComputeKey.NULL;
		else if (this.prefix == EC)
			this.key = EDPublicKey.from(bytes, offset+1);
		else if (this.prefix == BLS)
			this.key = BLSPublicKey.from(bytes, offset+1);
		else if (this.prefix == COMPUTE)
			this.key = ComputeKey.from(bytes, offset+1);
		else
			throw new IllegalArgumentException("Identity of type "+this.prefix+" is not supported for "+Base58.toBase58(bytes));
	}

	public Identity(final byte prefix, final Key key)
	{
		Objects.requireNonNull(key, "Key is null");
		if (prefix != 0x00 && prefix != EC && prefix != BLS && prefix != COMPUTE)
			throw new IllegalArgumentException("Key prefix "+prefix+" is unknown");
		
		if (prefix == 0x00 && key.equals(NULL.key) == false)
			throw new IllegalArgumentException("Key with prefix "+prefix+" is invalid");
		
		this.key = key;
		this.prefix = prefix;
	}

	public byte getPrefix()
	{
		return this.prefix;
	}

	@SuppressWarnings("unchecked")
	public <T extends Key> T getKey()
	{
		return (T) this.key;
	}

	@Override
	public boolean canSign()
	{
		return this.key.canSign();
	}

	@Override
	public boolean canVerify()
	{
		return this.key.canVerify();
	}
	
	@Override
	int computeHashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + this.prefix;
		result = prime * result + this.key.hashCode();
		return result;
	}

	@Override
	boolean computeEqual(final Object object)
	{
		if (object == null)
			return false;
		
		if (object == this)
			return true;

		if (object instanceof Identity identity)
		{
			if (this.prefix != identity.prefix)
				return false;
			
			if (Arrays.equals(this.key.toByteArray(), identity.key.toByteArray()) == false)
				return false;
			
			return true;
		}
		
		return false;
	}

	@JsonValue
	@Override
	public synchronized byte[] toByteArray()
	{
		if (this.cachedBytes == null)
			this.cachedBytes = ArrayUtils.addFirst(this.key.toByteArray(), this.prefix);
		
		return this.cachedBytes;
	}

	// MEH but can't really do anything about it!  An identity is a key and a key can be an identity
	@Override
	public final Identity getIdentity() 
	{
		return this;
	}
	
	public UInt128 distance(final Identity other)
	{
		Hash oh = Hash.hash(toByteArray(), other.toByteArray());
		UInt128 d = UInt128.from(oh.toByteArray()).xor(UInt128.from(other.getHash().toByteArray()));
		return d;
	}
}
