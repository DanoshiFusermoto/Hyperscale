package org.radix.hyperscale.ledger.exceptions;

import java.util.Objects;

import org.radix.hyperscale.common.Primitive;
import org.radix.hyperscale.crypto.Hash;

public final class PrimitiveLockException extends LockException
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 7978429252785915519L;
	
	private final Class<? extends Primitive> primitive;
	private final Hash hash;
	
	private PrimitiveLockException(String message, Class<? extends Primitive> primitive, Hash hash)
	{
		super(message);

		Objects.requireNonNull(message, "Message is null");
		Objects.requireNonNull(primitive, "Primitive type is null");
		Objects.requireNonNull(hash, "Primitive hash is null");
		Hash.notZero(hash, "Primitive hash is ZERO");

		this.primitive = primitive;
		this.hash = hash;
	}

	public PrimitiveLockException(Class<? extends Primitive> primitive, Hash hash)
	{
		this("Primitive "+hash+" of type "+primitive+" is locked", primitive, hash);
	}

	public Class<? extends Primitive> getPrimitive() 
	{
		return this.primitive;
	}

	public Hash getHash() 
	{
		return this.hash;
	}
}
