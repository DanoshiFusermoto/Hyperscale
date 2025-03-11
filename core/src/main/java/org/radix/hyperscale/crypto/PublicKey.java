package org.radix.hyperscale.crypto;

import java.util.Objects;

public abstract class PublicKey<S extends Signature> extends Key
{
	@Override
	public final boolean canSign()
	{
		return false;
	}

	@Override
	public final boolean canVerify()
	{
		return true;
	}

	public final boolean verify(final Hash hash, final S signature) throws CryptoException
	{
		return verify(Objects.requireNonNull(hash, "Hash to verify is null").toByteArray(), signature);
	}

	public abstract boolean verify(final byte[] hash, final S signature)  throws CryptoException;
}
