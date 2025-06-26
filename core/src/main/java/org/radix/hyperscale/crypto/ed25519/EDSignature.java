package org.radix.hyperscale.crypto.ed25519;

import java.util.Arrays;
import java.util.Objects;

import org.radix.hyperscale.crypto.Signature;
import org.radix.hyperscale.utils.Bytes;

public final class EDSignature extends Signature
{
	public final static EDSignature NULL = new EDSignature(new byte[ED25519.SIGNATURE_SIZE], 0);
	
	public static EDSignature from(byte[] bytes)
	{
		return from(bytes, 0);
	}
	
	public static EDSignature from(byte[] bytes, int offset)
	{
		return new EDSignature(bytes, offset);
	}

	private final byte[] bytes;

	private EDSignature(byte[] bytes, int offset)
	{
		Objects.requireNonNull(bytes, "Signature bytes is null");
		this.bytes = Arrays.copyOfRange(bytes, offset, bytes.length);
	}

	@Override
	public String toString()
	{
		return Bytes.toHexString(this.bytes);
	}

	@Override
	public byte[] toByteArray()
	{
		return this.bytes;
	}
}
