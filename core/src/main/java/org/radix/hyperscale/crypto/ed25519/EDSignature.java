package org.radix.hyperscale.crypto.ed25519;

import java.util.Objects;

import org.apache.commons.lang3.ArrayUtils;
import org.radix.hyperscale.crypto.Signature;
import org.radix.hyperscale.utils.Bytes;
import org.radix.hyperscale.utils.Numbers;

public final class EDSignature extends Signature
{
	public final static EDSignature NULL = new EDSignature(new byte[ED25519.SIGNATURE_SIZE]);
	
	public static EDSignature from(byte[] bytes)
	{
		Objects.requireNonNull(bytes, "Bytes is null for EC point");
		Numbers.isZero(bytes.length, "Bytes length is zero");
		return new EDSignature(bytes);
	}
	
	private final byte[] bytes;

	private EDSignature(byte[] bytes)
	{
		this.bytes = ArrayUtils.clone(bytes);
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
