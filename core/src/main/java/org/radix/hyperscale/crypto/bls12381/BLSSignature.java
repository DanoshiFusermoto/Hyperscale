package org.radix.hyperscale.crypto.bls12381;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.radix.hyperscale.crypto.Signature;
import org.radix.hyperscale.crypto.bls12381.group.G1Point;
import org.radix.hyperscale.utils.Numbers;

/**
 * Forked from https://github.com/ConsenSys/mikuli/tree/master/src/main/java/net/consensys/mikuli/crypto
 * 
 * Modified for use with Cassandra as internal code not a dependency
 * 
 * Original repo source has no license headers.
 */
public final class BLSSignature extends Signature
{
	public final static BLSSignature NULL = new BLSSignature(G1Point.NULL);
	
	public static BLSSignature from(final G1Point point) 
	{
		return new BLSSignature(point);
	}

	public static BLSSignature from(final byte[] bytes) 
	{
		return from(bytes, 0);
	}

	public static BLSSignature from(final byte[] bytes, int offset) 
	{
		return new BLSSignature(bytes, 0);
	}

	private byte[] bytes;
	private volatile G1Point point;

	@SuppressWarnings("unused")
	private BLSSignature()
	{
		// FOR SERIALIZER
	}
	
	private BLSSignature(final byte[] bytes, int offset) 
	{
		Objects.requireNonNull(bytes, "Bytes for signature is null");
		Numbers.isZero(bytes.length, "Bytes length is zero");
		
		this.bytes = Arrays.copyOfRange(bytes, offset, bytes.length);
	}

	private BLSSignature(final G1Point point) 
	{
		Objects.requireNonNull(point, "Point for signature is null");
		this.point = point;
		this.bytes = this.point.toBytes();
	}

	@Override
	public String toString() 
	{
		return "Signature [ecpPoint="+g1Point()+"]";
	}

	public BLSSignature combine(final List<BLSSignature> signatures) 
	{
		return combine(signatures, 0, signatures.size());
	}

	public BLSSignature combine(final List<BLSSignature> signatures, int start, int end) 
	{
		Objects.requireNonNull(signatures, "Signature to combine is null");
		if (start == end)
			return this;
		
		G1Point[] toAdd = new G1Point[end-start];
		for (int i = start ; i < end ; i++)
			toAdd[i-start] = signatures.get(i).g1Point();

		G1Point aggregated = g1Point().add(toAdd);
		return new BLSSignature(aggregated);
	}

	public BLSSignature combine(final BLSSignature signature) 
	{
		Objects.requireNonNull(signature, "Signature to combine is null");
		return new BLSSignature(g1Point().add(signature.g1Point()));
	}

	public BLSSignature reduce(final BLSSignature signature) 
	{
		Objects.requireNonNull(signature, "Signature to reduce is null");
		return new BLSSignature(g1Point().sub(signature.g1Point()));
	}

	synchronized G1Point g1Point() 
  	{
		if (this.point == null)
			this.point = G1Point.fromBytes(this.bytes);

		return point;
  	}
  
	public byte[] toByteArray() 
	{
		return this.bytes;
	}
}
