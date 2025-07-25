package org.radix.hyperscale.crypto.bls12381;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.radix.hyperscale.crypto.Identity;
import org.radix.hyperscale.crypto.PublicKey;
import org.radix.hyperscale.crypto.Signature.VerificationResult;
import org.radix.hyperscale.crypto.bls12381.group.G2Point;
import org.radix.hyperscale.utils.Base58;
import org.radix.hyperscale.utils.Numbers;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Forked from https://github.com/ConsenSys/mikuli/tree/master/src/main/java/net/consensys/mikuli/crypto
 * 
 * Modified for use with Cassandra as internal code not a dependency
 * 
 * Original repo source has no license headers.
 */
public final class BLSPublicKey extends PublicKey<BLSSignature>
{
	public final static BLSPublicKey NULL = new BLSPublicKey(G2Point.NULL);

	@JsonCreator
	public static BLSPublicKey from(final byte[] bytes) 
	{
	    return from(bytes, 0);
	}

	public static BLSPublicKey from(final String key)
	{
		Objects.requireNonNull(key, "Key string is null");
		Numbers.isZero(key.length(), "Key string is empty");
		byte[] bytes = Base58.fromBase58(Objects.requireNonNull(key, "Key string is null"));
	    return new BLSPublicKey(bytes, 0);
	}

	public static BLSPublicKey from(final byte[] bytes, int offset) 
	{
	    return new BLSPublicKey(bytes, offset);
	}
	
	private byte[] bytes;
	private volatile G2Point point;
	
	private transient Identity identity;

	@SuppressWarnings("unused")
	private BLSPublicKey()
	{
		// FOR SERIALIZER
	}

	BLSPublicKey(final byte[] bytes, int offset) 
	{
		Objects.requireNonNull(bytes, "Bytes for public key is null");
		Numbers.isZero(bytes.length, "Bytes length is zero");
		
		this.bytes = Arrays.copyOfRange(bytes, offset, bytes.length);
	}

	BLSPublicKey(final G2Point point) 
	{
		Objects.requireNonNull(point, "Public key point is null");
		this.point = point;
		this.bytes = this.point.toBytes();
	}

	public BLSPublicKey combine(final List<BLSPublicKey> publicKeys) 
	{
		Objects.requireNonNull(publicKeys, "Public keys to combine is null");
		return combine(publicKeys, 0, publicKeys.size());
	}
	
	public BLSPublicKey combine(final List<BLSPublicKey> publicKeys, int start, int end) 
	{
		Objects.requireNonNull(publicKeys, "Public keys to combine is null");
		if (start == end)
			return this;
		
		G2Point[] toAdd = new G2Point[end-start];
		for (int i = start ; i < end ; i++)
			toAdd[i-start] = publicKeys.get(i).g2Point();

		G2Point aggregated = g2Point().add(toAdd);
		return new BLSPublicKey(aggregated);
	}

	public BLSPublicKey combine(final BLSPublicKey publicKey) 
	{
		Objects.requireNonNull(publicKey, "Public key for combine is null");
		return new BLSPublicKey(g2Point().add(publicKey.g2Point()));
	}

	synchronized G2Point g2Point() 
  	{
		if (this.point == null)
			this.point = G2Point.fromBytes(this.bytes);

		return this.point;
  	}
  	
	public synchronized Identity getIdentity()
	{
		if (this.identity == null)
			this.identity = new Identity(Identity.BLS, this);
		
		return this.identity;
	}

	@JsonValue
	@Override
	public byte[] toByteArray()
	{
		return this.bytes;
	}

	@Override
	public boolean verify(final byte[] hash, final BLSSignature signature)
	{
		Objects.requireNonNull(hash, "Hash to verify is null");
		Numbers.isZero(hash.length, "Hash length is zero");
		Objects.requireNonNull(signature, "Signature to verify is null");
		
		if (signature.isVerified().equals(VerificationResult.UNVERIFIED) == false)
		{
			if (signature.isVerified().equals(VerificationResult.SUCCESS))
				return true;
			
			return false;
		}

		if (BLS12381.SKIP_VERIFICATION)
			return true;
		
		boolean isSuccessful = BLS12381.verify(this, signature, hash);
		signature.setVerified(isSuccessful ? VerificationResult.SUCCESS : VerificationResult.FAILED);
		BLSKeyPair.verifications.incrementAndGet();
		return isSuccessful;
	}
}
