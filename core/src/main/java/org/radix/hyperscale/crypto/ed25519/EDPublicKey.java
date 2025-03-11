package org.radix.hyperscale.crypto.ed25519;

import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Arrays;
import java.util.Objects;

import org.radix.hyperscale.crypto.CryptoException;
import org.radix.hyperscale.crypto.Identity;
import org.radix.hyperscale.crypto.PublicKey;
import org.radix.hyperscale.crypto.Signature.VerificationResult;
import org.radix.hyperscale.utils.Base58;

import com.fasterxml.jackson.annotation.JsonCreator;

/**
 * Asymmetric EC public key provider fixed to curve 'secp256k1'
 */
public final class EDPublicKey extends PublicKey<EDSignature>
{
	@JsonValue
	private final byte[] publicKey;
	
	private transient Identity identity;

	@JsonCreator
	public static EDPublicKey from(byte[] bytes)
	{
	    return new EDPublicKey(bytes, 0);
	}

	public static EDPublicKey from(byte[] bytes, int offset) 
	{
	    return new EDPublicKey(bytes, offset);
	}
	
	public static EDPublicKey from(String key)
	{
		byte[] bytes = Base58.fromBase58(Objects.requireNonNull(key, "Key string is null"));
	    return new EDPublicKey(bytes, 0);
	}

	private EDPublicKey(byte[] key, int offset)
	{
		Objects.requireNonNull(key, "Key bytes is null");
		
		this.publicKey = Arrays.copyOfRange(key, offset, key.length);
		validatePublic(this.publicKey);
	}

	private void validatePublic(byte[] key) 
	{
		if (key.length != ED25519.PUBLIC_KEY_SIZE)
			throw new IllegalArgumentException("Public key is an invalid format or size");
	}

	public synchronized Identity getIdentity()
	{
		if (this.identity == null)
			this.identity = new Identity(Identity.EC, this);
		
		return this.identity;
	}
	
	public byte[] toByteArray() 
	{
		return this.publicKey;
	}
	
    @Override
	public boolean verify(final byte[] hash, final EDSignature signature) throws CryptoException 
	{
		Objects.requireNonNull(signature, "Signature to verify is null");
		Objects.requireNonNull(hash, "Hash to verify is null");
		if (hash.length == 0)
			throw new IllegalArgumentException("Hash length is zero");

		if (signature.isVerified().equals(VerificationResult.UNVERIFIED) == false)
		{
			if (signature.isVerified().equals(VerificationResult.SUCCESS))
				return true;
			
			return false;
		}
		
		if (EDKeyPair.SKIP_VERIFICATION)
			return true;
		
		boolean isSuccessful = ED25519.verifyPrehashed(this.publicKey, hash, signature.toByteArray());
		signature.setVerified(isSuccessful ? VerificationResult.SUCCESS : VerificationResult.FAILED);
		EDKeyPair.verifications.incrementAndGet();
		return isSuccessful;
	}
}
