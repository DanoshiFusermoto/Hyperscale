package org.radix.hyperscale.crypto.ed25519;

import java.util.Objects;

import org.radix.hyperscale.crypto.CryptoException;
import org.radix.hyperscale.crypto.PrivateKey;
import org.radix.hyperscale.crypto.Signature.VerificationResult;
import org.radix.hyperscale.utils.Bytes;

public final class EDPrivateKey extends PrivateKey<EDSignature>
{
	public static final int	BYTES = 32;

	private byte[] privateKey;
	
	public static EDPrivateKey from(byte[] key) throws CryptoException 
	{
		return new EDPrivateKey(key);
	}
	
	public static EDPrivateKey from(String key) throws CryptoException 
	{
		return new EDPrivateKey(key);
	}

	private EDPrivateKey(String key) throws CryptoException
	{
		this(Bytes.fromBase64String(Objects.requireNonNull(key, "Key string is null")));
	}

	private EDPrivateKey(byte[] key) throws CryptoException 
	{
		Objects.requireNonNull(key, "Key bytes is null");
		
		try {
			validatePrivate(key);
			this.privateKey = trimPrivateKey(key);
		} catch (CryptoException ex) {
			throw ex;
		}
		catch (Exception ex) {
			throw new CryptoException(ex);
		}
	}

	private byte[] trimPrivateKey(byte[] privKey) 
	{
		if (privKey.length > BYTES && privKey[0] == 0) 
		{
			byte[] tmp = new byte[privKey.length - 1];
			System.arraycopy(privKey, 1, tmp, 0, privKey.length - 1);
			return tmp;
		}
		
		if (privKey.length < BYTES) 
		{
			byte[] tmp = new byte[BYTES];
			System.arraycopy(privKey, 0, tmp, BYTES - privKey.length, privKey.length);
		}
		
		return privKey;
	}

	private void validatePrivate(byte[] privateKey) throws CryptoException 
	{
		if (privateKey == null || privateKey.length == 0)
			throw new CryptoException("Private key is null");

		int pklen = privateKey.length;
		if (allZero(privateKey, 0, pklen))
			throw new CryptoException("Private key is zero");

		if (allZero(privateKey, 0, pklen - 1) && privateKey[pklen - 1] == 1)
			throw new CryptoException("Private key is one");
	}
	
	public EDSignature sign(byte[] hash) throws CryptoException 
	{ 
		if (EDKeyPair.SKIP_SIGNING)
			return EDSignature.NULL;
			
		EDSignature signature = EDSignature.from(ED25519.signPrehashed(this.privateKey, hash));
		signature.setVerified(VerificationResult.SUCCESS);
		EDKeyPair.signings.incrementAndGet();
		return signature;
	}
	
	private boolean allZero(byte[] bytes, int offset, int len) 
	{
		for (int i = 0; i < len; ++i) 
		{
			if (bytes[offset + i] != 0)
				return false;
		}
		return true;
	}
	
	public byte[] toByteArray()
	{
		return this.privateKey;
	}
}
