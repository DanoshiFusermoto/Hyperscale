package org.radix.hyperscale.crypto.ed25519;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.radix.hyperscale.crypto.CryptoException;
import org.radix.hyperscale.crypto.KeyPair;
import org.radix.hyperscale.crypto.Signature.VerificationResult;

import com.google.common.collect.ImmutableSet;

/**
 * Asymmetric EC key pair provider fixed to curve 'secp256k1'.
 */
public final class EDKeyPair extends KeyPair<EDPrivateKey, EDPublicKey, EDSignature>
{
	public static final boolean SKIP_SIGNING = Boolean.getBoolean("EC.skip_signing");
	public static final boolean SKIP_VERIFICATION = Boolean.getBoolean("EC.skip_verification");
	
	static
	{
		if (SKIP_SIGNING)
			System.err.println("Warning: ECC signing is being skipped");
		if (SKIP_VERIFICATION)
			System.err.println("Warning: ECC verification is being skipped");
	}
	
	// Metrics
	static final AtomicLong signings = new AtomicLong(0);
	static final AtomicLong verifications = new AtomicLong(0);
	
	public final static long signCount()
	{
		return signings.get();
	}

	public final static long verifyCount()
	{
		return verifications.get();
	}

	/**
	 * Load a private key from file, and compute the public key.
	 *
	 * @param file  The file to load the private key from.
	 * @param create Set to {@code true} if the file should be created if it doesn't exist.
	 * @return An {@link EDKeyPair}
	 * @throws IOException If reading or writing the file fails
	 * @throws CryptoException If the key read from the file is invalid
	 */
	public static final EDKeyPair fromFile(File file, boolean create) throws IOException, CryptoException 
	{
		if (file.exists() == false) 
		{
			if (create == false)
				throw new FileNotFoundException("Keyfile " + file.toString() + " not found");

			File dir = file.getParentFile();
			if (dir != null && dir.exists() == false && dir.mkdirs() == false)
				throw new FileNotFoundException("Failed to create directory: " + dir.toString());

			try (FileOutputStream io = new FileOutputStream(file)) 
			{
				try 
				{
					Set<PosixFilePermission> perms = ImmutableSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE);
					Files.setPosixFilePermissions(file.toPath(), perms);
				} 
				catch (UnsupportedOperationException ignoredException) 
				{
					// probably windows
				}

				EDKeyPair key = new EDKeyPair();
				io.write(key.getPrivateKey().toByteArray());
				return key;
			}
		} 
		else 
		{
			try (FileInputStream io = new FileInputStream(file)) 
			{
				DataInputStream dis = new DataInputStream(io);
				byte[] privBytes = new byte[dis.readInt()];
				dis.readFully(privBytes);
				return new EDKeyPair(privBytes);
			}
		}
	}

	private final EDPrivateKey privateKey;
	private final EDPublicKey publicKey;

	public EDKeyPair() throws CryptoException 
	{
        byte[][] keyPairBytes = ED25519.generateKeyPair();
        this.privateKey = EDPrivateKey.from(keyPairBytes[0]);
        this.publicKey = EDPublicKey.from(keyPairBytes[1]);
	}
	
	public EDKeyPair(byte[] key) throws CryptoException 
	{
		try 
		{
			this.privateKey = EDPrivateKey.from(key);
			this.publicKey = EDPublicKey.from(ED25519.derivePublicKey(key));
		} 
		catch (Exception ex) 
		{
			throw new CryptoException(ex);
		}
	}
	
	public EDPrivateKey getPrivateKey() 
	{
		return this.privateKey;
	}

	public EDPublicKey getPublicKey() 
	{
		return this.publicKey;
	}

	@Override
	public EDSignature sign(byte[] hash) throws CryptoException
	{
		EDSignature signature = EDSignature.from(ED25519.signPrehashed(this.privateKey.toByteArray(), this.publicKey.toByteArray(), hash));
		signature.setVerified(VerificationResult.SUCCESS);
		EDKeyPair.signings.incrementAndGet();
		return signature;
	}
}
