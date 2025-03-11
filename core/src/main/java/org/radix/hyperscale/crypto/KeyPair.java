package org.radix.hyperscale.crypto;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Objects;
import java.util.Set;

import org.radix.hyperscale.utils.Bytes;

import com.google.common.collect.ImmutableSet;

public abstract class KeyPair<P extends PrivateKey<S>, K extends PublicKey<S>, S extends Signature>
{
	/**
	 * Write a private key to a file.
	 *
	 * @param file  The file to store the private key to.
	 * @param key   The key to store.
	 * 
	 * @throws IOException If writing the file fails
	 */
	public static final void toFile(File file, KeyPair<?, ?, ?> key) throws IOException 
	{
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

			DataOutputStream dos = new DataOutputStream(io);
			dos.writeInt(key.getPrivateKey().toByteArray().length);
			dos.write(key.getPrivateKey().toByteArray());
			dos.flush();
		}
	}

	public abstract P getPrivateKey(); 

	public abstract K getPublicKey(); 
	
	public final S sign(final Hash hash) throws CryptoException
	{
		Objects.requireNonNull(hash, "Hash to sign is null");
		return sign(hash.toByteArray());
	}

	public abstract S sign(byte[] hash) throws CryptoException; 

	public final Identity getIdentity()
	{
		return this.getPublicKey().getIdentity();
	}

	@Override
	public final boolean equals(Object object) 
	{
		if (this == object)
			return true;

		if (object instanceof KeyPair keyPair) 
		{
			// Comparing private keys should be sufficient
			return this.getPrivateKey().equals(keyPair.getPrivateKey());
		}
		
		return false;
	}

	@Override
	public final int hashCode() 
	{
		return this.getPrivateKey().hashCode();
	}

	@Override
	public final String toString() 
	{
		// Not going to print the private key here DUH
		return String.format("%s[%s]", getClass().getSimpleName(), Bytes.toBase64String(getPublicKey().toByteArray()));
	}
}
