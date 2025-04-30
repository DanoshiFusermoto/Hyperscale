package org.radix.hyperscale.ledger.primitives;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import org.radix.hyperscale.Constants;
import org.radix.hyperscale.common.ExtendedObject;
import org.radix.hyperscale.common.Primitive;
import org.radix.hyperscale.crypto.CryptoException;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.crypto.Identity;
import org.radix.hyperscale.crypto.KeyPair;
import org.radix.hyperscale.crypto.PublicKey;
import org.radix.hyperscale.crypto.Signature;
import org.radix.hyperscale.exceptions.ValidationException;
import org.radix.hyperscale.ledger.StateContext;
import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;
import org.radix.hyperscale.serialization.DsonCached;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.Serialization;
import org.radix.hyperscale.serialization.SerializationException;
import org.radix.hyperscale.serialization.SerializerId2;
import org.radix.hyperscale.serialization.DsonOutput.Output;
import org.radix.hyperscale.utils.Numbers;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.atom")
@StateContext("atom")
@DsonCached
public final class Atom extends ExtendedObject implements Primitive
{
	private static final Logger serializerlog = Logging.getLogger("serializer");

	public static final int MAX_MANIFEST_ITEMS = 512;
	
	public final static class Builder
	{
		private final Atom atom;
		private final Map<Identity, KeyPair<?,?,?>> signers;
		
		private volatile boolean built = false;
		
		private Builder(long nonce)
		{
			this.atom = new Atom();
			this.atom.nonce = nonce;
			this.signers = new HashMap<Identity, KeyPair<?,?,?>>(3); // Rarely will there be more than three signers
		}

		public Builder()
		{
			this(ThreadLocalRandom.current().nextLong());
		}

		public Builder(final List<String> manifest)
		{
			this(ThreadLocalRandom.current().nextLong(), manifest);
		}
		
		public Builder(final long nonce, final List<String> manifest)
		{
			this(nonce);
			this.atom.manifest.addAll(manifest);
		}
		
		public void reset()
		{
			this.atom.nonce = ThreadLocalRandom.current().nextLong();
			this.atom.manifest.clear();
			this.signers.clear();
			this.built = false;
		}
		
		private void throwIfBuilt()
		{
			if (this.built)
				throw new IllegalStateException("Atom is built and now immutable");
		}
		
		public long getNonce()
		{
			return this.atom.nonce;
		}
		
		public Atom get()
		{
			if (this.built == false)
				throw new IllegalStateException("Atom is not built");
			
			return this.atom;
		}
		
		public List<String> getManifest()
		{
			return Collections.unmodifiableList(this.atom.manifest);
		}
		
		public Builder push(final Blob blob) 
		{
			Objects.requireNonNull(blob);
			
			throwIfBuilt();

			if (this.atom.manifest.size() == Atom.MAX_MANIFEST_ITEMS)
				throw new IllegalStateException("Manifest contains maximum items of "+Atom.MAX_MANIFEST_ITEMS);

			this.atom.manifest.add(blob.asDataURL());
			return this;
		}

		public Builder push(final Collection<String> manifest) 
		{
			Objects.requireNonNull(manifest);
			
			throwIfBuilt();

			if (this.atom.manifest.size() + manifest.size() > Atom.MAX_MANIFEST_ITEMS)
				throw new IllegalStateException("Manifest would exceed maximum items of "+Atom.MAX_MANIFEST_ITEMS);
			
			this.atom.manifest.addAll(manifest);
			return this;
		}
		
		public Builder push(final String instruction) 
		{
			Objects.requireNonNull(instruction);
			Numbers.isZero(instruction.length(), "Manifest instruction is empty");

			throwIfBuilt();
			
			if (this.atom.manifest.size() == Atom.MAX_MANIFEST_ITEMS)
				throw new IllegalStateException("Manifest contains maximum items of "+Atom.MAX_MANIFEST_ITEMS);
			
			this.atom.manifest.add(instruction);
			return this;
		}
		
		public Builder signer(final KeyPair<?,?,?> signer) 
		{
			Objects.requireNonNull(signer);
			
			throwIfBuilt();

			this.signers.putIfAbsent(signer.getIdentity(), signer);
			return this;
		}
		
		public Atom build(final int difficulty) throws CryptoException
		{
			return build(difficulty, null);
		}
		
		public Atom build(final int difficulty, Collection<KeyPair<?,?,?>> signers) throws CryptoException
		{
			throwIfBuilt();

			if (difficulty > 0)
				discoverHash(difficulty, false);

			// Signers set via builder
			if (this.signers.isEmpty() == false)
			{
				for (final KeyPair<?,?,?> signer : this.signers.values())
					this.atom.sign(signer);
			}

			// Signers passed as parameters
			if (signers != null && signers.isEmpty() == false)
			{
				for (final KeyPair<?,?,?> signer : signers)
					this.atom.sign(signer);
			}
			
			this.built = true;
			
			return this.atom;
		}
		
		public Hash discoverHash(final int difficulty, boolean reset)
		{
			if (reset)
				this.atom.nonce = ThreadLocalRandom.current().nextLong();
			
			Hash hash = this.atom.computeHash();
			while(hash.leadingZeroBits() < difficulty)
			{
				this.atom.nonce++;
				hash = this.atom.computeHash();
			}
			return hash;
		}
	}

	@JsonProperty("nonce")
	@DsonOutput(Output.ALL)
	private long nonce;

	@JsonProperty("manifest")
	@DsonOutput(Output.ALL)
	private List<String> manifest;
	
	@JsonProperty("signatures")
	@DsonOutput(value = {Output.API, Output.WIRE, Output.PERSIST})
	@JsonDeserialize(as = HashMap.class)
	private Map<Identity, Signature> signatures;
	
	private volatile boolean immutable = false;
	
	@SuppressWarnings("unused")
	private Atom()
	{
		super();
		
		this.nonce = ThreadLocalRandom.current().nextLong();
		this.manifest = new ArrayList<String>(4);
		this.signatures = new HashMap<Identity, Signature>(4);
	}

	/**
	 * Create a copy of the supplied atom discarding the state snapshot.
	 * 
	 * TODO this probably isn't required anymore as Atoms done carry a state snapshot with them.  
	 * This function is hooked into a number of critical places though, so not going to touch for now.
	 * 
	 * @param atom
	 */
	public Atom(final Atom atom)
	{
		super();
		
		Objects.requireNonNull(atom, "Atom is null");
		
		this.nonce = atom.nonce;
		this.manifest = new ArrayList<String>(atom.manifest);
		this.signatures = new HashMap<Identity, Signature>(atom.signatures);
	}

	@Override
	public boolean isDeferredPersist()
	{
		return true;
	}
	
	public List<String> getManifest()
	{
		return Collections.unmodifiableList(this.manifest);
	}
	
	public int getManifestSize()
	{
		return this.manifest.size();
	}

	public boolean isSealed()
	{
		return this.signatures.isEmpty() == false;
	}
	
	private void throwIfImmutable()
	{
		if (this.immutable)
			throw new IllegalStateException("Atom "+getHash()+" is sealed");
	}

	public Blob get(final Hash hash) 
	{
		Objects.requireNonNull(hash);
		
		for (String instruction : this.manifest)
		{
			if (instruction.startsWith("data:") == false)
				continue;
			
			Blob blob = new Blob(instruction);
			if (blob.getHash().equals(hash))
				return blob;
		}
		
		return null;
	}
	
	public Signature sign(final KeyPair<?,?,?> key) throws CryptoException
	{
		throwIfImmutable();

		Signature signature = this.signatures.get(key.getIdentity());
		if (signature == null)
		{
			signature = key.getPrivateKey().sign(getHash());
			this.signatures.put(key.getIdentity(), signature);
		}
		return signature;
	}

	public Signature signature(final PublicKey<?> key, final Signature signature)
	{
		throwIfImmutable();
		
		this.signatures.put(key.getIdentity(), signature);
		return signature;
	}
	
	public void validate() throws ValidationException
	{
		if (this.nonce == 0)
			throw new ValidationException("Nonce is zero");

		if (this.manifest == null)
			throw new ValidationException("Manifest is nuil");

		if (this.manifest.isEmpty())
			throw new ValidationException("Manifest is empty");
		
		if (this.signatures == null)
			throw new ValidationException("Signatures is nuil");

		if (this.signatures.isEmpty())
			throw new ValidationException("Signatures is empty");
	}

	public boolean verify() throws CryptoException
	{
		if (this.signatures.isEmpty())
			throw new CryptoException("Signatures is empty");
		
		for(final Entry<Identity, Signature> entry : this.signatures.entrySet())
		{
			try
			{
				if (entry.getKey().canVerify() == false)
					return false;
				
				if (((PublicKey)entry.getKey().getKey()).verify(getHash(), entry.getValue()) == false)
					return false;
			}
			catch (Exception ex)
			{
				throw ex;
			}
		}
		
		this.immutable = true;
		
		// Verified, also is now immutable so trigger the DSON caching
		try 
		{
			if(getCachedDsonOutput() == null)
				Serialization.getInstance().toDson(this, Output.PERSIST);
			else
				serializerlog.warn("DSON cache already present for atom "+this);
		} 
		catch (SerializationException ex) 
		{
			serializerlog.error("DSON cache priming failed", ex);
		}
		
		return true;
	}
	
	public boolean hasAuthority(final Identity authority)
	{
		Objects.requireNonNull(authority, "Authority is null");
		return this.signatures.containsKey(authority);
	}
	
	public Set<Identity> getAuthorities()
	{
		return new HashSet<Identity>(this.signatures.keySet());
	}
}
