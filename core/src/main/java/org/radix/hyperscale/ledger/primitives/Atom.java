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

import org.radix.hyperscale.common.ExtendedObject;
import org.radix.hyperscale.common.Primitive;
import org.radix.hyperscale.crypto.CryptoException;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.crypto.Identity;
import org.radix.hyperscale.crypto.Key;
import org.radix.hyperscale.crypto.KeyPair;
import org.radix.hyperscale.crypto.PublicKey;
import org.radix.hyperscale.crypto.Signature;
import org.radix.hyperscale.crypto.ed25519.EDSignature;
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
import com.google.common.annotations.VisibleForTesting;
import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.atom")
@StateContext("atom")
@DsonCached
public final class Atom extends ExtendedObject implements Primitive
{
	private static final Logger serializerlog = Logging.getLogger("serializer");

	public static final int MAX_MANIFEST_ITEMS = 512;
	
	@JsonProperty("nonce")
	@DsonOutput(Output.ALL)
	private long nonce;

	@JsonProperty("manifest")
	@DsonOutput(Output.ALL)
	private List<String> manifest;
	
	@JsonProperty("signatures")
	@DsonOutput(value = {Output.API, Output.WIRE, Output.PERSIST})
	@JsonDeserialize(as = HashMap.class)
	private Map<Identity, EDSignature> signatures;
	
	private volatile boolean immutable = false;
	
	public Atom()
	{
		super();
		
		this.nonce = ThreadLocalRandom.current().nextLong();
		this.manifest = new ArrayList<String>(3);
		this.signatures = new HashMap<Identity, EDSignature>(3);
	}

	@VisibleForTesting
	// IMPORTANT: Use with care when setting the nonce manually.  Should only be used by system functions such as collaborative consensus
	public Atom(long nonce)
	{
		super();
		
		this.nonce = nonce;
		this.manifest = new ArrayList<String>(3);
		this.signatures = new HashMap<Identity, EDSignature>(3);
	}

	/**
	 * Create a copy of the supplied atom discarding the state snapshot.
	 * 
	 * @param atom
	 */
	public Atom(final Atom atom)
	{
		super();
		
		Objects.requireNonNull(atom, "Atom is null");
		
		this.nonce = atom.nonce;
		this.manifest = new ArrayList<String>(atom.manifest);
		this.signatures = new HashMap<Identity, EDSignature>(atom.signatures);
	}

	public Atom(final String ... manifest)
	{
		super();
		
		Objects.requireNonNull(manifest, "Manifest is null");
		Numbers.isZero(manifest.length, "Manifest is empty");
		Numbers.greaterThan(manifest.length, Atom.MAX_MANIFEST_ITEMS, "Manifest exceeds maximum items of "+Atom.MAX_MANIFEST_ITEMS);
		
		this.nonce = ThreadLocalRandom.current().nextLong();
		this.manifest = new ArrayList<String>(manifest.length);
		for(int i = 0 ; i < manifest.length ; i++)
			this.manifest.add(manifest[i]);
		this.signatures = new HashMap<Identity, EDSignature>(3);
	}

	public Atom(final List<String> manifest)
	{
		super();
		
		Objects.requireNonNull(manifest, "Manifest is null");
		Numbers.isZero(manifest.size(), "Manifest is empty");
		Numbers.greaterThan(manifest.size(), Atom.MAX_MANIFEST_ITEMS, "Manifest exceeds maximum items of "+Atom.MAX_MANIFEST_ITEMS);
		
		this.nonce = ThreadLocalRandom.current().nextLong();
		this.manifest = new ArrayList<String>(manifest.size());
		for (int i = 0 ; i < manifest.size() ; i++)
			this.manifest.add(manifest.get(i));
			
		this.signatures = new HashMap<Identity, EDSignature>(3);
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
	
	public boolean isSealed()
	{
		return this.signatures.isEmpty() == false;
	}
	
	private void throwIfSealed()
	{
		if (isSealed())
			throw new IllegalStateException("Atom "+getHash()+" is sealed");
	}
	
	private void throwIfImmutable()
	{
		if (this.immutable)
			throw new IllegalStateException("Atom "+getHash()+" is sealed");
	}

	public void push(final String instruction) 
	{
		Objects.requireNonNull(instruction);
		Numbers.isZero(instruction.length(), "Manifest instruction is empty");

		throwIfSealed(); throwIfImmutable();
		
		if (this.manifest.size() == Atom.MAX_MANIFEST_ITEMS)
			throw new IllegalStateException("Manifest contains maximum items of "+Atom.MAX_MANIFEST_ITEMS);
		
		this.manifest.add(instruction);
	}

	public void push(final Blob blob) 
	{
		Objects.requireNonNull(blob);
		
		throwIfSealed(); throwIfImmutable();
		
		if (this.manifest.size() == Atom.MAX_MANIFEST_ITEMS)
			throw new IllegalStateException("Manifest contains maximum items of "+Atom.MAX_MANIFEST_ITEMS);

		this.manifest.add(blob.asDataURL());
	}

	public void push(final Collection<String> manifest) 
	{
		Objects.requireNonNull(manifest);
		
		throwIfSealed(); throwIfImmutable();
		
		if (this.manifest.size() + manifest.size() > Atom.MAX_MANIFEST_ITEMS)
			throw new IllegalStateException("Manifest would exceed maximum items of "+Atom.MAX_MANIFEST_ITEMS);
		
		this.manifest.addAll(manifest);
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
	
	public Signature sign(final KeyPair<?, ?, ?> key) throws CryptoException
	{
		throwIfImmutable();

		final EDSignature signature = (EDSignature) key.getPrivateKey().sign(getHash());
		this.signatures.put(key.getIdentity(), signature);
		return signature;
	}

	public Signature sign(final Key key, final EDSignature signature)
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
		
		for(final Entry<Identity, EDSignature> entry : this.signatures.entrySet())
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
