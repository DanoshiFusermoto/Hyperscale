package org.radix.hyperscale.ledger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.eclipse.collections.api.factory.Lists;
import org.radix.hyperscale.common.CompoundPrimitive;
import org.radix.hyperscale.common.ExtendedObject;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.crypto.Hashable;
import org.radix.hyperscale.crypto.Identity;
import org.radix.hyperscale.ledger.BlockHeader.InventoryType;
import org.radix.hyperscale.ledger.primitives.Atom;
import org.radix.hyperscale.ledger.primitives.AtomCertificate;
import org.radix.hyperscale.ledger.sme.PolyglotPackage;
import org.radix.hyperscale.ledger.timeouts.CommitTimeout;
import org.radix.hyperscale.ledger.timeouts.ExecutionTimeout;
import org.radix.hyperscale.network.messages.Message;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.Serialization;
import org.radix.hyperscale.serialization.SerializationException;
import org.radix.hyperscale.serialization.SerializerId2;
import org.radix.hyperscale.serialization.DsonOutput.Output;
import org.radix.hyperscale.time.Time;
import org.radix.hyperscale.utils.UInt256;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.primitives.Longs;

@SerializerId2("ledger.block")
@StateContext("block")
public final class Block extends ExtendedObject implements CompoundPrimitive
{
	public static final long toHeight(Hash block)
	{
		return Longs.fromByteArray(block.toByteArray());
	}
	
	// The maximum amount of each type of inventory per proposal
	static final int MAX_BLOCK_SIZE = Integer.MAX_VALUE; //1<<18;
	static final int RESERVED_HEADER_SIZE = Message.MAX_MESSAGE_SIZE >> 4; //1<<18;

	@JsonProperty("header")
	@DsonOutput(value = {Output.API, Output.WIRE, Output.PERSIST})
	private BlockHeader header;
	
	@JsonProperty("accepted")
	@JsonInclude(JsonInclude.Include.NON_NULL)
	@DsonOutput(value = {Output.API, Output.WIRE, Output.PERSIST})
	private List<Atom> accepted;
	
	@JsonProperty("unaccepted")
	@JsonInclude(JsonInclude.Include.NON_NULL)
	@DsonOutput(value = {Output.API, Output.WIRE, Output.PERSIST})
	private List<Atom> unaccepted;

	@JsonProperty("unexecuted")
	@JsonInclude(JsonInclude.Include.NON_NULL)
	@DsonOutput(value = {Output.API, Output.WIRE, Output.PERSIST})
	private List<ExecutionTimeout> unexecuted;

	@JsonProperty("certificates")
	@JsonInclude(JsonInclude.Include.NON_NULL)
	@DsonOutput(value = {Output.API, Output.WIRE, Output.PERSIST})
	private List<AtomCertificate> certificates;
	
	@JsonProperty("uncommitted")
	@JsonInclude(JsonInclude.Include.NON_NULL)
	@DsonOutput(value = {Output.API, Output.WIRE, Output.PERSIST})
	private List<CommitTimeout> uncommitted;

	@JsonProperty("packages")
	@JsonInclude(JsonInclude.Include.NON_NULL)
	@DsonOutput(value = {Output.API, Output.WIRE, Output.PERSIST})
	private List<PolyglotPackage> packages;
	
	private transient int size = -1;

	@SuppressWarnings("unused")
	private Block()
	{
		super();
	}
	
	Block(final BlockHeader header, final Collection<Atom> accepted, final Collection<Atom> unaccepted, final Collection<ExecutionTimeout> unexecuted, final Collection<AtomCertificate> certificates, final Collection<CommitTimeout> uncommitted, final Collection<PolyglotPackage> packages)
	{
		super();

		Objects.requireNonNull(header, "Header is null");
		Objects.requireNonNull(accepted, "Accepted is null");
		Objects.requireNonNull(unaccepted, "Unaccepted is null");
		Objects.requireNonNull(unexecuted, "Unexecuted is null");
		Objects.requireNonNull(certificates, "Certificates is null");
		Objects.requireNonNull(uncommitted, "Uncommitted is null");
		Objects.requireNonNull(packages, "Packages is null");

		this.header = header;

		// TODO prevent duplicate inventory
		//		allowed currently to allow testing of duplicate atom injections which should fail during consensus
		this.accepted = accepted.isEmpty() ? Collections.emptyList() : Lists.immutable.ofAll(accepted).castToList();
		this.unaccepted = unaccepted.isEmpty() ? Collections.emptyList() : Lists.immutable.ofAll(unaccepted).castToList();
		this.unexecuted = unexecuted.isEmpty() ? Collections.emptyList() : Lists.immutable.ofAll(unexecuted).castToList();
		this.certificates = certificates.isEmpty() ? Collections.emptyList() : Lists.immutable.ofAll(certificates).castToList();
		this.uncommitted = uncommitted.isEmpty() ? Collections.emptyList() : Lists.immutable.ofAll(uncommitted).castToList();
		this.packages = packages.isEmpty() ? Collections.emptyList() : Lists.immutable.ofAll(packages).castToList();
	}

	public Block(final long height, final Hash previous, final long difficulty, final UInt256 work,
				 final long nonce, final long index, final Identity proposer, 
			     final Collection<Atom> accepted, final Collection<Atom> unaccepted, 
			     final Collection<ExecutionTimeout> unexecuted, final Collection<AtomCertificate> certificates, 
			     final Collection<CommitTimeout> uncommitted, final Collection<Hash> executables, final Collection<Hash> latent,
			     final Collection<PolyglotPackage> packages)
	{
		this(height, previous, difficulty, work, nonce, index, Time.getLedgerTimeMS(), proposer, 
			 accepted, unaccepted, unexecuted, certificates, uncommitted, executables, latent, packages);
	}
	
	public Block(final long height, final Hash previous, final long difficulty, final UInt256 work, final long nonce, final long index, final long timestamp, final Identity proposer, 
				 final Collection<Atom> accepted, final Collection<Atom> unaccepted, 
				 final Collection<ExecutionTimeout> unexecuted, final Collection<AtomCertificate> certificates, 
				 final Collection<CommitTimeout> uncommitted, final Collection<Hash> executables, final Collection<Hash> latent, 
				 final Collection<PolyglotPackage> packages)
	{
		super();

		Objects.requireNonNull(accepted, "Accepted is null");
		Objects.requireNonNull(unaccepted, "Unaccepted is null");
		Objects.requireNonNull(unexecuted, "Unexecuted is null");
		Objects.requireNonNull(certificates, "Certificates is null");
		Objects.requireNonNull(uncommitted, "Uncommitted is null");
		Objects.requireNonNull(packages, "Packages is null");

		Objects.requireNonNull(executables, "Executables is null");
		Objects.requireNonNull(latent, "Latent is null");
		
		// TODO prevent duplicate inventory
		//		allowed currently to allow testing of duplicate atom injections which should fail during consensus
		this.accepted = accepted.isEmpty() ? Collections.emptyList() : Lists.immutable.ofAll(accepted).castToList();
		this.unaccepted = unaccepted.isEmpty() ? Collections.emptyList() : Lists.immutable.ofAll(unaccepted).castToList();
		this.unexecuted = unexecuted.isEmpty() ? Collections.emptyList() : Lists.immutable.ofAll(unexecuted).castToList();
		this.certificates = certificates.isEmpty() ? Collections.emptyList() : Lists.immutable.ofAll(certificates).castToList();
		this.uncommitted = uncommitted.isEmpty() ? Collections.emptyList() : Lists.immutable.ofAll(uncommitted).castToList();
		this.packages = packages.isEmpty() ? Collections.emptyList() : Lists.immutable.ofAll(packages).castToList();
		
		final Map<InventoryType, Collection<Hash>> inventory = new EnumMap<>(InventoryType.class);
		inventory.put(InventoryType.ACCEPTED,    createHashList(this.accepted));
		inventory.put(InventoryType.UNACCEPTED,  createHashList(this.unaccepted));
		inventory.put(InventoryType.UNEXECUTED,  createHashList(this.unexecuted));
		inventory.put(InventoryType.COMMITTED,   createHashList(this.certificates));
		inventory.put(InventoryType.UNCOMMITTED, createHashList(this.uncommitted));
		inventory.put(InventoryType.PACKAGES,    createHashList(this.packages));
		
		inventory.put(InventoryType.EXECUTABLE, executables.isEmpty() ? Collections.emptyList() : new ArrayList<Hash>(executables));
		inventory.put(InventoryType.LATENT, latent.isEmpty() ? Collections.emptyList() : new ArrayList<Hash>(latent));

		this.header = new BlockHeader(height, previous, difficulty, work, nonce, index, inventory, timestamp, proposer);
	}
	
	private <T extends Hashable> List<Hash> createHashList(Collection<T> collection) 
	{
	    if (collection.isEmpty())
	        return Collections.emptyList();
	    
	    List<Hash> hashes = new ArrayList<>(collection.size());
	    for (T item : collection)
	        hashes.add(item.getHash());

	    return hashes;
	}

	@Override
	protected synchronized Hash computeHash()
	{
		return this.header.getHash();
	}
	
	// TODO only used for Genesis check right now, but can probably be removed / improved
	public boolean contains(final Hash hash)
	{
		Objects.requireNonNull(hash, "Atom hash is null");
		Hash.notZero(hash, "Atom hash is zero");

		for (int a = 0 ; a < this.accepted.size() ; a++)
		{
			Atom atom = this.accepted.get(a);
			if (atom.getHash().equals(hash))
				return true;
		}
		
		return false;
	}
	
	public BlockHeader getHeader()
	{
		return this.header;
	}

	public Collection<Atom> getAccepted()
	{
		return this.accepted;
	}

	public Collection<Atom> getUnaccepted()
	{
		return this.unaccepted;
	}
	
	public Collection<AtomCertificate> getCertificates()
	{
		return this.certificates;
	}

	public Collection<ExecutionTimeout> getUnexecuted()
	{
		return this.unexecuted;
	}

	public Collection<CommitTimeout> getUncommitted()
	{
		return this.uncommitted;
	}

	public Collection<PolyglotPackage> getPackages()
	{
		return this.packages;
	}

	public Collection<Hash> getExecutables()
	{
		return this.header.getInventory(InventoryType.EXECUTABLE);
	}
	
	public Collection<Hash> getLatent()
	{
		return this.header.getInventory(InventoryType.LATENT);
	}
	
	public synchronized int getSize() throws SerializationException
	{
		if (this.size == -1)
		{
			int currentSize = getHeader().getSize();
			for (Atom atom : getAccepted())
			{
				byte[] cached = atom.getCachedDsonOutput();
				if (cached == null)
					cached = Serialization.getInstance().toDson(atom, Output.WIRE);
				
				currentSize += cached.length;
			}
			
			for (Atom atom : getUnaccepted())
			{
				byte[] cached = atom.getCachedDsonOutput();
				if (cached == null)
					cached = Serialization.getInstance().toDson(atom, Output.WIRE);
				
				currentSize += cached.length;
			}

			for (AtomCertificate certificate : getCertificates())
			{
				byte[] cached = certificate.getCachedDsonOutput();
				if (cached == null)
					cached = Serialization.getInstance().toDson(certificate, Output.WIRE);
				
				currentSize += cached.length;
			}

			for (ExecutionTimeout timeout : getUnexecuted())
			{
				byte[] cached = timeout.getCachedDsonOutput();
				if (cached == null)
					cached = Serialization.getInstance().toDson(timeout, Output.WIRE);
				
				currentSize += cached.length;
			}

			for (CommitTimeout timeout : getUncommitted())
			{
				byte[] cached = timeout.getCachedDsonOutput();
				if (cached == null)
					cached = Serialization.getInstance().toDson(timeout, Output.WIRE);
				
				currentSize += cached.length;
			}
			
			for (PolyglotPackage pakage : getPackages())
			{
				byte[] cached = pakage.getCachedDsonOutput();
				if (cached == null)
					cached = Serialization.getInstance().toDson(pakage, Output.WIRE);
				
				currentSize += cached.length;
			}

			currentSize += getExecutables().size() * Hash.BYTES;
			currentSize += getLatent().size() * Hash.BYTES;
			this.size = currentSize;
		}
		
		return this.size;
	}
}
