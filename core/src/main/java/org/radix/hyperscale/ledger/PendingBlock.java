package org.radix.hyperscale.ledger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import org.eclipse.collections.api.factory.Maps;
import org.eclipse.collections.impl.factory.Sets;
import org.radix.hyperscale.Context;
import org.radix.hyperscale.collections.Bloom;
import org.radix.hyperscale.crypto.CryptoException;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.crypto.Hashable;
import org.radix.hyperscale.crypto.bls12381.BLS12381;
import org.radix.hyperscale.crypto.bls12381.BLSPublicKey;
import org.radix.hyperscale.crypto.bls12381.BLSSignature;
import org.radix.hyperscale.exceptions.ValidationException;
import org.radix.hyperscale.ledger.AtomStatus.State;
import org.radix.hyperscale.ledger.BlockHeader.InventoryType;
import org.radix.hyperscale.ledger.events.BlockAppliedEvent;
import org.radix.hyperscale.ledger.events.BlockConstructedEvent;
import org.radix.hyperscale.ledger.primitives.Atom;
import org.radix.hyperscale.ledger.primitives.AtomCertificate;
import org.radix.hyperscale.ledger.sme.PolyglotPackage;
import org.radix.hyperscale.ledger.timeouts.CommitTimeout;
import org.radix.hyperscale.ledger.timeouts.ExecutionTimeout;
import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;
import org.radix.hyperscale.time.Time;
import org.radix.hyperscale.utils.Numbers;

import com.google.common.collect.Multimap;

public final class PendingBlock implements Hashable
{
	private static final Logger blocksLog = Logging.getLogger("blocks");
	
	private static final int DEFAULT_STATE_LOCKS_PER_ATOM = 4;
	private static final int STATE_LOCKS_PER_ATOM_MULTIPLIER = 2;

	private final Context context;
	private	final long 	  witnessedAt;

	private final BlockHeader	header;
	private volatile Block 		block;
	private final AtomicBoolean constructing;
	
	private volatile boolean 	unbranched;
	private volatile boolean 	applied;
	private volatile Throwable	thrown;
	
	private final Map<Hash, PendingAtom> accepted;
	private final Map<Hash, PendingAtom> unaccepted;
	private final Map<Hash, PendingAtom> committed;
	private final Map<Hash, PendingAtom> unexecuted;
	private final Map<Hash, PendingAtom> uncommitted;
	private final Map<Hash, PendingAtom> executable;
	private final Map<Hash, PendingAtom> latent;
	private final Map<Hash, PolyglotPackage> packages;
	private final Set<Hash> absent;

	private volatile long voteWeight;
	private volatile long voteThreshold;
	private final Map<BLSPublicKey, BLSSignature> votes;
	
	private final Map<StateAddress, PendingAtom> statesLocked;
	private final Map<StateAddress, PendingAtom> statesUnlocked;
	
	PendingBlock(final Context context, final BlockHeader header)
	{
		this.context = Objects.requireNonNull(context, "Context is null");
		this.header = Objects.requireNonNull(header, "Block header is null");
		
		this.block = null;
		this.constructing = new AtomicBoolean(false);
		
		this.witnessedAt = Time.getLedgerTimeMS();
		this.unbranched = true;
		this.applied = false;

		this.accepted = Maps.mutable.ofInitialCapacity(header.getInventorySize(InventoryType.ACCEPTED));
		this.unaccepted = Maps.mutable.ofInitialCapacity(header.getInventorySize(InventoryType.UNACCEPTED));
		this.committed = Maps.mutable.ofInitialCapacity(header.getInventorySize(InventoryType.COMMITTED));
		this.unexecuted = Maps.mutable.ofInitialCapacity(header.getInventorySize(InventoryType.UNEXECUTED));
		this.uncommitted = Maps.mutable.ofInitialCapacity(header.getInventorySize(InventoryType.UNCOMMITTED));
		this.executable = Maps.mutable.ofInitialCapacity(header.getInventorySize(InventoryType.EXECUTABLE));
		this.latent = Maps.mutable.ofInitialCapacity(header.getInventorySize(InventoryType.LATENT));

		this.packages = Maps.mutable.ofInitialCapacity(header.getInventorySize(InventoryType.PACKAGES));

		this.votes = Maps.mutable.ofInitialCapacity(4);
		this.voteWeight = 0;
		this.voteThreshold = -1;
		
		final InventoryType[] types = InventoryType.values();
		this.absent = Sets.mutable.ofInitialCapacity(header.getTotalInventorySize());
		for (int i = 0 ; i < types.length ; i++)
			this.absent.addAll(header.getInventory(types[i]));
		
		if (this.absent.size() != header.getTotalInventorySize())
		{
			// Find the duplicate
			for (final Hash item : this.absent)
			{
				InventoryType discoveredType = null;
				for (int i = 0 ; i < types.length ; i++)
				{
					if (header.contains(item, types[i]) != null)
					{
						if (discoveredType != null)
						{
							blocksLog.fatal(this.context.getName()+": Item "+item+" is discovered in inventory types "+discoveredType+" & "+types[i]+" in block "+header);
							throw new IllegalStateException("Non-normalized header, must not contain duplicate references!");
						}
						
						discoveredType = types[i];
					}
				}
			}
		}
		
		int predictedStateCountsToLock = header.getInventorySize(InventoryType.ACCEPTED)*STATE_LOCKS_PER_ATOM_MULTIPLIER;
		int predictedStateCountsToUnlock = header.getInventorySize(InventoryType.COMMITTED)*STATE_LOCKS_PER_ATOM_MULTIPLIER;
		this.statesLocked = Maps.mutable.ofInitialCapacity(predictedStateCountsToLock == 0 ? DEFAULT_STATE_LOCKS_PER_ATOM : predictedStateCountsToLock);
		this.statesUnlocked = Maps.mutable.ofInitialCapacity(predictedStateCountsToUnlock == 0 ? DEFAULT_STATE_LOCKS_PER_ATOM : predictedStateCountsToUnlock);
	}

	PendingBlock(final Context context, final BlockHeader header, final Multimap<InventoryType, Hashable> inventory) throws ValidationException
	{
		this(context, header);
		
		Objects.requireNonNull(inventory, "Inventory is null");

		if (inventory.containsKey(InventoryType.ACCEPTED))    inventory.get(InventoryType.ACCEPTED).forEach(pa    -> 	this.accepted.put(pa.getHash(), PendingAtom.class.cast(pa)));
		if (inventory.containsKey(InventoryType.UNACCEPTED))  inventory.get(InventoryType.UNACCEPTED).forEach(pa  -> 	this.unaccepted.put(pa.getHash(), PendingAtom.class.cast(pa)));
		if (inventory.containsKey(InventoryType.COMMITTED))   inventory.get(InventoryType.COMMITTED).forEach(pa   -> 	this.committed.put(pa.getHash(), PendingAtom.class.cast(pa)));
		if (inventory.containsKey(InventoryType.UNEXECUTED))  inventory.get(InventoryType.UNEXECUTED).forEach(pa  -> 	this.unexecuted.put(pa.getHash(), PendingAtom.class.cast(pa)));
		if (inventory.containsKey(InventoryType.UNCOMMITTED)) inventory.get(InventoryType.UNCOMMITTED).forEach(pa -> 	this.uncommitted.put(pa.getHash(), PendingAtom.class.cast(pa)));
		if (inventory.containsKey(InventoryType.EXECUTABLE))  inventory.get(InventoryType.EXECUTABLE).forEach(pa  -> 	this.executable.put(pa.getHash(), PendingAtom.class.cast(pa)));
		if (inventory.containsKey(InventoryType.LATENT))      inventory.get(InventoryType.LATENT).forEach(pa      -> 	this.latent.put(pa.getHash(), PendingAtom.class.cast(pa)));
		if (inventory.containsKey(InventoryType.PACKAGES))    inventory.get(InventoryType.PACKAGES).forEach(pkg   ->	this.packages.put(pkg.getHash(), PolyglotPackage.class.cast(pkg)));
		
		constructBlock();
	}
	
	PendingBlock(final PendingBlock other) 
	{
		this(Objects.requireNonNull(other, "Pending block is null").context, other.getHeader());

		this.accepted.putAll(other.accepted);
		this.unaccepted.putAll(other.unaccepted);
		this.committed.putAll(other.committed);
		this.unexecuted.putAll(other.unexecuted);
		this.uncommitted.putAll(other.uncommitted);
		this.executable.putAll(other.executable);
		this.latent.putAll(other.latent);
		this.packages.putAll(other.packages);

		if (other.isConstructed() == false)
			throw new IllegalStateException("Can only copy pending blocks via constructor which are constructed");
			
		this.block = other.block;
		this.statesLocked.putAll(other.statesLocked);
		this.statesUnlocked.putAll(other.statesUnlocked);
	}
	
	@Override
	public Hash getHash()
	{
		return this.header.getHash();
	}
	
	public long getHeight()
	{
		return this.header.getHeight();
	}
	
	boolean isUnbranched()
	{
		return this.unbranched;
	}
	
	private static boolean caughtMuchVoteThresholdVeryMath = false;
	void setInBranch(final PendingBranch branch) throws IOException
	{
		synchronized(this)
		{
			if (this.unbranched == false)
				throw new IllegalStateException("Branch already set for "+this);
			
			this.voteThreshold = branch.getVotePowerThreshold(getHeight());
			this.unbranched = false;
			
			if (PendingBlock.caughtMuchVoteThresholdVeryMath == true)
				return;
		}
	}
	
	long getVoteThreshold()
	{
		synchronized(this)
		{
			if (this.unbranched)
				throw new IllegalStateException("Branch not set for "+this);
			
			return this.voteThreshold;
		}
	}

	boolean isApplied()
	{
		return this.applied;
	}
	
	void setApplied()
	{
		if (this.applied)
			return;
		
		this.applied = true;
		
		this.context.getEvents().post(new BlockAppliedEvent(this));
	}

	public Throwable thrown()
	{
		return this.thrown;
	}
	
	public void thrown(Throwable thrown)
	{
		this.thrown = thrown;
	}

	public BlockHeader getHeader()
	{
		return this.header;
	}
			
	public Block getBlock()
	{
		if (this.constructing.get())
			return null;
		
		return this.block;
	}
	
	PendingAtom getLockedBy(final StateAddress address)
	{
		synchronized(this)
		{
			return this.statesLocked.get(address);
		}
	}

	boolean isLocked(final StateAddress address)
	{
		synchronized(this)
		{
			return this.statesLocked.containsKey(address);
		}
	}
	
	boolean isUnlocked(final StateAddress address)
	{
		synchronized(this)
		{
			return this.statesUnlocked.containsKey(address);
		}
	}
	
	void constructBlock() throws ValidationException
	{
		if (this.constructing.compareAndSet(false, true) == false)
			return;

		try
		{
			synchronized(this)
			{
				if (this.block != null)
					throw new IllegalStateException("Block "+this.getHash()+" is already constructed");
		
				final Block block;
				if (this.accepted.size() != this.header.getInventorySize(InventoryType.ACCEPTED))
					throw new ValidationException(getHeader(), "Construction is missing "+(this.header.getInventorySize(InventoryType.ACCEPTED) - this.accepted.size())+" ACCEPTED atoms for "+getHash());
	
				if (this.unaccepted.size() != this.header.getInventorySize(InventoryType.UNACCEPTED))
					throw new ValidationException(getHeader(), "Construction is missing "+(this.header.getInventorySize(InventoryType.UNACCEPTED) - this.unaccepted.size())+" UNACCEPTED atoms for "+getHash());
					
				if (this.committed.size() != this.header.getInventorySize(InventoryType.COMMITTED))
					throw new ValidationException(getHeader(), "Construction is missing "+(this.header.getInventorySize(InventoryType.COMMITTED) - this.committed.size())+" CERTIFICATES for "+getHash());
	
				if (this.unexecuted.size() != this.header.getInventorySize(InventoryType.UNEXECUTED))
					throw new ValidationException(getHeader(), "Construction is missing "+(this.header.getInventorySize(InventoryType.UNEXECUTED) - this.unexecuted.size())+" UNEXECUTED for "+getHash());
	
				if (this.uncommitted.size() != this.header.getInventorySize(InventoryType.UNCOMMITTED))
					throw new ValidationException(getHeader(), "Construction is missing "+(this.header.getInventorySize(InventoryType.UNCOMMITTED) - this.uncommitted.size())+" UNCOMMITTED for "+getHash());
	
				if (this.executable.size() != this.header.getInventorySize(InventoryType.EXECUTABLE))
					throw new ValidationException(getHeader(), "Construction is missing "+(this.header.getInventorySize(InventoryType.EXECUTABLE) - this.executable.size())+" EXECUTION signals for "+getHash());
	
				if (this.latent.size() != this.header.getInventorySize(InventoryType.LATENT))
					throw new ValidationException(getHeader(), "Construction is missing "+(this.header.getInventorySize(InventoryType.LATENT) - this.latent.size())+" LATENT EXECTUTION signals for "+getHash());
				
				if (this.packages.size() != this.header.getInventorySize(InventoryType.PACKAGES))
					throw new ValidationException(getHeader(), "Construction is missing "+(this.header.getInventorySize(InventoryType.PACKAGES) - this.packages.size())+" PACKAGE for "+getHash());
	
				final List<Atom> accepted = new ArrayList<Atom>();
				for (Hash hash : this.header.getInventory(InventoryType.ACCEPTED))
				{
					PendingAtom pendingAtom = this.accepted.get(hash);
					
					if (pendingAtom == null)
						throw new ValidationException(getHeader(), "Atom "+hash+" for ACCEPTED is not in inventory during construction of "+getHash());
					
					accepted.add(pendingAtom.getAtom());
				}
				
				final List<Atom> unaccepted = new ArrayList<Atom>();
				for (Hash hash : this.header.getInventory(InventoryType.UNACCEPTED))
				{
					PendingAtom pendingAtom = this.unaccepted.get(hash);
					
					if (pendingAtom == null)
						throw new ValidationException(getHeader(), "Atom "+hash+" for UNACCEPTED is not in inventory during construction of "+getHash());
					
					unaccepted.add(pendingAtom.getAtom());
				}
	
				Set<Hash> inventory = this.header.getInventory(InventoryType.COMMITTED);
				final List<AtomCertificate> certificates = new ArrayList<AtomCertificate>();
				for (PendingAtom atom : this.committed.values())
				{
					if (atom.getCertificate() == null)
						throw new ValidationException(getHeader(), "Atom "+atom.getHash()+" does not have certificate during construction of "+getHash());
	
					if (inventory.contains(atom.getCertificate().getHash()) == false)
						throw new ValidationException(getHeader(), "Atom certificate "+atom.getCertificate().getHash()+" in atom "+atom.getHash()+" is not in inventory during construction of "+getHash());
	
					certificates.add(atom.getCertificate());
				}
				
				inventory = this.header.getInventory(InventoryType.UNEXECUTED);
				List<ExecutionTimeout> unexecuted = new ArrayList<ExecutionTimeout>();
				for (PendingAtom atom : this.unexecuted.values())
				{
					if (atom.getTimeout() == null || ExecutionTimeout.class.isAssignableFrom(atom.getTimeout().getClass()) == false)
						throw new ValidationException(getHeader(), "Atom "+atom.getHash()+" does not have execution timeout during construction of "+getHash());
	
					if (inventory.contains(atom.getTimeout().getHash()) == false)
						throw new ValidationException(getHeader(), "Execution timeout "+atom.getTimeout().getHash()+" in atom "+atom.getHash()+" is not in inventory during construction of "+getHash());
	
					unexecuted.add(atom.getTimeout());
				}
	
				inventory = this.header.getInventory(InventoryType.UNCOMMITTED);
				List<CommitTimeout> uncommitted = new ArrayList<CommitTimeout>();
				for (PendingAtom atom : this.uncommitted.values())
				{
					if (atom.getTimeout() == null || CommitTimeout.class.isAssignableFrom(atom.getTimeout().getClass()) == false)
						throw new ValidationException(getHeader(), "Atom "+atom.getHash()+" does not have commit timeout during construction of "+getHash());
	
					if (inventory.contains(atom.getTimeout().getHash()) == false)
						throw new ValidationException(getHeader(), "Commit timeout "+atom.getTimeout().getHash()+" in atom "+atom.getHash()+" is not in inventory during construction of "+getHash());
	
					uncommitted.add(atom.getTimeout());
				}
	
				final List<PolyglotPackage> packages = new ArrayList<PolyglotPackage>();
				for (Hash hash : this.header.getInventory(InventoryType.PACKAGES))
				{
					PolyglotPackage pakage = this.packages.get(hash);
					
					if (pakage == null)
						throw new ValidationException(getHeader(), "Package "+hash+" is not in inventory during construction of "+getHash());
					
					packages.add(pakage);
				}
	
				this.accepted.forEach((h, pa) -> pa.forStateAddresses(StateLockMode.WRITE, sa -> PendingBlock.this.statesLocked.put(sa, pa)));
				this.committed.forEach((h, pa) -> pa.forStateAddresses(StateLockMode.WRITE, sa -> PendingBlock.this.statesUnlocked.put(sa, pa)));
				this.unexecuted.forEach((h, pa) -> pa.forStateAddresses(StateLockMode.WRITE, sa -> PendingBlock.this.statesUnlocked.put(sa, pa)));
				this.uncommitted.forEach((h, pa) -> pa.forStateAddresses(StateLockMode.WRITE, sa -> PendingBlock.this.statesUnlocked.put(sa, pa)));
				block = new Block(this.header, accepted, unaccepted, unexecuted, certificates, uncommitted, packages);
	
				this.block = block;
				
				if (blocksLog.hasLevel(Logging.DEBUG))
					blocksLog.debug(this.context.getName()+": Block is constructed "+this.header);
					
				this.context.getEvents().post(new BlockConstructedEvent(this));
			}
		}
		finally
		{
			this.constructing.set(false);
		}
	}
	
	public InventoryType references(final Hash item, final InventoryType ... types) 
	{
		int extendedTestMask = 0;
	    for (int i = 0 ; i < types.length ; i ++) 
	    {
	        switch (types[i]) 
	        {
	            case ACCEPTED, UNACCEPTED, EXECUTABLE, LATENT, PACKAGES:
	            {
	            	InventoryType type = this.header.contains(item, types[i]);
	            	if (type != null)
	            		return type;
	                continue;
	            }
	            default:
	            	extendedTestMask |= types[i].value();
	        }
	    }
		
		// Check inventory types that do not refer to atom types, certificates, timeouts, packages.
		if (extendedTestMask > 0)
		{
			synchronized(this)
			{
				if ((extendedTestMask & InventoryType.COMMITTED.value()) != 0)
				{
					if (this.committed.containsKey(item))
						return InventoryType.COMMITTED;
				}

				if ((extendedTestMask & InventoryType.UNCOMMITTED.value()) != 0)
				{
					if (this.uncommitted.containsKey(item))
						return InventoryType.UNCOMMITTED;
				}

				if ((extendedTestMask & InventoryType.UNEXECUTED.value()) != 0)
				{
					if (this.unexecuted.containsKey(item))
						return InventoryType.UNEXECUTED;
				}
			}
		}
		
		return null;
	}
	
	boolean requires(final Hashable item, final InventoryType type) 
	{
		synchronized(this)
		{
			return this.absent.contains(item.getHash());
		}
	}

	Collection<InventoryType> requires(final Hashable item) 
	{
		List<InventoryType> types = new ArrayList<InventoryType>(0);
		synchronized(this)
		{
			for (InventoryType type : InventoryType.values())
			{
				if (item instanceof PendingAtom pendingAtom)
				{
					if (type.equals(InventoryType.COMMITTED))
					{
						if (pendingAtom.getCertificate() == null || this.absent.contains(pendingAtom.getCertificate().getHash()) == false)
							continue;
					}
					else if (type.equals(InventoryType.ACCEPTED))
					{
						if (pendingAtom.isPrepared() == false || this.absent.contains(pendingAtom.getHash()) == false)
							continue;
					}
					else if (type.equals(InventoryType.UNCOMMITTED))
					{
						if (pendingAtom.getTimeout() == null || this.absent.contains(pendingAtom.getTimeout().getHash()) == false)
							continue;
					}
					else if (type.equals(InventoryType.UNEXECUTED))
					{
						if (pendingAtom.getTimeout() == null || this.absent.contains(pendingAtom.getTimeout().getHash()) == false)
							continue;
					}
					else if (type.equals(InventoryType.UNACCEPTED))
					{
						if (pendingAtom.getTimeout() == null || this.absent.contains(pendingAtom.getTimeout().getHash()) == false)
							continue;
					}
					else if (this.absent.contains(item.getHash()) == false)
						continue;
				}
				else if (item instanceof PolyglotPackage)
				{
					if (this.absent.contains(item.getHash()) == false)
						continue;
				}
				else 
					throw new IllegalArgumentException("Unsupported hashable type "+item.getClass()+":"+item.getHash());
				
				types.add(type);
			}
		}

		return types;
	}

	/**
	 * Puts the item into the pending block according the to the type specified 
	 * 
	 * @param type
	 * @param atom
	 * 
	 * @return true if pending atom was applied to type, otherwise false
	 */
	<T> boolean put(final T item, final InventoryType type)
	{
		Objects.requireNonNull(type, "Inventory type is null");
		Objects.requireNonNull(item, "Pending item is null");
		synchronized(this)
		{
			if (type == InventoryType.ACCEPTED)
				return putAccepted((PendingAtom) item);
			else if (type == InventoryType.UNACCEPTED)
				return putUnaccepted((PendingAtom) item);
			else if (type == InventoryType.COMMITTED)
				return putCommitted((PendingAtom) item);
			else if (type == InventoryType.EXECUTABLE)
				return putExecutable((PendingAtom) item);
			else if (type == InventoryType.LATENT)
				return putLatent((PendingAtom) item);
			else if (type == InventoryType.UNEXECUTED)
				return putUnexecuted((PendingAtom) item);
			else if (type == InventoryType.UNCOMMITTED)
				return putUncommitted((PendingAtom) item);
			else if (type == InventoryType.PACKAGES)
				return putPackage((PolyglotPackage) item);
			
			return false;
		}
	}

	private boolean putAccepted(final PendingAtom pendingAtom)
	{
		Objects.requireNonNull(pendingAtom, "Pending atom is null");
		
		if (pendingAtom.getStatus().before(State.PREPARED))
			throw new IllegalStateException("Pending atom "+pendingAtom.getHash()+" is not PREPARED but "+pendingAtom.getStatus());

		synchronized(this)
		{
			if (this.accepted.containsKey(pendingAtom.getHash()) == false)
			{
				if (this.absent.remove(pendingAtom.getHash()) == false)
					throw new IllegalStateException("Expected to find ACCEPTED "+pendingAtom.getHash()+" as absent");

				this.accepted.put(pendingAtom.getHash(), pendingAtom);
				return true;
			}
			
			return false;
		}
	}
	
	private boolean putUnaccepted(final PendingAtom pendingAtom)
	{
		Objects.requireNonNull(pendingAtom, "Pending atom is null");
		
		if (pendingAtom.getStatus().before(State.PREPARED))
			throw new IllegalStateException("Pending atom "+pendingAtom.getHash()+" is not PREPARED but "+pendingAtom.getStatus());

		synchronized(this)
		{
			if (this.unaccepted.containsKey(pendingAtom.getHash()) == false)
			{
				if (this.absent.remove(pendingAtom.getHash()) == false)
					throw new IllegalStateException("Expected to find UNACCEPTED "+pendingAtom.getHash()+" as absent");

				this.unaccepted.put(pendingAtom.getHash(), pendingAtom);
				return true;
			}
			
			return false;
		}
	}

	private boolean putExecutable(final PendingAtom pendingAtom)
	{
		Objects.requireNonNull(pendingAtom, "Pending atom is null");
		
		// TODO should this be PREPARED and not ACCEPTED?
		if (pendingAtom.getStatus().before(State.PREPARED))
			throw new IllegalStateException("Pending atom "+pendingAtom.getHash()+" is not PREPARED but "+pendingAtom.getStatus());

		synchronized(this)
		{
			if (this.executable.containsKey(pendingAtom.getHash()) == false)
			{
				if (this.absent.remove(pendingAtom.getHash()) == false)
					throw new IllegalStateException("Expected to find EXECUTABLE "+pendingAtom.getHash()+" as absent");

				this.executable.put(pendingAtom.getHash(), pendingAtom);
				return true;
			}
			
			return false;
		}
	}

	private boolean putLatent(final PendingAtom pendingAtom)
	{
		Objects.requireNonNull(pendingAtom, "Pending atom is null");
		
		if (pendingAtom.getStatus().before(State.PREPARED))
			throw new IllegalStateException("Pending atom "+pendingAtom.getHash()+" is not PREPARED but "+pendingAtom.getStatus());

		synchronized(this)
		{
			if (this.latent.containsKey(pendingAtom.getHash()) == false)
			{
				if (this.absent.remove(pendingAtom.getHash()) == false)
					throw new IllegalStateException("Expected to find LATENT "+pendingAtom.getHash()+" as absent");

				this.latent.put(pendingAtom.getHash(), pendingAtom);
				return true;
			}
			
			return false;
		}
	}

	private boolean putUnexecuted(final PendingAtom pendingAtom)
	{
		Objects.requireNonNull(pendingAtom, "Pending atom is null");
		
		if (pendingAtom.getStatus().before(State.PREPARED))
			throw new IllegalStateException("Pending atom "+pendingAtom.getHash()+" is not PREPARED but "+pendingAtom.getStatus());

		synchronized(this)
		{
			ExecutionTimeout timeout = pendingAtom.getTimeout();
			if (timeout == null || ExecutionTimeout.class.isAssignableFrom(timeout.getClass()) == false)
				return false;
			
			if (this.unexecuted.containsKey(pendingAtom.getHash()) == false)
			{
				if (this.absent.remove(timeout.getHash()) == false)
					throw new IllegalStateException("Expected to find UNEXECUTED "+timeout.getHash()+" as absent");

				this.unexecuted.put(pendingAtom.getHash(), pendingAtom);
				return true;
			}
			
			return false;
		}
	}

	private boolean putCommitted(final PendingAtom pendingAtom)
	{
		Objects.requireNonNull(pendingAtom, "Pending atom is null");
		Objects.requireNonNull(pendingAtom.getCertificate(), "Pending atom certificate is null");
		
		if (pendingAtom.getStatus().before(State.PREPARED))
			throw new IllegalStateException("Pending atom "+pendingAtom.getHash()+" is not PREPARED but "+pendingAtom.getStatus());

		synchronized(this)
		{
			if (this.committed.containsKey(pendingAtom.getHash()) == false)
			{
				if (this.absent.remove(pendingAtom.getCertificate().getHash()) == false)
					throw new IllegalStateException("Expected to find "+pendingAtom.getCertificate().getHash()+" as absent");
				
				this.committed.put(pendingAtom.getHash(), pendingAtom);
				return true;
			}
			
			return false;
		}
	}
	
	private boolean putUncommitted(final PendingAtom pendingAtom)
	{
		Objects.requireNonNull(pendingAtom, "Pending atom is null");
		
		if (pendingAtom.getStatus().before(State.PREPARED))
			throw new IllegalStateException("Pending atom "+pendingAtom.getHash()+" is not PREPARED but "+pendingAtom.getStatus());

		synchronized(this)
		{
			CommitTimeout timeout = pendingAtom.getTimeout();
			if (timeout == null || CommitTimeout.class.isAssignableFrom(timeout.getClass()) == false)
				return false;

			if (this.uncommitted.containsKey(pendingAtom.getHash()) == false)
			{
				if (this.absent.remove(timeout.getHash()) == false)
					throw new IllegalStateException("Expected to find UNCOMMITTED "+timeout.getHash()+" as absent");

				this.uncommitted.put(pendingAtom.getHash(), pendingAtom);
				return true;
			}
			
			return false;
		}
	}
	
	private boolean putPackage(final PolyglotPackage pakage)
	{
		Objects.requireNonNull(pakage, "Package is null");
		
		synchronized(this)
		{
			if (this.packages.containsKey(pakage.getHash()) == false)
			{
				if (this.absent.remove(pakage.getHash()) == false)
					throw new IllegalStateException("Expected to find PACKAGE "+pakage.getHash()+" as absent");

				this.packages.put(pakage.getHash(), pakage);
				return true;
			}
			
			return false;
		}
	}

	boolean isConstructable()
	{
		synchronized(this)
		{
			return this.absent.isEmpty();
		}
	}
	
	boolean isConstructing()
	{
		return this.constructing.get();
	}
	
	boolean isConstructed()
	{
		if (this.constructing.get())
			return false;
		
		return this.block != null ? true : false;
	}

	List<Hash> getAbsent(final InventoryType type)
	{
		Objects.requireNonNull(type, "Inventory type is null");

		synchronized(this)
		{
			List<Hash> absent = new ArrayList<Hash>(this.header.getInventorySize(type));
			for(Hash item : this.header.getInventory(type))
			{
				if (this.absent.contains(item) == true)
					absent.add(item);
			}

			return Collections.unmodifiableList(absent);
		}
	}
	
	boolean contains(final Hash item, final InventoryType type)
	{
		Objects.requireNonNull(type, "Inventory type is null");
		Objects.requireNonNull(item, "Inventory item hash is null");
		Hash.notZero(item, "Inventory item hash is ZERO");
		
		synchronized(this)
		{
			switch(type)
			{
			case ACCEPTED: 
				return this.accepted.containsKey(item);
			case COMMITTED:
				return this.committed.containsKey(item);
			case EXECUTABLE:
				return this.executable.containsKey(item);
			case LATENT:
				return this.latent.containsKey(item);
			case UNACCEPTED:
				return this.unaccepted.containsKey(item);
			case UNCOMMITTED:
				return this.uncommitted.containsKey(item);
			case UNEXECUTED:
				return this.unexecuted.containsKey(item);
			case PACKAGES:
				return this.packages.containsKey(item);
			default:
				throw new IllegalArgumentException("Inventory type "+type+" is not supported");
			}
		}
	}

	
	boolean contains(final Hash item, final InventoryType ... types)
	{
		Objects.requireNonNull(types, "Inventory types is null");
		Numbers.isZero(types.length, "Inventory types is empty");
		Objects.requireNonNull(item, "Inventory item hash is null");
		Hash.notZero(item, "Inventory item hash is ZERO");
		
		synchronized(this)
		{
			boolean contains = false;
			
			for (int i = 0 ; i < types.length ; i++)
			{
				final InventoryType type = types[i];
				switch(type)
				{
				case ACCEPTED: 
					contains = this.accepted.containsKey(item); break;
				case COMMITTED:
					contains = this.committed.containsKey(item); break;
				case EXECUTABLE:
					contains = this.executable.containsKey(item); break;
				case LATENT:
					contains = this.latent.containsKey(item); break;
				case UNACCEPTED:
					contains = this.unaccepted.containsKey(item); break;
				case UNCOMMITTED:
					contains = this.uncommitted.containsKey(item); break;
				case UNEXECUTED:
					contains = this.unexecuted.containsKey(item); break;
				case PACKAGES:
					contains = this.packages.containsKey(item); break;
				default:
					throw new IllegalArgumentException("Inventory type "+type+" is not supported");
				}
				
				if (contains)
					return true;
			}
			
			return false;
		}
	}

	boolean provisioned(final Hash item, final InventoryType type)
	{
		Objects.requireNonNull(type, "Inventory type is null");
		Objects.requireNonNull(item, "Inventory item hash is null");
		Hash.notZero(item, "Inventory item hash is ZERO");
		
		synchronized(this)
		{
			switch(type)
			{
			case ACCEPTED: 
				return this.accepted.containsKey(item);
			case COMMITTED:
				return this.committed.containsKey(item);
			case EXECUTABLE:
				return this.executable.containsKey(item);
			case LATENT:
				return this.latent.containsKey(item);
			case UNACCEPTED:
				return this.unaccepted.containsKey(item);
			case UNCOMMITTED:
				return this.uncommitted.containsKey(item);
			case UNEXECUTED:
				return this.unexecuted.containsKey(item);
			case PACKAGES:
				return this.packages.containsKey(item);
			default:
				throw new IllegalArgumentException("Inventory type "+type+" is not supported");
			}
		}
	}
	
	void forInventory(final InventoryType type, final Consumer<PendingAtom> consumer)
	{
		Objects.requireNonNull(type, "Inventory type is null");

		synchronized(this)
		{
			switch(type)
			{
			case ACCEPTED:
				this.accepted.forEach((h, pa) -> consumer.accept(pa)); break;
			case COMMITTED:
				this.committed.forEach((h, pa) -> consumer.accept(pa)); break;
			case EXECUTABLE:
				this.executable.forEach((h, pa) -> consumer.accept(pa)); break;
			case UNACCEPTED:
				this.unaccepted.forEach((h, pa) -> consumer.accept(pa)); break;
			case LATENT:
				this.latent.forEach((h, pa) -> consumer.accept(pa)); break;
			case UNCOMMITTED:
				this.uncommitted.forEach((h, pa) -> consumer.accept(pa)); break;
			case UNEXECUTED:
				this.unexecuted.forEach((h, pa) -> consumer.accept(pa)); break;
			default:
				throw new IllegalArgumentException("Inventory type "+type+" is not supported");
			}
		}
	}

	@SuppressWarnings("unchecked")
	<T> List<T> get(final InventoryType type)
	{
		Objects.requireNonNull(type, "Inventory type is null");

		synchronized(this)
		{
			switch(type)
			{
			case ACCEPTED:
				return (List<T>) (this.accepted.isEmpty() ? Collections.emptyList() : Collections.unmodifiableList(new ArrayList<>(this.accepted.values())));
			case UNACCEPTED:
				return (List<T>) (this.unaccepted.isEmpty() ? Collections.emptyList() : Collections.unmodifiableList(new ArrayList<>(this.unaccepted.values())));
			case COMMITTED:
				return (List<T>) (this.committed.isEmpty() ? Collections.emptyList() : Collections.unmodifiableList(new ArrayList<>(this.committed.values())));
			case EXECUTABLE:
				return (List<T>) (this.executable.isEmpty() ? Collections.emptyList() : Collections.unmodifiableList(new ArrayList<>(this.executable.values())));
			case LATENT:
				return (List<T>) (this.latent.isEmpty() ? Collections.emptyList() : Collections.unmodifiableList(new ArrayList<>(this.latent.values())));
			case UNCOMMITTED:
				return (List<T>) (this.uncommitted.isEmpty() ? Collections.emptyList() : Collections.unmodifiableList(new ArrayList<>(this.uncommitted.values())));
			case UNEXECUTED:
				return (List<T>) (this.unexecuted.isEmpty() ? Collections.emptyList() : Collections.unmodifiableList(new ArrayList<>(this.unexecuted.values())));
			case PACKAGES:
				return (List<T>) (this.packages.isEmpty() ? Collections.emptyList() : Collections.unmodifiableList(new ArrayList<>(this.packages.values())));
			default:
				throw new IllegalArgumentException("Inventory type "+type+" is not supported");
			}
		}
	}
	
	@SuppressWarnings("unchecked")
	<T> T get(final InventoryType type, final Hash item)
	{
		Objects.requireNonNull(type, "Inventory type is null");
		Objects.requireNonNull(item, "Inventory item hash is null");
		Hash.notZero(item, "Inventory item hash is ZERO");

		synchronized(this)
		{
			switch(type)
			{
			case ACCEPTED:
				return (T) this.accepted.get(item);
			case UNACCEPTED:
				return (T) this.unaccepted.get(item);
			case COMMITTED:
				return (T) this.committed.get(item);
			case EXECUTABLE:
				return (T) this.executable.get(item);
			case LATENT:
				return (T) this.latent.get(item);
			case UNCOMMITTED:
				return (T) this.uncommitted.get(item);
			case UNEXECUTED:
				return (T) this.unexecuted.get(item);
			case PACKAGES:
				return (T) this.packages.get(item);
			default:
				throw new IllegalArgumentException("Inventory type "+type+" is not supported");
			}
		}
	}

	@Override
	public int hashCode()
	{
		return this.header.getHash().hashCode();
	}

	@Override
	public boolean equals(final Object object)
	{
		if (object == null)
			return false;

		if (object == this)
			return true;

		if (object instanceof PendingBlock pendingBlock)
		{
			if (pendingBlock.getHeader().getHash().equals(this.header.getHash()))
				return true;
		}
		
		return false;
	}

	@Override
	public String toString()
	{
		return this.header.getHeight()+" "+this.header.getHash()+" "+this.header.getPrevious()+" @ "+this.witnessedAt;
	}
	
	public long getWitnessedAt()
	{
		return this.witnessedAt;
	}
	
	boolean voted(final BLSPublicKey identity)
	{
		Objects.requireNonNull(identity, "Public key is null");
		
		synchronized(this)
		{
			return this.votes.containsKey(identity);
		}
	}
	
	BlockCertificate buildCertificate() throws CryptoException
	{
		synchronized(this)
		{
			if (this.header.getCertificate() != null)
			{
				blocksLog.warn(this.context.getName()+": Block header already has a certificate "+this.header);
				return this.header.getCertificate();
			}
			
			if (this.voteWeight < this.voteThreshold)
				throw new IllegalStateException("Can not build a certificate when vote weight of "+this.voteWeight+" is less than vote threshold "+this.voteThreshold);
			
			final Bloom signers = new Bloom(0.000001, this.votes.size());
			final List<BLSPublicKey> keys = new ArrayList<>(this.votes.size());
			final List<BLSSignature> signatures = new ArrayList<>(this.votes.size());
			
			this.votes.forEach((k, s) -> {
				keys.add(k);
				signers.add(k.getIdentity().toByteArray());
				signatures.add(s);
			});

			final BLSPublicKey key = BLS12381.aggregatePublicKey(keys);
			final BLSSignature signature = BLS12381.aggregateSignatures(signatures);
			final BlockCertificate certificate = new BlockCertificate(this.header.getHash(), signers, key, signature);
			this.header.setCertificate(certificate);
			return certificate;
		}
	}

	boolean vote(final BlockVote vote, final long voteWeight) throws ValidationException
	{
		Objects.requireNonNull(vote, "Block vote is null");
		
		try
		{
			if (vote.getObject().equals(getHash()) == false)
				throw new ValidationException(vote, "Vote from "+vote.getOwner()+" is not for "+getHash());
			
			synchronized(this)
			{
				if (this.votes.containsKey(vote.getOwner()) == false)
				{
					this.votes.put(vote.getOwner(), vote.getSignature());
					this.voteWeight += voteWeight;
					return true;
				}
				else
				{
					blocksLog.warn(this.context.getName()+": "+vote.getOwner()+" has already cast a vote for "+this.header.getHash());
					return false;
				}
			}
		}
		catch (ValidationException ex)
		{
			throw new ValidationException(getHeader(), ex);
		}
	}

	public long getVoteWeight() 
	{
		synchronized(this)
		{
			return this.voteWeight;
		}
	}

	/**
	 * Checks if two pending blocks intersect with each other.
	 * 
	 * TODO needs to be more thorough to check cross-references such as unaccepted / accepted
	 * 
	 * @param other
	 * 
	 * @return
	 */
	boolean intersects(final PendingBlock other) 
	{
		synchronized(this)
		{
			synchronized(other)
			{
				for (Hash hash : this.accepted.keySet()) if (other.accepted.containsKey(hash)) return true;
				for (Hash hash : this.unaccepted.keySet()) if (other.unaccepted.containsKey(hash)) return true;
				for (Hash hash : this.committed.keySet()) if (other.committed.containsKey(hash)) return true;
				for (Hash hash : this.executable.keySet()) if (other.executable.containsKey(hash)) return true;
				for (Hash hash : this.latent.keySet()) if (other.latent.containsKey(hash)) return true;
				for (Hash hash : this.packages.keySet()) if (other.packages.containsKey(hash)) return true;
				for (Hash hash : this.uncommitted.keySet()) if (other.uncommitted.containsKey(hash)) return true;
				for (Hash hash : this.unexecuted.keySet()) if (other.unexecuted.containsKey(hash)) return true;
				
				return false;
			}
		}
	}
}
