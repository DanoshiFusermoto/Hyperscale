package org.radix.hyperscale.ledger;

import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.eclipse.collections.api.map.primitive.MutableObjectLongMap;
import org.eclipse.collections.impl.factory.primitive.ObjectLongMaps;
import org.radix.hyperscale.common.Primitive;
import org.radix.hyperscale.crypto.CryptoException;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.crypto.Hashable;
import org.radix.hyperscale.crypto.Identity;
import org.radix.hyperscale.crypto.MerkleProof;
import org.radix.hyperscale.crypto.MerkleTree;
import org.radix.hyperscale.crypto.MerkleProof.Branch;
import org.radix.hyperscale.crypto.bls12381.BLSKeyPair;
import org.radix.hyperscale.crypto.bls12381.BLSPublicKey;
import org.radix.hyperscale.crypto.bls12381.BLSSignature;
import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;
import org.radix.hyperscale.network.TransportParameters;
import org.radix.hyperscale.serialization.DsonCached;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.Serializable;
import org.radix.hyperscale.serialization.Serialization;
import org.radix.hyperscale.serialization.SerializationException;
import org.radix.hyperscale.serialization.SerializerId2;
import org.radix.hyperscale.serialization.DsonOutput.Output;
import org.radix.hyperscale.utils.Longs;
import org.radix.hyperscale.utils.Numbers;
import org.radix.hyperscale.utils.UInt256;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.annotations.VisibleForTesting;

@SerializerId2("ledger.block.header")
@StateContext("block.header")
@TransportParameters(urgent = true, weight = 25)
@DsonCached
public final class BlockHeader extends Serializable implements Comparable<BlockHeader>, Hashable, Primitive
{
	public static final UInt256 MAX_WORK = UInt256.from(new byte[]{(byte)0xff, (byte)0xff, (byte)0xff, (byte)0xff, (byte)0xff, (byte)0xff, (byte)0xff, (byte)0xff});
	public static final int	MAX_PACKAGES = 16;
	public static final int	MAX_INVENTORY_TYPE_PRIMITIVES = 4096;
	
	public static enum InventoryType
	{

		ACCEPTED(1), COMMITTED(2), EXECUTABLE(4), LATENT(8), PACKAGES(128), UNACCEPTED(16), UNCOMMITTED(32), UNEXECUTED(64);
		
		private final int value;
		
		InventoryType(int value)
		{
			this.value = value;
		}
		
		public int value()
		{
			return this.value;
		}
		
		@JsonValue
		@Override
		public String toString() 
		{
			return this.name();
		}
	}
	
	public static final InventoryType[] ACCEPT_SELECT_FILTER = {InventoryType.ACCEPTED, InventoryType.UNACCEPTED};
	public static final InventoryType[] ACCEPT_VERIFY_FILTER = {InventoryType.ACCEPTED, InventoryType.UNACCEPTED};
	public static final InventoryType[] UNACCEPT_SELECT_FILTER = {InventoryType.ACCEPTED, InventoryType.UNACCEPTED};
	public static final InventoryType[] UNACCEPT_VERIFY_FILTER = {InventoryType.ACCEPTED, InventoryType.UNACCEPTED};
	public static final InventoryType[] EXECUTE_SELECT_FILTER = {InventoryType.EXECUTABLE, InventoryType.UNACCEPTED, InventoryType.COMMITTED, InventoryType.UNEXECUTED, InventoryType.UNCOMMITTED};
	public static final InventoryType[] EXECUTE_VERIFY_FILTER = {InventoryType.EXECUTABLE, InventoryType.UNEXECUTED, InventoryType.UNACCEPTED};
	public static final InventoryType[] LATENT_SELECT_FILTER = {InventoryType.EXECUTABLE, InventoryType.LATENT, InventoryType.UNACCEPTED, InventoryType.COMMITTED, InventoryType.UNEXECUTED, InventoryType.UNCOMMITTED};
	public static final InventoryType[] LATENT_VERIFY_FILTER = {InventoryType.UNACCEPTED, InventoryType.EXECUTABLE, InventoryType.UNEXECUTED, InventoryType.LATENT};
	public static final InventoryType[] UNEXECUTED_SELECT_FILTER = {InventoryType.EXECUTABLE, InventoryType.UNACCEPTED, InventoryType.COMMITTED, InventoryType.UNCOMMITTED, InventoryType.UNEXECUTED};
	public static final InventoryType[] UNEXECUTED_VERIFY_FILTER = {InventoryType.UNACCEPTED, InventoryType.EXECUTABLE, InventoryType.UNEXECUTED};
	public static final InventoryType[] COMMIT_SELECT_FILTER = {InventoryType.UNACCEPTED, InventoryType.UNEXECUTED, InventoryType.COMMITTED, InventoryType.UNCOMMITTED};
	public static final InventoryType[] COMMIT_VERIFY_FILTER = {InventoryType.UNACCEPTED, InventoryType.UNCOMMITTED, InventoryType.UNEXECUTED, InventoryType.COMMITTED};
	public static final InventoryType[] UNCOMMITTED_SELECT_FILTER = {InventoryType.UNACCEPTED, InventoryType.COMMITTED, InventoryType.UNEXECUTED, InventoryType.UNCOMMITTED};
	public static final InventoryType[] UNCOMMITTED_VERIFY_FILTER = {InventoryType.UNACCEPTED, InventoryType.UNEXECUTED, InventoryType.COMMITTED, InventoryType.UNCOMMITTED};


	private static final Logger blocksLog = Logging.getLogger("blocks");

	@JsonProperty("height")
	@DsonOutput(Output.ALL)
	private long height;

	@JsonProperty("index")
	@DsonOutput(Output.ALL)
	private long index;

	@JsonProperty("previous")
	@DsonOutput(Output.ALL)
	private Hash previous;

	@JsonProperty("difficulty")
	@DsonOutput(Output.ALL)
	private long difficulty;

	@JsonProperty("nonce")
	@DsonOutput(Output.ALL)
	private long nonce;

	@JsonProperty("work")
	@DsonOutput(Output.ALL)
	private UInt256 previousWork;

	@JsonProperty("timestamp")
	@DsonOutput(Output.ALL)
	private long timestamp;

	@JsonProperty("proposer")
	@DsonOutput(Output.ALL)
	private Identity proposer;

	// TODO inventory of atoms, certificates etc, inefficient, find a better method
	// 	    not included in hash as covered by merkle
	@JsonProperty("inventory")
	@JsonInclude(JsonInclude.Include.NON_NULL)
	@DsonOutput(value = {Output.API, Output.WIRE, Output.PERSIST})
	@JsonDeserialize(as = LinkedHashMap.class)
	private Map<InventoryType, LinkedHashSet<Hash>> inventory;

	@JsonProperty("inventory_merkle")
	@DsonOutput(Output.ALL)
	private Hash inventoryMerkle;

	@JsonProperty("view")
	@DsonOutput(Output.ALL)
	private QuorumCertificate view;

	@JsonProperty("signature")
	@DsonOutput(value = {Output.API, Output.WIRE, Output.PERSIST})
	private BLSSignature signature;
	
	private final transient MutableObjectLongMap<Hash> indexes;
	
	private transient UInt256 localWork;
	private transient UInt256 totalWork;
	
	private transient long nextIndex;
	private transient Hash hash;
	private transient int  size = -1;
	
	private static LinkedHashSet<Hash> EMPTY_INVENTORY_SET = new LinkedHashSet<Hash>(0, 1.0f);
	
	BlockHeader()
	{
		super();

		this.indexes = ObjectLongMaps.mutable.empty();
	}
	
	BlockHeader(final long height, final Hash previous, final long difficulty, final UInt256 previousWork, final long nonce, final long index, 
			    final Map<InventoryType, Collection<Hash>> inventory, final long timestamp, final Identity proposer, final QuorumCertificate view)
	{
		super();
		
		Numbers.isNegative(height, "Height is negative");
		Numbers.isNegative(index, "Index is negative");
		Numbers.isNegative(timestamp, "Timestamp is negative");
		Numbers.isNegative(difficulty, "Difficulty is negative");
		Objects.requireNonNull(inventory, "Inventory is null");
		Objects.requireNonNull(previous, "Previous proposal is null");
		Objects.requireNonNull(view, "Quorum certificate is null");
		
		if (height == 0 && previous.equals(Hash.ZERO) == false)
			throw new IllegalArgumentException("Previous proposal hash must be ZERO for genesis");
		
		if (height != 0)
			Hash.notZero(previous, "Previous block hash is ZERO");
		
		Objects.requireNonNull(proposer, "Proposer is null");
		if (proposer.getPrefix() != Identity.BLS)
			throw new IllegalArgumentException("Proposer is not a BLS identity");
		
		this.proposer = proposer;
		this.difficulty = difficulty;
		this.previousWork = previousWork;
		this.previous = previous;
		this.height = height;
		this.index = index;
		this.timestamp = timestamp;
		this.nonce = nonce;
		
		// TODO needs validation?
		this.view = view;

		this.inventory = new EnumMap<InventoryType, LinkedHashSet<Hash>>(InventoryType.class);
		inventory.forEach((type, items) -> BlockHeader.this.inventory.put(type, items.isEmpty() ? EMPTY_INVENTORY_SET : new LinkedHashSet<Hash>(items)));
		for(InventoryType type : InventoryType.values())
		{
			if (this.inventory.containsKey(type))
				continue;
			
			this.inventory.put(type, EMPTY_INVENTORY_SET);
		}
		
		final MerkleTree inventoryMerkleTree = new MerkleTree();
		this.inventory.getOrDefault(InventoryType.ACCEPTED, EMPTY_INVENTORY_SET).forEach(a -> inventoryMerkleTree.appendLeaf(a));
		this.inventory.getOrDefault(InventoryType.UNACCEPTED, EMPTY_INVENTORY_SET).forEach(t -> inventoryMerkleTree.appendLeaf(t));
		this.inventory.getOrDefault(InventoryType.EXECUTABLE, EMPTY_INVENTORY_SET).forEach(e -> inventoryMerkleTree.appendLeaf(e));
		this.inventory.getOrDefault(InventoryType.LATENT, EMPTY_INVENTORY_SET).forEach(l -> inventoryMerkleTree.appendLeaf(l));
		this.inventory.getOrDefault(InventoryType.UNEXECUTED, EMPTY_INVENTORY_SET).forEach(t -> inventoryMerkleTree.appendLeaf(t));
		this.inventory.getOrDefault(InventoryType.COMMITTED, EMPTY_INVENTORY_SET).forEach(c -> inventoryMerkleTree.appendLeaf(c));
		this.inventory.getOrDefault(InventoryType.UNCOMMITTED, EMPTY_INVENTORY_SET).forEach(t -> inventoryMerkleTree.appendLeaf(t));
		this.inventory.getOrDefault(InventoryType.PACKAGES, EMPTY_INVENTORY_SET).forEach(t -> inventoryMerkleTree.appendLeaf(t));
		
		if (inventoryMerkleTree.isEmpty() == false)
			this.inventoryMerkle = inventoryMerkleTree.buildTree();
		else
			this.inventoryMerkle = Hash.ZERO;
		
		this.indexes = ObjectLongMaps.mutable.ofInitialCapacity(getTotalInventorySize());
		
		onDeserialized();
	}
	
	@Override
	public void onDeserialized() 
	{
		long index = this.index;
		Set<Hash> inventory;
		inventory = this.inventory.getOrDefault(InventoryType.ACCEPTED, EMPTY_INVENTORY_SET); for (Hash item : inventory) this.indexes.put(item, index++);
		inventory = this.inventory.getOrDefault(InventoryType.COMMITTED, EMPTY_INVENTORY_SET); for (Hash item : inventory) this.indexes.put(item, index++);
		inventory = this.inventory.getOrDefault(InventoryType.UNACCEPTED, EMPTY_INVENTORY_SET); for (Hash item : inventory) this.indexes.put(item, index++);
		inventory = this.inventory.getOrDefault(InventoryType.UNEXECUTED, EMPTY_INVENTORY_SET); for (Hash item : inventory) this.indexes.put(item, index++);
		inventory = this.inventory.getOrDefault(InventoryType.UNCOMMITTED, EMPTY_INVENTORY_SET); for (Hash item : inventory) this.indexes.put(item, index++);
		inventory = this.inventory.getOrDefault(InventoryType.PACKAGES, EMPTY_INVENTORY_SET); for (Hash item : inventory) this.indexes.put(item, index++);
		this.nextIndex = index;
		
		calculateWork();
	
		super.onDeserialized();
	}
	
	@Override
	public boolean isDeferredPersist() 
	{
		return true;
	}

	public long getHeight() 
	{
		return this.height;
	}

	public long getNonce() 
	{
		return this.nonce;
	}

	synchronized void incrementNonce() 
	{
		this.nonce++;
		this.hash = null;
	}

	public long getIndex() 
	{
		return this.index;
	}

	@VisibleForTesting
	public long getNextIndex() 
	{
		return this.nextIndex;
	}

	public long getIndexOf(final Hash hash)
	{
		Objects.requireNonNull(hash, "Hash is null");
		Hash.notZero(hash, "Hash is ZERO");
		
		long index = this.indexes.getIfAbsent(hash, -1l);
		if (index == -1)
			blocksLog.warn("Index for "+hash+" not found");
		
		return index;
	}

	public long getTimestamp() 
	{
		return this.timestamp;
	}
	
	public long getDifficulty()
	{
		return this.difficulty;
	}
	
	private void calculateWork()
	{
		long blockWork = Longs.fromByteArray(getHash().toByteArray(), Hash.BYTES - Long.BYTES);
		UInt256 blockWorkUInt = UInt256.from(blockWork);
		this.localWork = MAX_WORK.subtract(blockWorkUInt);
		this.totalWork = this.previousWork.add(this.localWork);
	}

	public UInt256 getLocalWork()
	{
		return this.localWork;
	}

	public UInt256 getTotalWork()
	{
		return this.totalWork;
	}

	public long getAverageWork()
	{
		if (this.height == 0)
			return 0;
		
		UInt256 work = getTotalWork();
		UInt256	averageWork = work.divide(UInt256.from(this.height+1));
		return averageWork.getLow();
	}

	@Override
	@JsonProperty("hash")
	@DsonOutput(value = Output.HASH, include = false)
	public synchronized Hash getHash()
	{
		if (this.hash == null)
			this.hash = computeHash();
		
		if (this.hash == null)
			throw new NullPointerException("Block hash is null");

		return this.hash;
	}

	protected synchronized Hash computeHash()
	{
		try
		{
			byte[] contentBytes = Serialization.getInstance().toDson(this, Output.HASH);
			byte[] hashBytes = new byte[Hash.BYTES];
			System.arraycopy(Longs.toByteArray(getHeight()), 0, hashBytes, 0, Long.BYTES);
			System.arraycopy(Hash.hash(contentBytes).toByteArray(), 0, hashBytes, Long.BYTES, Hash.BYTES - Long.BYTES);
			return Hash.from(hashBytes);
		}
		catch (Exception e)
		{
			throw new RuntimeException("Error generating hash: " + e, e);
		}
	}

	@JsonProperty("hash")
	void setHash(final Hash hash)
	{
		Objects.requireNonNull(hash, "Hash is null");
		Hash.notZero(hash, "Hash is ZERO");
		this.hash = hash;
	}

	public Hash getInventoryMerkle() 
	{
		return this.inventoryMerkle;
	}
	
	public InventoryType contains(final Hash item, final InventoryType type) 
	{
		if (this.inventory.get(type).contains(item))
			return type;
		
		return null;
	}

	public InventoryType contains(final Hash item, final InventoryType ... types) 
	{
		for (int i = 0 ; i < types.length ; i++)
			if (this.inventory.get(types[i]).contains(item))
				return types[i];
		
		return null;
	}

	public Set<Hash> getInventory(final InventoryType type)
	{
		Objects.requireNonNull(type, "Inventory type is null");
		
		Set<Hash> inventory = this.inventory.get(type);
		if (inventory == null)
			return Collections.emptySet();
		
		return Collections.unmodifiableSet(inventory);
	}
	
	public int getInventorySize(final InventoryType type)
	{
		Objects.requireNonNull(type, "Inventory type is null");

		Set<Hash> inventory = this.inventory.get(type);
		if (inventory == null)
			return 0;
		
		return inventory.size();
	}

	public int getTotalInventorySize()
	{
		int size = 0;
		InventoryType[] inventoryTypes = InventoryType.values();
		for (int i = 0 ; i < inventoryTypes.length ; i++)
		{
			InventoryType inventoryType = inventoryTypes[i];
			size += this.inventory.getOrDefault(inventoryType, EMPTY_INVENTORY_SET).size();
		}
		
		return size;
	}

	public Hash getPrevious() 
	{
		return this.previous;
	}
	
	public final QuorumCertificate getView()
	{
		return this.view;
	}

	@Override
	public int hashCode() 
	{
		return this.hash.hashCode();
	}

	@Override
	public boolean equals(final Object other) 
	{
		if (other == null)
			return false;
		
		if (other == this)
			return true;

		if (other instanceof BlockHeader header)
		{
			if (header.getHash().equals(getHash()))
				return true;
		}
		
		return false;
	}

	@Override
	public String toString() 
	{
		return this.height+" "+getHash()+" "+this.previous+" "+this.difficulty+" "+getTotalWork()+" - "+getTotalWork()+" "+this.inventoryMerkle+" "+this.timestamp;
	}
	
	@Override
	public int compareTo(BlockHeader other)
	{
		return Long.compare(getHeight(), other.getHeight());
	}
	
	public Identity getProposer()
	{
		return this.proposer;
	}

	public synchronized void sign(final BLSKeyPair key) throws CryptoException
	{
		Objects.requireNonNull(key, "Key pair is null");
		
		if (this.signature != null)
			throw new IllegalStateException("Block header already signed "+this);
		
		if (key.getIdentity().equals(this.proposer.getIdentity()) == false)
			throw new CryptoException("Attempting to sign block header with key that doesn't match owner");

		this.signature = key.getPrivateKey().sign(getHash());
	}

	public synchronized boolean verify(final BLSPublicKey key) throws CryptoException
	{
		Objects.requireNonNull(key, "Public key is null");

		if (this.signature == null)
			throw new CryptoException("Signature is not present");
		
		if (this.proposer == null)
			throw new CryptoException("Proposer is not present");

		if (key.getIdentity().equals(this.proposer.getIdentity()) == false)
			throw new CryptoException("Proposer does not match key");

		return key.verify(getHash(), this.signature);
	}

	boolean requiresSignature()
	{
		return true;
	}
	
	public synchronized BLSSignature getSignature()
	{
		return this.signature;
	}
	
	public final boolean isInRange(final BlockHeader other, final int limit)
	{
		Objects.requireNonNull(other, "Block header is null");
		
		long thisHeight = this.getHeight();
		long otherHeight = other.getHeight();
		long heightDelta = Math.abs(thisHeight - otherHeight);
		if (heightDelta > limit)
			return false;
		
		return true;
	}

	public final boolean isAheadOf(final BlockHeader other, final int limit)
	{
		Objects.requireNonNull(other, "Block header is null");

		long thisHeight = this.getHeight();
		long otherHeight = other.getHeight();
		long heightDelta = thisHeight - otherHeight;
		if (heightDelta >= limit)
			return true;
		
		return false;
	}

	public List<MerkleProof> getInventoryMerkleProof(Hash primitive) 
	{
		// TODO hold an inventory merkle tree and query for audit proofs
		return Collections.singletonList(MerkleProof.from(this.inventoryMerkle, Branch.ROOT));
	}
	
	public synchronized int getSize() throws SerializationException
	{
		if (this.size == -1)
			this.size = Serialization.getInstance().toDson(this, Output.WIRE).length;
		
		return this.size;
	}
}
