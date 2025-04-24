package org.radix.hyperscale.ledger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.eclipse.collections.api.factory.Maps;
import org.eclipse.collections.api.map.MutableMap;
import org.radix.hyperscale.Constants;
import org.radix.hyperscale.Context;
import org.radix.hyperscale.crypto.CryptoException;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.crypto.MerkleProof;
import org.radix.hyperscale.crypto.MerkleTree;
import org.radix.hyperscale.crypto.MerkleProof.Branch;
import org.radix.hyperscale.crypto.bls12381.BLSKeyPair;
import org.radix.hyperscale.crypto.bls12381.BLSSignature;
import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;
import org.radix.hyperscale.time.Time;
import org.radix.hyperscale.utils.Base58;
import org.radix.hyperscale.utils.Numbers;

import com.google.common.primitives.UnsignedBytes;

public final class StateVoteCollector
{
	static final boolean DEBUG_SIMPLE_UNGROUPED = true;
	
	public static final boolean MERKLE_AUDITS_DISABLED = Boolean.getBoolean("merkle.audit.disabled");
	static 
	{
		if (MERKLE_AUDITS_DISABLED)
			System.err.println("Info: Merkle audits are disabled");
	}
	
	private static final Logger statePoolLog = Logging.getLogger("statepool");
	
	private final Context context;
	private final Hash ID;
	private final Hash block;
	private final long timestamp;
	private final long votePower;
	private final long voteThreshold;
	private final MutableMap<Hash, PendingState> states;
	private final MutableMap<Hash, StateVote> votes;
	
	private final boolean isLatent;
	private volatile boolean isCompleted = false;
	
	StateVoteCollector(final Context context, final Hash block, final PendingState state, final long votePower, final long voteThreshold)
	{
		Objects.requireNonNull(state, "Pending state is null");
		Objects.requireNonNull(block, "Block is null");
		Hash.notZero(block, "Block is ZERO");

		this.context = Objects.requireNonNull(context, "Context is null");
		this.block = block;
		this.timestamp = Time.getSystemTime();
		this.votes = Maps.mutable.ofInitialCapacity(4);
		this.states = Maps.mutable.of(state.getHash(), state);
		this.isLatent = true;

		this.votePower = votePower;
		this.voteThreshold = voteThreshold;

		this.ID = Hash.hash(block, state.getHash());
		
		if (statePoolLog.hasLevel(Logging.INFO))
			statePoolLog.info(context.getName()+": Created state vote collector "+toString());
	}

	StateVoteCollector(final Context context, final Hash block, final Collection<PendingState> states, long votePower, long voteThreshold)
	{
		Objects.requireNonNull(states, "Pending states is null");
		Numbers.isZero(states.size(), "Pending states is empty");
		Objects.requireNonNull(block, "Block is null");
		Hash.notZero(block, "Block is ZERO");

		this.context = Objects.requireNonNull(context, "Context is null");
		this.block = block;
		this.timestamp = Time.getSystemTime();
		this.votes = Maps.mutable.ofInitialCapacity(states.size());
		this.states = Maps.mutable.ofInitialCapacity(states.size());
		this.isLatent = false;

		// Order of states is important!
		final List<PendingState> sortedStates = new ArrayList<PendingState>(states);
		sortedStates.sort((ps1, ps2) -> ps1.getAddress().compareTo(ps2.getAddress()));
		sortedStates.forEach(ps -> this.states.put(ps.getHash(), ps));
		
		this.votePower = votePower;
		this.voteThreshold = voteThreshold;

		Hash stateHash = Hash.valueOf(sortedStates, s -> s.getHash().toByteArray());
		this.ID = Hash.hash(block, stateHash);
		
		if (statePoolLog.hasLevel(Logging.DEBUG))
			statePoolLog.info(context.getName()+": Created state vote collector "+toString());
	}
	
	Hash getID()
	{
		return this.ID;
	}
	
	@Override
	public int hashCode()
	{
		return this.ID.hashCode();
	}
	
	@Override
	public boolean equals(Object other)
	{
		if (other == null)
			return false;
		
		if (other == this)
			return true;
		
		if (other instanceof StateVoteCollector stateVoteCollector)
		{
			if (stateVoteCollector.block.equals(this.block) == false)
				return false;
			
			if (stateVoteCollector.ID.equals(this.ID) == false)
				return false;

			return true;
		}
		
		return false;
	}
	
	Hash getBlock()
	{
		return this.block;
	}
	
	boolean contains(final Hash pendingState) 
	{
		Objects.requireNonNull(pendingState, "Pending state hash is null");
		Hash.notZero(pendingState, "Pending state hash is ZERO");

		synchronized(this)
		{
			return this.states.containsKey(pendingState);
		}
	}

	Collection<PendingState> getStates() 
	{
		synchronized(this)
		{
			return this.states.toList();
		}
	}

	boolean isCompleted()
	{
		return this.isCompleted;
	}

	boolean isEmpty()
	{
		synchronized(this)
		{
			return this.states.isEmpty();
		}
	}

	boolean isLatent() 
	{
		return this.isLatent;
	}

	boolean isStale()
	{
		synchronized(this)
		{
			if (isCompleted())
				return false;
			
			if (this.states.isEmpty())
				return true;
		}
		
		if (Time.getSystemTime() - this.timestamp < TimeUnit.SECONDS.toMillis(Constants.PRIMITIVE_STALE))
			return false;
			
		return true;
	}
	
	boolean isCompletable()
	{
		synchronized(this)
		{
			if (this.isCompleted)
				throw new IllegalStateException("State vote collector is already completed "+toString()+" with states "+this.states.values().toString());
			
			if (this.states.isEmpty())
				throw new IllegalStateException("State vote collector is empty "+toString());
		
			if (this.states.size() != this.votes.size())
				return false;
			
			return true;
		}
	}
	
	StateVoteBlock tryComplete(final BLSKeyPair key) throws IOException, CryptoException
	{
		Objects.requireNonNull(key, "BLSKeyPair is null");

		synchronized(this)
		{
			if (this.isCompleted)
				throw new IllegalStateException("State vote collector is already completed "+toString()+" with states "+this.states.values().toString());
			
			if (this.states.isEmpty())
				throw new IllegalStateException("State vote collector is empty "+toString());
		
			if (this.states.size() != this.votes.size())
				return null;

			// IMPORTANT Strict order is important for state & vote merkles as validators may execute in different sequence!
			final Hash voteMerkleHash;
			final MerkleTree voteMerkleTree;
			final List<MerkleProof> rootMerkleProofAudit;
			final List<Hash> sortedStateKeys = new ArrayList<Hash>(this.states.keySet());
			sortedStateKeys.sort((h1, h2) -> UnsignedBytes.lexicographicalComparator().compare(h1.toByteArray(), h2.toByteArray()));
			if (MERKLE_AUDITS_DISABLED == false)
			{
				voteMerkleTree = new MerkleTree(this.states.size());
				for (Hash stateKey : sortedStateKeys)
				{
					StateVote stateVote = this.votes.get(stateKey);
					voteMerkleTree.appendLeaf(stateVote.getObject());
				}
				
				voteMerkleHash = voteMerkleTree.buildTree();
				rootMerkleProofAudit = Collections.singletonList(MerkleProof.from(voteMerkleHash, Branch.ROOT));
			}
			else
			{
				voteMerkleTree = null;
				voteMerkleHash = this.block;
				rootMerkleProofAudit = Collections.singletonList(MerkleProof.from(voteMerkleHash, Branch.ROOT));
			}
			
			BLSSignature signature = key.getPrivateKey().sign(voteMerkleHash);
			if (statePoolLog.hasLevel(Logging.TRACE))
				statePoolLog.trace(this.context.getName()+": Signed "+voteMerkleHash+" "+key.getPublicKey().toString()+":"+Base58.toBase58(signature.toByteArray()));
			
			List<StateVote> completedStateVotes = new ArrayList<StateVote>(this.states.size());
			for (Hash stateKey : sortedStateKeys)
			{
				StateVote stateVote = this.votes.get(stateKey);
				List<MerkleProof> voteMerkleAudit = MERKLE_AUDITS_DISABLED == false && this.states.size() > 1 ? voteMerkleTree.auditProof(stateVote.getObject()) : rootMerkleProofAudit;
				StateVote completedStateVote = new StateVote(stateVote.getAddress(), stateVote.getAtom(), stateVote.getBlock(), stateVote.getExecution(),
															 voteMerkleHash, voteMerkleAudit, key.getPublicKey(), signature, stateVote.getWeight());
				completedStateVotes.add(completedStateVote);
			}
			
			this.isCompleted = true;
			
			if (statePoolLog.hasLevel(Logging.INFO))
			{
				if (this.isLatent)
					statePoolLog.info(this.context.getName()+": Latent state vote collector completed "+toString()+" "+this.votePower+"/"+this.voteThreshold);
				else
					statePoolLog.info(this.context.getName()+": State vote collector completed "+toString()+" "+this.votePower+"/"+this.voteThreshold);
			}
			
			if (this.states.size() > 1)
				this.context.getMetaData().increment("ledger.pool.state.vote.collectors.latency", (System.currentTimeMillis() - this.timestamp));

			StateVoteBlock stateVoteBlock = new StateVoteBlock(this.ID, completedStateVotes);

			if (statePoolLog.hasLevel(Logging.INFO))
				statePoolLog.info(this.context.getName()+": Created state vote block "+stateVoteBlock);
			if (statePoolLog.hasLevel(Logging.DEBUG))
				statePoolLog.debug(this.context.getName()+": 						 "+completedStateVotes);
				
			return stateVoteBlock;
		}
	}
	
	boolean hasVoted(PendingState state)
	{
		Objects.requireNonNull(state, "Pending state is null");

		if (state.getBlockHeader().getHash().equals(this.block) == false)
			throw new IllegalArgumentException("State does not reference block in "+toString());

		synchronized(this)
		{
			if (this.states.containsKey(state.getHash()) == false)
				throw new IllegalArgumentException("State "+state+" is not referenced in this state vote collector "+toString());

			return this.votes.containsKey(state.getHash());
		}
	}
	
	void vote(final PendingState pendingState, final long votePower)
	{
		Objects.requireNonNull(pendingState, "Pending state is null");

		if (pendingState.getBlockHeader().getHash().equals(this.block) == false)
			throw new IllegalArgumentException("Pending state does not reference block in state vote collector "+toString());

		synchronized(this)
		{
			if (this.states.containsKey(pendingState.getHash()) == false)
				throw new IllegalArgumentException("Pending state "+pendingState.getAddress()+" is not referenced in state vote collector "+toString());

			this.votes.compute(pendingState.getHash(), (h, v) -> {
				if (v != null)
					throw new IllegalArgumentException("Already have a state vote for "+pendingState.getAddress()+" in "+StateVoteCollector.this.toString());
				
				return new StateVote(pendingState.getAddress(), pendingState.getAtom().getHash(), pendingState.getBlockHeader().getHash(),
							  		 pendingState.getAtom().getExecutionDigest(), this.context.getNode().getKeyPair().getPublicKey(), votePower);
			});
		}

		if (this.isLatent && statePoolLog.hasLevel(Logging.INFO))
			statePoolLog.info(this.context.getName()+": Voted on latent state vote collector "+pendingState.getAddress()+" in "+toString());
		else if (statePoolLog.hasLevel(Logging.DEBUG))
			statePoolLog.debug(this.context.getName()+": Voted "+this.votes.get(pendingState.getHash())+" in state vote collector "+toString());
	}
	
	boolean remove(final PendingState state)
	{
		Objects.requireNonNull(state, "Pending state is null");

		synchronized(this)
		{
			if (this.isCompleted)
				throw new IllegalStateException("State vote collector is already completed when attempting removal of "+state.getAddress()+" in "+toString());

			Hash pendingStateHash = state.getHash();
			if (this.states.containsKey(pendingStateHash) == false)
				throw new IllegalArgumentException("State "+state.getAddress()+" is not referenced in this state vote collector "+toString());

			if (this.votes.containsKey(pendingStateHash))
				return false;

			if (this.states.remove(pendingStateHash) == null)
				throw new IllegalArgumentException("State "+state.getAddress()+" could not be removed from state vote collector "+toString());
		}

		if (statePoolLog.hasLevel(Logging.INFO))
			statePoolLog.info(this.context.getName()+": State is removed "+state.getAddress()+" in state vote collector "+toString());
		
		return true;
	}
	
	@Override
	public String toString()
	{
		return this.ID+" "+this.states.size()+"/"+this.votes.size()+" "+(this.isLatent ? "LATENT ":"")+Block.toHeight(this.block)+"@"+this.block;
	}
}
