package org.radix.hyperscale.ledger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.eclipse.collections.api.factory.Sets;
import org.radix.hyperscale.Configuration;
import org.radix.hyperscale.Context;
import org.radix.hyperscale.collections.Bloom;
import org.radix.hyperscale.collections.LRUCacheMap;
import org.radix.hyperscale.crypto.CryptoException;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.crypto.Hashable;
import org.radix.hyperscale.crypto.Identity;
import org.radix.hyperscale.crypto.bls12381.BLS12381;
import org.radix.hyperscale.crypto.bls12381.BLSPublicKey;
import org.radix.hyperscale.crypto.bls12381.BLSSignature;
import org.radix.hyperscale.exceptions.ValidationException;
import org.radix.hyperscale.ledger.primitives.StateCertificate;
import org.radix.hyperscale.ledger.primitives.StateInput;
import org.radix.hyperscale.ledger.primitives.StateOutput;
import org.radix.hyperscale.ledger.primitives.StateReference;
import org.radix.hyperscale.ledger.sme.StateMachine;
import org.radix.hyperscale.ledger.sme.SubstateLog;
import org.radix.hyperscale.ledger.sme.StateMachineStatus.State;
import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;
import org.radix.hyperscale.utils.Numbers;

public final class PendingState implements Hashable, StateAddressable, Comparable<PendingState>
{
	// State vote merkle verification cache
	// TODO refactor this away from here
	private static final LRUCacheMap<Hash, StateVerificationRecord> verificationCache;
	
	static 
	{
		int numContexts = Configuration.getDefault().getCommandLine("contexts", 1); 
		verificationCache = new LRUCacheMap<Hash, StateVerificationRecord>((1<<12) * numContexts);
	}
	
	private static final Logger stateLog = Logging.getLogger("state");
	
	static Hash getHash(final Hash atom, final StateAddress stateAddress)
	{
		Objects.requireNonNull(stateAddress, "State address is null");
		Objects.requireNonNull(atom, "Atom hash is null");
		Hash.notZero(atom, "Atom hash is zero");
		return Hash.hash(atom.toByteArray(), stateAddress.getHash().toByteArray());
	}

	static Hash getHash(final Hash atom, final Hash stateAddressHash)
	{
		Objects.requireNonNull(stateAddressHash, "State address hash is null");
		Objects.requireNonNull(atom, "Atom hash is null");
		Hash.notZero(atom, "Atom hash is zero");
		return Hash.hash(atom.toByteArray(), stateAddressHash.toByteArray());
	}

	private final Context 		context;
	
	private final PendingAtom	atom;
	private final StateAddress  address;

	private volatile BlockHeader header;
	private volatile long 		 voteWeight;
	private volatile long 		 voteThreshold;
	private volatile Set<Identity> signers; 
	private volatile List<BLSSignature> signatures;		

	private volatile StateLockMode	lockMode;
	private volatile StateInput		stateInput;
	private volatile StateOutput	stateOutput;
	private volatile SubstateLog 	substateLog;
	private volatile StateVote 		refStateVote;
	private volatile StateMachine 	stateMachine;
	private volatile StateVoteCollector stateVoteCollector;
	
	private final transient Hash hash;

	public PendingState(final Context context, final StateAddress address, final PendingAtom atom)
	{
		this.context = Objects.requireNonNull(context, "Context is null");
		this.address = Objects.requireNonNull(address, "State address is null");
		this.atom = Objects.requireNonNull(atom, "Atom is null");
		this.header = null;
		this.voteThreshold = Long.MAX_VALUE;
		
		this.hash = getHash(this.atom.getHash(), this.address);
	}

	@Override
	public Hash getHash()
	{
		return this.hash;
	}

	@Override
	public StateAddress getAddress()
	{
		return this.address;
	}
	
	public StateInput getStateInput()
	{
		synchronized(this)
		{
			return this.stateInput;
		}
	}

	public void setStateInput(final StateInput stateInput)
	{
		synchronized(this)
		{
			if (this.stateInput != null)
				throw new IllegalStateException("Input substate is already set");

			if (stateInput.getAtom().equals(this.atom.getHash()) == false)
				throw new IllegalArgumentException("Input substate is not for atom "+this.atom.getHash());
			
			if (stateInput.getAddress().equals(this.address) == false)
				throw new IllegalArgumentException("Input substate is not for state address "+this.address);
			
			this.stateInput = stateInput;
			
			if (stateLog.hasLevel(Logging.DEBUG))
				stateLog.debug(this.context.getName()+": State input is set for pending state "+stateInput);
		}
	}
	
	public void setStateOutput(final StateOutput stateOutput)
	{
		if (stateOutput.getAtom().equals(this.atom.getHash()) == false)
			throw new IllegalArgumentException("State output is not for atom "+this.atom.getHash());

		if (stateOutput.getAddress().equals(this.address) == false)
			throw new IllegalArgumentException("Output substate is not for state address "+this.address);

		synchronized(this)
		{
			if (this.stateOutput != null)
				throw new IllegalStateException("State output is already set");

			this.stateOutput = stateOutput;
		}
	}

	public SubstateLog getSubstateLog()
	{
		return this.substateLog;
	}
	
	public StateOutput getStateOutput()
	{
		synchronized(this)
		{
			return this.stateOutput;
		}
	}

	public void accepted(final BlockHeader header)
	{
		Objects.requireNonNull(header, "Block header is null");

		synchronized(this)
		{
			if (this.header != null)
				throw new IllegalStateException("Pending state "+this.address+" in atom "+this.atom.getAtom().getHash()+" already accepted");
			
			this.header = header;
		}
	}

	public void accepted(final BlockHeader header, final long voteThreshold)
	{
		Objects.requireNonNull(header, "Block header is null");
		Numbers.lessThan(voteThreshold, 1, "Vote threshold is less than 1");

		synchronized(this)
		{
			if (this.header != null)
				throw new IllegalStateException("Pending state "+this.address+" in atom "+this.atom.getAtom().getHash()+" already accepted");

			this.header = header;
			this.voteThreshold = voteThreshold;
		}
	}
	
	public void lock(final StateLockMode lockMode, final StateMachine stateMachine)
	{
		Objects.requireNonNull(lockMode, "Lock mode is null");
		Objects.requireNonNull(stateMachine, "State machine is null");

		synchronized(this)
		{
			if (this.stateMachine != null)
			{
				if (this.stateMachine.getStatus().after(State.NONE))
					throw new IllegalStateException("State machine for "+this.atom.getHash()+" is already prepared");
			
				if (this.lockMode == null)
					throw new IllegalStateException("Expected lock to be set for "+this.address+" on PREPARED state machine in "+this.atom.getHash());
				
				if (this.lockMode.equals(StateLockMode.WRITE))
					return;
				
				this.lockMode = lockMode;
				return;
			}
			
			this.lockMode = lockMode;
			this.stateMachine = stateMachine;
			
			if (lockMode.equals(StateLockMode.WRITE))
			{
				this.signers = Sets.mutable.ofInitialCapacity(4);
				this.signatures = new ArrayList<BLSSignature>(4);
			}
			else
			{
				this.signers = Collections.emptySet();
				this.signatures = Collections.emptyList();
			}
		}
	}
	
	public void provision()
	{
		synchronized(this)
		{
			if (this.stateInput == null)
				throw new IllegalStateException("Input substate is not set");

			if (this.substateLog != null)
				throw new IllegalStateException("Input substate is already provisioned");

			this.substateLog = new SubstateLog(this.stateMachine, stateInput.getSubstate());
		}
	}
	
	public boolean isProvisioned()
	{
		return this.substateLog != null;
	}

	public long getHeight()
	{
		return this.header.getHeight();
	}

	public BlockHeader getBlockHeader()
	{
		synchronized(this)
		{
			return this.header;
		}
	}
			
	public PendingAtom getAtom()
	{
		return this.atom;
	}
	
	public StateLockMode getStateLockMode()
	{
		return this.lockMode;
	}
	
	StateVoteCollector getStateVoteCollector()
	{
		return this.stateVoteCollector;
	}

	void setStateVoteCollector(final StateVoteCollector stateVoteCollector)
	{
		if (stateVoteCollector.getBlock().equals(getBlockHeader().getHash()) == false)
			throw new IllegalArgumentException("Expected proposal "+getBlockHeader().getHash()+" for pending state "+getHash()+" but provided "+stateVoteCollector.getBlock()+" in state vote collector "+stateVoteCollector);
		
		if (stateVoteCollector.contains(getHash()) == false)
			throw new IllegalArgumentException("Pending state "+getHash()+" not referenced by state vote collector "+stateVoteCollector);

		// TODO other checks
		this.stateVoteCollector = stateVoteCollector;
	}

	boolean isFinalized()
	{
		return this.stateOutput != null;
	}
	
	<T extends StateOutput> T tryFinalize() throws IOException, CryptoException
	{
		synchronized(this)
		{
			if (this.stateOutput != null)
				throw new IllegalStateException("Pending state "+this+" is already finalized");
			
			final StateOutput stateOutput;
			if (this.lockMode.equals(StateLockMode.WRITE))
			{
				if (this.voteThreshold == Long.MAX_VALUE)
					throw new UnsupportedOperationException("Pending state "+this.address+" in atom "+this.atom.getHash()+" is not local to validator");

				if (this.voteWeight < this.voteThreshold)
					return null;
				
				// Already aggregated and verified a suitable signature?
				StateVerificationRecord cachedVerification = PendingState.verificationCache.get(this.refStateVote.getVoteMerkle());
				if (cachedVerification == null)
				{
					Bloom signersBloom = null;
					BLSPublicKey aggregatedPublicKey = null;
					BLSSignature aggregatedSignature = null;

					// Aggregate and verify
					// TODO failures
					signersBloom = new Bloom(0.00001, this.signers.size()*2);
					
					// Make sure no double votes
					for (Identity signer : this.signers)
						signersBloom.add(signer.toByteArray());
					
					aggregatedPublicKey = BLS12381.aggregatePublicKey(this.signers.stream().map(Identity::<BLSPublicKey>getKey).toList());
					aggregatedSignature = BLS12381.aggregateSignatures(this.signatures);
					
					cachedVerification = new StateVerificationRecord(this.refStateVote.getVoteMerkle(), signersBloom, aggregatedPublicKey, aggregatedSignature);
					PendingState.verificationCache.put(this.refStateVote.getVoteMerkle(), cachedVerification);
				}
				
				if (stateLog.hasLevel(Logging.TRACE))
					stateLog.trace(this.context.getName()+": Verification info for pending state "+getAddress()+" "+this.refStateVote.getVoteMerkle()+" "+cachedVerification.getKey()+":"+cachedVerification.getSignature());
				
				// TODO need merkles
				stateOutput = new StateCertificate(this.refStateVote.getAtom(), this.refStateVote.getBlock(),
												   this.substateLog.toSubstateTransitions(), this.refStateVote.getExecution(), 
												   this.header.getInventoryMerkle(), this.header.getInventoryMerkleProof(this.atom.getHash()),  // TODO verify block merkles / proofs are the correct representation (atom inclusion proof should be sufficient) and fail appropriately
												   this.refStateVote.getVoteMerkle(), this.refStateVote.getVoteAudit(), 
												   cachedVerification.getSigners(), cachedVerification.getKey(), cachedVerification.getSignature());

				if (stateLog.hasLevel(Logging.DEBUG))
					stateLog.debug(this.context.getName()+": Created state certificate "+stateOutput.getHash()+" for state "+getAddress()+" in atom "+getAtom()+" execution output "+this.refStateVote.getExecution()+" has "+this.refStateVote.getDecision()+" agreement with "+this.voteWeight+"/"+this.voteThreshold);
				else if (stateLog.hasLevel(Logging.INFO))
					stateLog.info(this.context.getName()+": Created state certificate "+stateOutput.getHash()+" for state "+getAddress()+" in atom "+getAtom()+" "+this.voteWeight+"/"+this.voteThreshold);
			}
			else
			{
				stateOutput = new StateReference(this.atom.getHash(), this.substateLog.toSubstateTransitions(), this.atom.getExecutionDigest());
				
				if (stateLog.hasLevel(Logging.INFO))
					stateLog.info(this.context.getName()+": Created state reference "+stateOutput.getHash()+" for state "+getAddress()+" in atom "+getAtom());
			}
			
			this.stateOutput = stateOutput;
			return (T) stateOutput;
		}
	}

	void vote(final StateVote vote, final long voteWeight) throws ValidationException
	{
		if (this.voteThreshold == Long.MAX_VALUE)
			throw new UnsupportedOperationException("Pending state "+this.address+" in atom "+this.atom.getHash()+" is not local to validator");

		if (this.lockMode.equals(StateLockMode.WRITE) == false)
			throw new UnsupportedOperationException("Pending state "+this.address+" in atom "+this.atom.getHash()+" does not have a write lock mode");

		synchronized(this)
		{
			if (vote.getAddress().equals(this.address) == false ||
				vote.getBlock().equals(this.header.getHash()) == false || 
				vote.getAtom().equals(this.atom.getHash()) == false)
				throw new ValidationException(vote, "Vote from "+vote.getOwner()+" is not for state address "+this.address+" -> "+this.atom.getHash()+" -> "+this.header.getHash());
				
			if (vote.getDecision().equals(CommitDecision.REJECT) == true && vote.getExecution().equals(Hash.ZERO) == false)
				throw new ValidationException(vote, "Vote from "+vote.getOwner()+" with decision "+vote.getDecision()+" for state address "+this.address+" -> "+this.atom.getHash()+" -> "+this.header.getHash()+" is not of valid form");
			
			if (this.refStateVote == null)
				this.refStateVote = vote;
			else if (vote.getVoteMerkle().equals(this.refStateVote.getVoteMerkle()) == false || vote.getExecution().equals(this.refStateVote.getExecution()) == false)
				throw new ValidationException(vote, "Vote from "+vote.getOwner()+" with decision "+vote.getDecision()+" does not match reference state vote for "+this.address+" -> "+this.atom.getHash()+" -> "+this.header.getHash());

			if (voteWeight > 0)
			{
				if (this.signers.add(vote.getOwner().getIdentity()) == false)
					throw new IllegalStateException(vote.getOwner()+" has already voted on pending state "+this);

				this.voteWeight += voteWeight;
				this.signatures.add(vote.getSignature());
			}
		}
	}

	@Override
	public int hashCode()
	{
		return this.hash.hashCode();
	}

	@Override
	public boolean equals(Object obj)
	{
		if (obj == null)
			return false;

		if (obj == this)
			return true;

		if (obj instanceof PendingState pendingState && pendingState.hash.equals(this.hash))
			return true;
		
		return false;
	}
	
	@Override
	public int compareTo(PendingState other)
	{
		return this.address.compareTo(other.address);
	}

	@Override
	public String toString()
	{
		return this.address+(this.lockMode == null ? "":" "+this.lockMode)+" @ "+this.atom.getHash()+" "+(this.header == null ? "unaccepted":(this.header.getHeight()+":"+this.header.getHash()));
	}
}
