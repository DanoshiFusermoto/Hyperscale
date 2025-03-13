package org.radix.hyperscale.ledger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.crypto.MerkleProof;
import org.radix.hyperscale.crypto.bls12381.BLSKeyPair;
import org.radix.hyperscale.crypto.bls12381.BLSPublicKey;
import org.radix.hyperscale.crypto.bls12381.BLSSignature;
import org.radix.hyperscale.utils.Numbers;

public final class StateVote extends Vote<BLSKeyPair, BLSPublicKey, BLSSignature>
{
	private static final ThreadLocal<ByteBuffer> hashBuffer = ThreadLocal.withInitial(() -> ByteBuffer.allocate(Hash.BYTES * 8));

	static Hash convertToHash(final StateAddress address, final Hash atom, final Hash block, final Hash execution)
	{
		try
		{
			Objects.requireNonNull(execution, "Execution is null");
			Objects.requireNonNull(atom, "Block is null");
			Hash.notZero(block, "Block is ZERO");
			Objects.requireNonNull(atom, "Atom is null");
			Hash.notZero(atom, "Atom is ZERO");

			ByteBuffer buffer = hashBuffer.get();
			buffer.clear();
			buffer.put(address.getHash().toByteArray());
			buffer.put(atom.toByteArray());
			buffer.put(block.toByteArray());
			buffer.put(execution.toByteArray());
			return Hash.hash(buffer.array(), 0, buffer.position());
		}
		catch (Exception ex)
		{
			throw ex;
		}
	}
	
	private final StateAddress address;

	private final Hash atom;
	private final Hash block;
	private final Hash execution;
	private final Hash voteMerkle;
	private final List<MerkleProof> voteAudit;
	
	private final transient long votePower;
	
	StateVote(final StateAddress address, final Hash atom, final Hash block, final Hash execution, final BLSPublicKey owner, final long votePower)
	{
		super(convertToHash(address, atom, block, execution), execution.equals(Hash.ZERO) == false ? CommitDecision.ACCEPT : CommitDecision.REJECT, owner);
		
		this.atom = atom;
		this.block = block;
		this.execution = execution;
		this.address = address;
		this.voteMerkle = Hash.ZERO;
		this.voteAudit = Collections.emptyList();
		this.votePower = Numbers.isNegative(votePower, "Vote power is negative");
	}

	StateVote(final StateAddress address, final Hash atom, final Hash block, final Hash execution, final Hash voteMerkle, final List<MerkleProof> voteAudit, final BLSPublicKey owner, final long votePower, final BLSSignature signature)
	{
		super(convertToHash(address, atom, block, execution), execution.equals(Hash.ZERO) == false ? CommitDecision.ACCEPT : CommitDecision.REJECT, owner, signature);
		Objects.requireNonNull(voteMerkle, "Vote merkle is null");
		if (StateVoteCollector.MERKLE_AUDITS_DISABLED == false)
			Hash.notZero(voteMerkle, "Vote merkle is ZERO");
		Objects.requireNonNull(voteAudit, "Vote audit is null");
		Numbers.isZero(voteAudit.size(), "Vote audit is empty");
		
		this.atom = atom;
		this.block = block;
		this.execution = execution;
		this.address = address;
		this.voteMerkle = voteMerkle;
		this.voteAudit = new ArrayList<MerkleProof>(voteAudit);
		this.votePower = Numbers.isNegative(votePower, "Vote power is negative");
	}
	
	StateVote(final Hash object, final StateAddress address, final Hash atom, final Hash block, final Hash execution, final Hash voteMerkle, final List<MerkleProof> voteAudit, final BLSPublicKey owner, final long votePower, final BLSSignature signature)
	{
		super(object, execution.equals(Hash.ZERO) == false ? CommitDecision.ACCEPT : CommitDecision.REJECT, owner, signature);
		Objects.requireNonNull(voteMerkle, "Vote merkle is null");
		if (StateVoteCollector.MERKLE_AUDITS_DISABLED == false)
			Hash.notZero(voteMerkle, "Vote merkle is ZERO");
		Objects.requireNonNull(voteAudit, "Vote audit is null");
		Numbers.isZero(voteAudit.size(), "Vote audit is empty");
		
		this.atom = atom;
		this.block = block;
		this.execution = execution;
		this.address = address;
		this.voteMerkle = voteMerkle;
		this.voteAudit = new ArrayList<MerkleProof>(voteAudit);
		this.votePower = Numbers.isNegative(votePower, "Vote power is negative");
	}

	@Override
	public int hashCode() 
	{
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + this.atom.hashCode();
		result = prime * result + this.block.hashCode();
		result = prime * result + this.execution.hashCode();
		result = prime * result + this.address.hashCode();
		result = prime * result + this.voteMerkle.hashCode();
		result = prime * result + this.voteAudit.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (obj == null)
			return false;

		if (obj == this)
			return true;

		if (obj instanceof StateVote stateVote)
		{
			if (stateVote.atom.equals(this.atom) == false)
				return false;
			
			if (stateVote.block.equals(this.block) == false)
				return false;
			
			if (stateVote.execution.equals(this.execution) == false)
				return false;
			
			if (stateVote.address.equals(this.address) == false)
				return false;

			if (stateVote.voteMerkle.equals(this.voteMerkle) == false)
				return false;

			if (stateVote.voteAudit.equals(this.voteAudit) == false)
				return false;

			return super.equals(obj);
		}
		
		return false;
	}

	public Hash getAtom()
	{
		return this.atom;
	}

	public Hash getBlock()
	{
		return this.block;
	}
	
	public long getHeight()
	{
		return Block.toHeight(this.block);
	}

	public StateAddress getAddress()
	{
		return this.address;
	}

	public Hash getExecution()
	{
		return this.execution;
	}
	
	public Hash getVoteMerkle()
	{
		return this.voteMerkle;
	}
	
	public List<MerkleProof> getVoteAudit()
	{
		return Collections.unmodifiableList(this.voteAudit);
	}
	
	public long getVotePower()
	{
		return this.votePower;
	}

	// TODO put back to final
	@Override
	public String toString()
	{
		return getHash()+" A: "+this.atom+" B: "+this.block+" S: "+this.address+" E: "+this.execution+" <- "+getOwner().toString(12);
	}
}
