package org.radix.hyperscale.ledger.primitives;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.radix.hyperscale.collections.Bloom;
import org.radix.hyperscale.crypto.CryptoException;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.crypto.MerkleProof;
import org.radix.hyperscale.crypto.VoteCertificate;
import org.radix.hyperscale.crypto.bls12381.BLSPublicKey;
import org.radix.hyperscale.crypto.bls12381.BLSSignature;
import org.radix.hyperscale.ledger.Block;
import org.radix.hyperscale.ledger.CommitDecision;
import org.radix.hyperscale.ledger.StateAddress;
import org.radix.hyperscale.ledger.StateVoteCollector;
import org.radix.hyperscale.ledger.sme.SubstateTransitions;
import org.radix.hyperscale.serialization.DsonCached;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.SerializerId2;
import org.radix.hyperscale.serialization.DsonOutput.Output;
import org.radix.hyperscale.utils.Numbers;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.state.certificate")
@DsonCached
public final class StateCertificate extends VoteCertificate implements StateOutput
{
	@JsonProperty("block")
	@DsonOutput(Output.ALL)
	private Hash block;

	@JsonProperty("atom")
	@DsonOutput(Output.ALL)
	private Hash atom;
	
	@JsonProperty("states")
	@DsonOutput(Output.ALL)
	private SubstateTransitions states;

	@JsonProperty("execution")
	@DsonOutput(Output.ALL)
	private Hash execution;

	// FIXME merkle and audit are for remote block proofing
	//		 not included in certificate hash currently so that aggregated state vote signatures can be verified
	@JsonProperty("block_merkle")
//	@DsonOutput(value = {Output.API, Output.WIRE, Output.PERSIST})
	@DsonOutput(value = {Output.WIRE, Output.PERSIST})
	private Hash blockMerkle;

	@JsonProperty("block_audit")
//	@DsonOutput(value = {Output.API, Output.WIRE, Output.PERSIST})
	@DsonOutput(Output.NONE)
	private List<MerkleProof> blockAudit;
	
	// Batched StateVote merkle and audit
	@JsonProperty("vote_merkle")
//	@DsonOutput(value = {Output.API, Output.WIRE, Output.PERSIST})
	@DsonOutput(value = {Output.WIRE, Output.PERSIST})
	private Hash voteMerkle;

	@JsonProperty("vote_audit")
//	@DsonOutput(value = {Output.API, Output.WIRE, Output.PERSIST})
	@DsonOutput(Output.NONE)
	private List<MerkleProof> voteAudit;

	@SuppressWarnings("unused")
	private StateCertificate()
	{
		super();
		
		// FOR SERIALIZER //
	}

	public StateCertificate(final Hash atom, final Hash block, final SubstateTransitions states, final Hash execution, 
							final Hash blockMerkle, final List<MerkleProof> blockAudit, final Hash voteMerkle, final List<MerkleProof> voteAudit, 
							final Bloom signers, final BLSPublicKey key, final BLSSignature signature)
	{
		super(Objects.requireNonNull(execution, "Execution is null").equals(Hash.ZERO) == false ? CommitDecision.ACCEPT : CommitDecision.REJECT, signers, key, signature);
		
		Objects.requireNonNull(block, "Block is null");
		Hash.notZero(block, "Block is ZERO");

		Objects.requireNonNull(atom, "Atom is null");
		Hash.notZero(atom, "Atom is ZERO");
		
		Objects.requireNonNull(states, "State inventory is null");
		Objects.requireNonNull(blockMerkle, "Block merkle is null");
		Hash.notZero(blockMerkle, "Block merkle is ZERO");
		Objects.requireNonNull(blockAudit, "Block audit is null");
		Numbers.isZero(blockAudit.size(), "Block audit is empty");

		Objects.requireNonNull(voteMerkle, "Vote merkle is null");
		if (StateVoteCollector.MERKLE_AUDITS_DISABLED == false)
			Hash.notZero(voteMerkle, "Vote merkle is ZERO");
		Objects.requireNonNull(voteAudit, "Vote audit is null");
		Numbers.isZero(voteAudit.size(), "Vote audit is empty");

		this.atom = atom;
		this.block = block;
		this.states = states;
		this.execution = execution;
		this.blockMerkle = blockMerkle;
		this.blockAudit = new ArrayList<MerkleProof>(blockAudit);
		this.voteMerkle = voteMerkle;
		this.voteAudit = new ArrayList<MerkleProof>(voteAudit);
	}

	public Hash getBlock()
	{
		return this.block;
	}
	
	public long getHeight()
	{
		return Block.toHeight(this.block);
	}

	public Hash getAtom()
	{
		return this.atom;
	}

	public StateAddress getAddress()
	{
		return this.states.getAddress();
	}

	public SubstateTransitions getStates()
	{
		return this.states;
	}

	public Hash getExecution()
	{
		return this.execution;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T getObject()
	{
		return (T) getAddress();
	}
	
	public Hash getBlockMerkle()
	{
		return this.blockMerkle;
	}
	
	public List<MerkleProof> getBlockAudit()
	{
		return Collections.unmodifiableList(this.blockAudit);
	}

	public Hash getVoteMerkle()
	{
		return this.voteMerkle;
	}
	
	public List<MerkleProof> getVoteAudit()
	{
		return Collections.unmodifiableList(this.voteAudit);
	}

	@Override
	protected Hash getTarget() throws CryptoException
	{
		try
		{
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			baos.write(this.atom.toByteArray());
			baos.write(this.block.toByteArray());
			baos.write(this.execution.toByteArray());
			baos.write(this.states.getAddress().getHash().toByteArray());
			return Hash.valueOf(baos.toByteArray());
		}
		catch (IOException ioex)
		{
			throw new CryptoException(ioex);
		}
	}
	
	@Override
	public String toString()
	{
		return getHash()+", atom="+this.atom+", block="+this.block+", voteMerkle="+this.voteMerkle+", target="+getObject()+", states="+this.states;
	}

}
