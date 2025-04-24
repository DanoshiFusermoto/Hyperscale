package org.radix.hyperscale.ledger;

import java.util.Objects;

import org.radix.hyperscale.collections.Bloom;
import org.radix.hyperscale.crypto.CryptoException;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.crypto.VoteCertificate;
import org.radix.hyperscale.crypto.bls12381.BLSPublicKey;
import org.radix.hyperscale.crypto.bls12381.BLSSignature;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.SerializerId2;
import org.radix.hyperscale.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.quorum.certificate")
@StateContext("quorum.certificate")
public final class QuorumCertificate extends VoteCertificate
{
	public final static QuorumCertificate NULL;
	
	static 
	{
		NULL = new QuorumCertificate();
		NULL.current = Hash.ZERO;
		NULL.previous = Hash.ZERO;
		NULL.committable = Hash.ZERO;
	}
	
	@JsonProperty("current")
	@DsonOutput(Output.ALL)
	private Hash current;
	
	@JsonProperty("previous")
	@DsonOutput(Output.ALL)
	private Hash previous;

	@JsonProperty("head")
	@DsonOutput(Output.ALL)
	private Hash committable;

	@SuppressWarnings("unused") 
	private QuorumCertificate()
	{
		super();
	}
	
	QuorumCertificate(final Hash current, final QuorumCertificate extend, final Hash committable)
	{
		super(CommitDecision.ACCEPT);
		
		Objects.requireNonNull(current, "Current is null");
		Objects.requireNonNull(extend, "Extend is null");
		Objects.requireNonNull(committable, "Committable is null");
		Hash.notZero(current, "Current is ZERO");
		Hash.notZero(committable, "Committable is ZERO");

		this.current = current;
		this.previous = extend.current;
		this.committable = committable;
	}

	QuorumCertificate(final Hash current, final QuorumCertificate extend, final Hash committable, final Bloom signers, final BLSPublicKey key, final BLSSignature signature)
	{
		super(CommitDecision.ACCEPT, signers, key, signature);

		Objects.requireNonNull(current, "Current is null");
		Objects.requireNonNull(extend, "Extend is null");
		Objects.requireNonNull(committable, "Committable is null");
		Hash.notZero(current, "Current is ZERO");
		Hash.notZero(committable, "Committable is ZERO");

		this.current = current;
		this.previous = extend.current;
		this.committable = committable;
	}

	public Hash getCurrent()
	{
		return this.current;
	}
	
	public Hash getPrevious()
	{
		return this.previous;
	}

	public Hash getCommittable()
	{
		return this.committable;
	}

	// TODO hacky
	public Hash updateCommittable(final Hash committable)
	{
		final Hash prevCommittable = this.committable;
		this.committable = committable;
		return prevCommittable;
	}

	public long getHeight()
	{
		return Block.toHeight(this.current);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T getObject()
	{
		return (T) this.current;
	}

	@Override
	protected Hash getTarget() throws CryptoException
	{
		return this.current;
	}
	
	@Override
	public String toString()
	{
		return "current="+Block.toHeight(this.current)+":"+this.current+" previous="+Block.toHeight(this.previous)+":"+this.previous+" committable="+Block.toHeight(this.committable)+":"+this.committable;
	}
}
