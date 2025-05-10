package org.radix.hyperscale.ledger;

import org.radix.hyperscale.common.EphemeralPrimitive;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.crypto.bls12381.BLSKeyPair;
import org.radix.hyperscale.crypto.bls12381.BLSPublicKey;
import org.radix.hyperscale.crypto.bls12381.BLSSignature;
import org.radix.hyperscale.network.TransportParameters;
import org.radix.hyperscale.serialization.DsonCached;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.SerializerId2;
import org.radix.hyperscale.utils.Numbers;
import org.radix.hyperscale.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.block.vote")
@TransportParameters(urgent = true)
@DsonCached
public final class BlockVote extends Vote<BLSKeyPair, BLSPublicKey, BLSSignature> implements EphemeralPrimitive
{
	/*
	 * The wall clock timestamp of the progression into the round referenced by this vote
	 */
	@JsonProperty("progressed_at")
	@DsonOutput(Output.ALL)
	private long progressedAt;
	
	@SuppressWarnings("unused")
	private BlockVote()
	{
		// SERIALIZER
	}
	
	public BlockVote(final Hash proposal, final long progressedAt, final BLSPublicKey owner)
	{
		super(proposal, CommitDecision.ACCEPT, owner);

		Numbers.isNegative(progressedAt, "Progress timestamp is negative");
		this.progressedAt = progressedAt;
	}
	
	public long progressedAt()
	{
		return this.progressedAt;
	}

	public Hash getBlock()
	{
		return getObject();
	}

	public long getHeight()
	{
		return Block.toHeight(getBlock());
	}
}
