package org.radix.hyperscale.ledger;

import org.radix.hyperscale.common.EphemeralPrimitive;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.crypto.bls12381.BLSKeyPair;
import org.radix.hyperscale.crypto.bls12381.BLSPublicKey;
import org.radix.hyperscale.crypto.bls12381.BLSSignature;
import org.radix.hyperscale.network.TransportParameters;
import org.radix.hyperscale.serialization.DsonCached;
import org.radix.hyperscale.serialization.SerializerId2;

@SerializerId2("ledger.block.vote")
@TransportParameters(urgent = true)
@DsonCached
public final class BlockVote extends Vote<BLSKeyPair, BLSPublicKey, BLSSignature> implements EphemeralPrimitive
{
	@SuppressWarnings("unused")
	private BlockVote()
	{
		// SERIALIZER
	}
	
	public BlockVote(final Hash header, BLSPublicKey owner)
	{
		super(header, CommitDecision.ACCEPT, owner);
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
