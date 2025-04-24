package org.radix.hyperscale.ledger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.eclipse.collections.api.factory.Maps;
import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.map.MutableMap;
import org.eclipse.collections.api.map.primitive.MutableObjectLongMap;
import org.eclipse.collections.api.set.MutableSet;
import org.eclipse.collections.impl.factory.primitive.ObjectLongMaps;
import org.radix.hyperscale.Context;
import org.radix.hyperscale.crypto.CryptoException;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.crypto.Identity;
import org.radix.hyperscale.crypto.bls12381.BLS12381;
import org.radix.hyperscale.crypto.bls12381.BLSPublicKey;
import org.radix.hyperscale.crypto.bls12381.BLSSignature;
import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

public final class BlockVoteCollector
{
	private static final Logger blocksLog = Logging.getLogger("blocks");
	
	private final Context context;
	private final ProgressRound progressRound;
	private final MutableMap<Identity, Long> voted;
	private final MutableMap<Hash, BlockVote> votes;
	private final MutableSet<Hash> verified;

	private final long voteThreshold;
	private volatile long voteWeight;
	
	BlockVoteCollector(final Context context, final ProgressRound progressRound)
	{
		Objects.requireNonNull(context, "Context is null");
		Objects.requireNonNull(progressRound, "Progress round is null");

		this.context = Objects.requireNonNull(context, "Context is null");
		this.progressRound = progressRound;
		this.voted = Maps.mutable.ofInitialCapacity(4);
		this.votes = Maps.mutable.ofInitialCapacity(4);
		this.verified = Sets.mutable.ofInitialCapacity(4);

		this.voteWeight = 0;
		this.voteThreshold = progressRound.getVoteThreshold();

		if (blocksLog.hasLevel(Logging.INFO))
			blocksLog.info(context.getName()+": Created block vote collector for progress round "+this.progressRound.clock());
	}
	
	long getHeight()
	{
		return this.progressRound.clock();
	}
	
	ProgressRound getProgressRound()
	{
		return this.progressRound;
	}
	
	boolean canVerify()
	{
		synchronized(this)
		{
			if (this.voteWeight < this.voteThreshold && this.progressRound.isVoteLatent() == false)
				return false;
			
			return true;
		}
	}
	
	boolean hasMetThreshold()
	{
		return this.voteWeight >= this.voteThreshold;
	}
	
	List<BlockVote> tryVerify() throws IOException, CryptoException
	{
		synchronized(this)
		{
			if (canVerify() == false)
				return Collections.emptyList();
			
			if (this.verified.size() == this.votes.size())
				return Collections.emptyList();

			final List<BlockVote> verifiedVotes = new ArrayList<BlockVote>(this.votes.size());
			final ListMultimap<Hash, BlockVote> votesByBlock = ArrayListMultimap.create(4, 4);
			for(final BlockVote blockVote : this.votes.values())
			{
				if (this.verified.contains(blockVote.getHash()))
					continue;
				
				votesByBlock.put(blockVote.getBlock(), blockVote);
			}
			
			// TODO sort by weight?
			for (final Hash block : votesByBlock.keySet())
			{
				final List<BlockVote> blockVotes = votesByBlock.get(block);
				final BLSPublicKey aggregatedPublicKey = BLS12381.aggregatePublicKey(blockVotes.stream().map(bv -> bv.getOwner()).collect(Collectors.toList()));
				final BLSSignature aggregatedSignature = BLS12381.aggregateSignatures(blockVotes.stream().map(bv -> bv.getSignature()).collect(Collectors.toList()));
				if (aggregatedPublicKey.verify(block, aggregatedSignature) == false)
				{
					// TODO Do individual
					blocksLog.warn(this.context.getName()+": Aggregated block vote verification failed for block "+Block.toHeight(block)+" "+block);
	
					this.context.getMetaData().increment("ledger.pool.block.vote.verifications");
				}
				else
				{
					verifiedVotes.addAll(blockVotes);
					for (final BlockVote blockVote : blockVotes)
						this.verified.add(blockVote.getHash());
					
					this.context.getMetaData().increment("ledger.pool.block.vote.verifications");
				}
			}
			
			if (blocksLog.hasLevel(Logging.INFO))
				blocksLog.info(this.context.getName()+": Verified "+votesByBlock.size()+" block votes for progress round "+this.progressRound.clock());
			
			return verifiedVotes;
		}
	}
	
	boolean hasVoted(final Identity identity)
	{
		Objects.requireNonNull(identity, "Identity is null");

		synchronized(this)
		{
			return this.voted.containsKey(identity);
		}
	}
	
	void vote(final BlockVote blockVote)
	{
		Objects.requireNonNull(blockVote, "Block vote is null");

		if (blockVote.getHeight() != this.progressRound.clock())
			throw new IllegalArgumentException("Block vote "+blockVote+" is not for progress round "+this.progressRound.clock());

		synchronized(this)
		{
			if (this.voted.containsKey(blockVote.getOwner().getIdentity()))
				throw new IllegalArgumentException("Block vote owner "+blockVote.getOwner().getIdentity().toString(12)+" has already cast a vote for progress round "+this.progressRound.clock());

			this.votes.put(blockVote.getHash(), blockVote);
			this.voted.put(blockVote.getOwner().getIdentity(), blockVote.getWeight());
			this.voteWeight += blockVote.getWeight();
		}

		if (blocksLog.hasLevel(Logging.INFO))
			blocksLog.info(this.context.getName()+": Block vote for "+blockVote.getHeight()+":"+blockVote.getBlock()+" from "+blockVote.getOwner().getIdentity().toString(12));
	}
	
	@Override
	public String toString()
	{
		return this.progressRound.clock()+" "+this.voteWeight+"/"+this.voteThreshold+" "+this.voted.size();
	}
}
