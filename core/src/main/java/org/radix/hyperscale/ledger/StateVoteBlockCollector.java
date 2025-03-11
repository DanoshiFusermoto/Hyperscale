package org.radix.hyperscale.ledger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.eclipse.collections.api.factory.Maps;
import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.map.MutableMap;
import org.eclipse.collections.api.map.primitive.MutableObjectLongMap;
import org.eclipse.collections.api.set.MutableSet;
import org.eclipse.collections.impl.factory.primitive.ObjectLongMaps;
import org.radix.hyperscale.Constants;
import org.radix.hyperscale.Context;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.crypto.bls12381.BLSPublicKey;
import org.radix.hyperscale.exceptions.ValidationException;
import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;
import org.radix.hyperscale.time.Time;
import org.radix.hyperscale.utils.Numbers;

class StateVoteBlockCollector
{
	private static final Logger statePoolLog = Logging.getLogger("statepool");
	
	private final Context context;
	private final Hash ID;
	private final Hash block;
	private final long timestamp;
	private final long voteThreshold;
	private final MutableSet<BLSPublicKey> voted;
	private final MutableObjectLongMap<Hash> votes;
	private final MutableMap<Hash, StateVoteBlock> stateVoteBlocks;
	
	private transient long voteWeight;
	private transient Hash selectedExecutionMerkle;
	
	StateVoteBlockCollector(final Context context, final Hash ID, final Hash block, final long voteThreshold, final long timestamp)
	{
		Numbers.isNegative(timestamp, "Timestamp is negative");
		this.context = Objects.requireNonNull(context, "Context is null");
		this.ID = ID;
		this.block = block;
		this.timestamp = timestamp;
		this.voteWeight = 0;
		this.selectedExecutionMerkle = null;
		this.voteThreshold = voteThreshold;
		this.votes = ObjectLongMaps.mutable.withInitialCapacity(8);
		this.voted = Sets.mutable.withInitialCapacity(8);
		this.stateVoteBlocks = Maps.mutable.ofInitialCapacity(8);
		
		if (statePoolLog.hasLevel(Logging.DEBUG))
			statePoolLog.info(context.getName()+": Created state vote block collector "+toString());
	}

	public synchronized StateVoteBlock getStateVoteBlock(Hash hash) 
	{
		return this.stateVoteBlocks.get(hash);
	}

	public synchronized List<StateVoteBlock> getStateVoteBlocks() 
	{
		return this.stateVoteBlocks.toList();
	}

	public synchronized boolean hasVoted(BLSPublicKey identity)
	{
		return this.voted.contains(identity);
	}
	
	public synchronized void vote(StateVoteBlock stateVoteBlock, long weight) throws ValidationException
	{
		if (this.voted.contains(stateVoteBlock.getOwner()))
			throw new IllegalStateException(stateVoteBlock.getOwner()+" has already voted on SVB "+stateVoteBlock);
		
		if (this.stateVoteBlocks.containsKey(stateVoteBlock.getHash()))
			throw new IllegalStateException("SVB "+stateVoteBlock.getHash()+" is already known");

		if (stateVoteBlock.getID().equals(this.ID) == false)
			throw new ValidationException(stateVoteBlock, "SVB with ID "+stateVoteBlock.getID()+" doesn't match collector ID "+this.ID);

		if (stateVoteBlock.getBlock().equals(this.block) == false)
			throw new ValidationException(stateVoteBlock, "SVB with ID "+stateVoteBlock.getID()+" doesn't match collector block"+this.block);
		
		if (statePoolLog.hasLevel(Logging.DEBUG))
			statePoolLog.debug(this.context.getName()+": Applying state vote block "+stateVoteBlock.getHash()+" with vote weight "+weight+" from "+stateVoteBlock.getOwner());

		this.voted.add(stateVoteBlock.getOwner());
		this.votes.addToValue(stateVoteBlock.getExecutionMerkle(), weight);
		
		if (this.votes.size() > 1)
		{
			statePoolLog.warn(this.context.getName()+": Detected "+this.votes.size()+" conflicting state vote block outputs for state ID "+this.ID);
			statePoolLog.warn(this.context.getName()+": 	"+stateVoteBlock);
			statePoolLog.warn(this.context.getName()+": 	"+this.stateVoteBlocks.values());
		}
		this.stateVoteBlocks.put(stateVoteBlock.getHash(), stateVoteBlock);
		
		if (statePoolLog.hasLevel(Logging.INFO))
		{
			if (stateVoteBlock.isHeader() == false)
				statePoolLog.info(this.context.getName()+": Applied state vote block "+stateVoteBlock);
			else
				statePoolLog.info(this.context.getName()+": Applied state vote block header "+stateVoteBlock);
		}
	}
	
	synchronized StateVoteBlock getSuper()
	{
		if (this.votes.size() == 0)
			return null;
		
		Hash superHash = null;
		for (Hash vote : this.votes.keySet())
		{
			if (this.votes.get(vote) < this.voteThreshold)
				continue;

			superHash = vote;
			break;
		}
		
		if (superHash == null)
			return null;
		
		StateVoteBlock superStateVoteBlock = null;
		for (StateVoteBlock stateVoteBlock : this.stateVoteBlocks.values())
		{
			if (stateVoteBlock.getExecutionMerkle().equals(superHash) == false)
				continue;
			
			superStateVoteBlock = stateVoteBlock;
			
			if (superStateVoteBlock.isHeader() == false)
				break;
		}
		
		return superStateVoteBlock;
	}
	
	synchronized Collection<StateVoteBlock> tryComplete()
	{
		StateVoteBlock superStateVoteBlock = getSuper();
		if (superStateVoteBlock == null)
			return null;
		
		if (superStateVoteBlock.isHeader())
			return null;

		List<StateVoteBlock> superStateVoteBlocks = new ArrayList<StateVoteBlock>(this.stateVoteBlocks.size());
		for (StateVoteBlock stateVoteBlock : this.stateVoteBlocks.values())
		{
			if (stateVoteBlock.getExecutionMerkle().equals(superStateVoteBlock.getExecutionMerkle()) == false)
				continue;
			
			superStateVoteBlocks.add(stateVoteBlock);
		}
		
		this.selectedExecutionMerkle = superStateVoteBlock.getExecutionMerkle();
		return superStateVoteBlocks; 
	}
	
	public boolean isStale()
	{
		if (Time.getSystemTime() - this.timestamp >= TimeUnit.SECONDS.toMillis(Constants.PRIMITIVE_STALE))
			return true;

		return false;
	}
	
	public boolean isCompleted()
	{
		return this.selectedExecutionMerkle != null ? true : false; 
	}
	
	@Override
	public String toString()
	{
		return this.ID+" "+this.stateVoteBlocks.size()+" "+Block.toHeight(this.block)+"@"+this.block+" "+this.voteWeight+"/"+this.voteThreshold;
	}
}
