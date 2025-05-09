package org.radix.hyperscale.ledger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.eclipse.collections.api.factory.Maps;
import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.map.primitive.MutableObjectLongMap;
import org.eclipse.collections.impl.factory.primitive.ObjectLongMaps;
import org.radix.hyperscale.Context;
import org.radix.hyperscale.collections.Bloom;
import org.radix.hyperscale.crypto.CryptoException;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.crypto.Identity;
import org.radix.hyperscale.crypto.bls12381.BLS12381;
import org.radix.hyperscale.crypto.bls12381.BLSPublicKey;
import org.radix.hyperscale.crypto.bls12381.BLSSignature;
import org.radix.hyperscale.time.Time;
import org.radix.hyperscale.utils.Numbers;

public class ProgressRound
{
	public enum State
	{
		NONE, PROPOSING, TRANSITION, VOTING, COMPLETED
	}

	private final Context context;
	
	private final long clock;
	private final Epoch epoch;

	private final long createdAt;
	private volatile State state;
	private volatile long startedAt;
	private volatile long proposeStartAt;
	private volatile long transitionStartAt;
	private volatile long transitionUpdateAt;
	private volatile long voteStartAt;
	private volatile long completedAt;

	private final List<Long> driftSamples;
	
	private volatile long phaseLatentAt;
	private volatile long phaseTimeoutAt;

	private final Map<Hash, BlockHeader> proposals;
	private final Set<Identity> proposed;
	private final Set<Identity> proposers;
	private final long proposalThreshold;
	private volatile long proposalWeight;
	private volatile int primariesProposed;

	private final Map<Identity, BlockVote> votes;
	private final long voteThreshold;
	private final long totalVotePower;
	private volatile long voteWeight;
	
	private volatile QuorumCertificate view;
	private volatile QuorumCertificate certificate;

	ProgressRound(final Context context, final BlockHeader head)
	{
		Objects.requireNonNull(context, "Context is null");
		this.context = context;

		this.createdAt = head.getTimestamp();
		this.startedAt = this.createdAt;
		this.completedAt = this.createdAt+Ledger.definitions().roundInterval();

		this.view = head.getView();
		this.certificate = new QuorumCertificate(head.getHash(), head.getView(), head.getHash());
		
		this.state = State.COMPLETED;
		this.clock = head.getHeight();
		
		this.driftSamples = new ArrayList<Long>();

		this.totalVotePower = 1;
		this.epoch = Epoch.from(this.clock / Ledger.definitions().proposalsPerEpoch());

		this.voteWeight = this.totalVotePower;
		this.voteThreshold = this.totalVotePower;
		this.votes = Collections.emptyMap();

		this.primariesProposed = 1;
		this.proposed = Sets.immutable.of(head.getProposer()).castToSet();
		this.proposers = Sets.immutable.of(head.getProposer()).castToSet();
		this.proposals = Maps.immutable.of(head.getHash(), head).castToMap();
		this.proposalWeight = this.totalVotePower;
		this.proposalThreshold = this.totalVotePower;
	}
	
	ProgressRound(final Context context, final long clock, final Set<Identity> proposers, final long proposersVotePower, final long totalVotePower)
	{
		Objects.requireNonNull(context, "Context is null");
		this.context = context;
		
		this.createdAt = Time.getSystemTime();
		this.completedAt = -1;

		Numbers.isNegative(clock, "Round clock is negative");

		this.state = State.NONE;
		this.clock = clock;
		
		this.driftSamples = new ArrayList<Long>();

		this.totalVotePower = totalVotePower;
		this.epoch = Epoch.from(clock / Ledger.definitions().proposalsPerEpoch());

		Numbers.isNegative(proposersVotePower, "Primary proposers vote power is negative");
		Numbers.isNegative(totalVotePower, "Total vote power is negative");
		
		this.voteWeight = 0;
		this.voteThreshold = ValidatorHandler.twoFPlusOne(totalVotePower);
		this.votes = Maps.mutable.<Identity, BlockVote>ofInitialCapacity(8).asSynchronized();

		Objects.requireNonNull(proposers, "Primary proposers set is null");
		Numbers.isZero(proposers.size(), "Primary proposers is empty");

		this.primariesProposed = 0;
		this.proposed = Sets.mutable.<Identity>ofInitialCapacity(proposers.size()).asSynchronized();
		this.proposers = Sets.immutable.<Identity>ofAll(proposers).castToSet();
		this.proposals = Maps.mutable.<Hash, BlockHeader>ofInitialCapacity(proposers.size()).asSynchronized();
		this.proposalWeight = 0;
		this.proposalThreshold = ValidatorHandler.twoFPlusOne(proposersVotePower);
	}

	public long clock() 
	{
		return this.clock;
	}

	public int drift() 
	{
		if (this.startedAt == 0 || this.driftSamples.isEmpty())
			return 0;

		Collections.sort(this.driftSamples);
		
		long medianValue;
		int middleSlot = this.driftSamples.size() / 2;
		if (this.driftSamples.size() % 2 == 0)
            medianValue = (long) ((this.driftSamples.get(middleSlot - 1) + this.driftSamples.get(middleSlot)) / 2.0);
        else
        	medianValue = this.driftSamples.get(middleSlot);
		
		return (int) ((medianValue - this.proposeStartAt) / 2);
	}
	
	public long phaseLatentAt()
	{
		return this.phaseLatentAt;
	}

	public long phaseTimeoutAt()
	{
		return this.phaseTimeoutAt;
	}

	public Epoch epoch() 
	{
		return this.epoch;
	}

	public State getState()
	{
		return this.state;
	}
	
	public QuorumCertificate getView()
	{
		if (this.startedAt == 0)
			throw new IllegalStateException("Progress round "+this.clock+" is not started");

		return this.view;
	}
	
	void start(final QuorumCertificate view)
	{
		Objects.requireNonNull(view, "Local view QC is null");
		
		if (this.startedAt != 0)
			throw new IllegalStateException("Progress round "+this.clock+" is already started");
		
		this.startedAt = System.currentTimeMillis();
		this.view = view;
	}
	
	boolean isStarted()
	{
		return this.startedAt != 0;
	}
	
	long startedAt()
	{
		return this.startedAt;
	}

	/** Terminates this proposal round, fast forwarding to the completed state.
	 * 
	 *  Useful in scenarios where a local instance is behind the network and the outcome of a round
	 *  has already been decided (such as a proposal already acquiring a quorum).
	 */
	void terminate()
	{
		this.state = State.COMPLETED;
		this.completedAt = Time.getSystemTime();
	}
	
	State stepState()
	{
		if (this.startedAt == 0)
			throw new IllegalStateException("Progress round "+this.clock+" is not started");
		
		if (this.state.equals(State.NONE))
		{
			this.state = State.PROPOSING;
			this.proposeStartAt = Time.getSystemTime();
			this.phaseLatentAt = this.proposeStartAt + (Ledger.definitions().roundInterval() / 3);
			this.phaseTimeoutAt = this.proposeStartAt + Ledger.definitions().roundInterval();
		}
		else if (this.state.equals(State.PROPOSING))
		{
			this.state = State.TRANSITION;
			this.transitionStartAt = Time.getSystemTime();
			this.phaseLatentAt = this.transitionStartAt + (Ledger.definitions().roundInterval() / ProgressRound.State.values().length);
			this.phaseTimeoutAt = Long.MAX_VALUE;
			this.transitionUpdateAt = this.phaseLatentAt;
		}
		else if (this.state.equals(State.TRANSITION))
		{
			this.state = State.VOTING;
			this.voteStartAt = Time.getSystemTime();
			this.phaseLatentAt = this.voteStartAt + (Ledger.definitions().roundInterval() / 3);
			this.phaseTimeoutAt = this.voteStartAt + Ledger.definitions().roundInterval();
		}
		else if (this.state.equals(State.VOTING))
		{
			this.state = State.COMPLETED;
			this.phaseLatentAt = Long.MAX_VALUE;
			this.phaseTimeoutAt = Long.MAX_VALUE;
			this.completedAt = Time.getSystemTime();
		}
		else
			throw new IllegalStateException("Progress round is COMPLETED");
		
		return this.state;
	}
	
	void vote(final BlockVote vote)
	{
		if (this.votes.putIfAbsent(vote.getOwner().getIdentity(), vote) != null)
			throw new IllegalStateException("Vote already cast by "+vote.getOwner().getIdentity()+" for progress round "+this);
		
		this.voteWeight += vote.getWeight();
		
		if (vote.getOwner().getIdentity().equals(this.context.getNode().getIdentity()) == false)
			this.driftSamples.add(vote.progressedAt());
	}
	
	public long getVoteWeight() 
	{
		return this.voteWeight;
	}

	public long getVoteThreshold() 
	{
		return this.voteThreshold;
	}

	public boolean isVoteCompleted()
	{
		if (this.voteWeight >= this.totalVotePower)
			return true;

		return this.voteWeight >= this.voteThreshold && Time.getSystemTime() >= this.phaseLatentAt;
	}

	public boolean isVoteLatent()
	{
		if (this.voteStartAt == 0)
			return false;
		
		return Time.getSystemTime() >= this.phaseLatentAt;
	}

	public boolean isVoteTimedout()
	{
		if (this.voteStartAt == 0)
			return false;
		
		return Time.getSystemTime() >= this.phaseTimeoutAt;
	}
	
	public boolean hasVoted(final Identity identity) 
	{
		return this.votes.containsKey(identity);
	}
	
	public boolean isTransitionLatent()
	{
		if (this.transitionStartAt == 0)
			return false;

		return Time.getSystemTime() >= this.phaseLatentAt;
	}

	public boolean canTransitionUpdate()
	{
		if (this.transitionStartAt == 0)
			return false;

		return Time.getSystemTime() >= this.transitionUpdateAt;
	}
	
	public void transitionUpdated()
	{
		this.transitionUpdateAt = Time.getSystemTime() + Ledger.definitions().roundInterval() / ProgressRound.State.values().length;
	}

	boolean propose(final BlockHeader proposal, final long votePower)
	{
		// TODO penalties
		if (canPropose(proposal.getProposer()) == false)
			return false;
		
		if (this.proposed.add(proposal.getProposer()) == false)
			return false;

		this.proposals.put(proposal.getHash(), proposal);
		this.proposalWeight += votePower;
		
		if (this.proposers.contains(proposal.getProposer()))
			this.primariesProposed++;
		
		return true;
	}
	
	List<Hash> getProposals()
	{
		return Collections.unmodifiableList(new ArrayList<Hash>(this.proposals.keySet()));
	}
	
	public boolean hasProposals()
	{
		return this.proposals.isEmpty() == false;
	}
	
	public boolean canPropose(final Identity identity) 
	{
		if (this.proposed.contains(identity))
			return false;
		
		if (this.proposers.contains(identity))
			return true;
		
		if ((this.state.equals(State.TRANSITION) && isTransitionLatent()) ||
			this.state.equals(State.VOTING) || this.state.equals(State.COMPLETED))
			return true;
		
		return false;
	}

	public boolean hasProposed(final Identity identity) 
	{
		return this.proposed.contains(identity);
	}

	public long getProposeWeight() 
	{
		return this.proposalWeight;
	}

	public long getProposeThreshold() 
	{
		return this.proposalThreshold;
	}

	public boolean isProposalsCompleted()
	{
		if (this.proposalWeight >= this.totalVotePower)
			return true;
		
		return this.proposalWeight >= this.proposalThreshold && Time.getSystemTime() >= this.phaseLatentAt;
	}

	public boolean isProposalsLatent()
	{
		if (this.proposeStartAt == 0)
			return false;

		return Time.getSystemTime() >= this.phaseLatentAt;
	}

	public boolean isProposalsTimedout()
	{
		if (this.proposeStartAt == 0)
			return false;
		
		return Time.getSystemTime() >= this.phaseTimeoutAt;
	}

	public Set<Identity> getProposers()
	{
		synchronized(this.proposed)
		{
			return Sets.immutable.ofAll(this.proposed).castToSet();
		}
	}
	
	public boolean isFullyProposed()
	{
		return this.primariesProposed == this.proposers.size();
	}
	
	public List<Identity> getAbsentProposers()
	{
		boolean haveAbsent = this.proposers.size() != this.proposed.size();
		if (haveAbsent == false)
			return Collections.emptyList();
		
		final List<Identity> absentProposers = new ArrayList<Identity>(this.proposers.size());
		for (final Identity proposer : this.proposers)
		{
			if (this.proposed.contains(proposer))
				continue;
			
			absentProposers.add(proposer);
		}
		
		return Collections.unmodifiableList(absentProposers);
	}
	
	public long getDuration()
	{
		return (this.completedAt == -1 ? Time.getSystemTime() : this.completedAt) - this.proposeStartAt;
	}
	
	boolean hasCertificate()
	{
		return this.certificate != null;
	}
	
	QuorumCertificate buildCertificate(final QuorumCertificate extend, final Hash head) throws CryptoException
	{
		Objects.requireNonNull(extend, "QC to extend is null");
		Objects.requireNonNull(head, "Head hash is null");
		
	    synchronized(this)
	    {
	        if (this.certificate != null)
	            throw new IllegalStateException("Quorum certificate is already constructed for progress round "+this);

	        if (this.voteWeight < this.voteThreshold)
	            throw new IllegalStateException("Can not build a certificate when vote weight of "+this.voteWeight+" is less than vote threshold "+this.voteThreshold);

	        // Find a round proposal which has a quorum of votes
	        Hash selectedProposal = null;
	        final MutableObjectLongMap<Hash> weights = ObjectLongMaps.mutable.ofInitialCapacity(this.proposers.size());
	        
	        // Calculate weights for all proposals
	        for (final BlockVote blockVote : this.votes.values())
	            weights.addToValue(blockVote.getBlock(), blockVote.getWeight());
	        
	        // Check if any proposal has reached the threshold
	        for (Hash proposal : weights.keySet())
	        {
	            long weight = weights.get(proposal);
	            if (weight >= this.voteThreshold)
	            {
	            	selectedProposal = proposal;
	                break;
	            }
	        }
	        
	        // If no proposal has a quorum but we have surpassed vote threshold
	        if (selectedProposal == null)
	        {
	            // Check if it's mathematically impossible to reach a quorum
	            long[] sortedWeights = weights.values().toSortedArray();
	            
	            // Calculate if the highest weighted proposal can reach the threshold
	            long highestWeight = sortedWeights[sortedWeights.length - 1];
	            long remainingPotentialVotes = this.totalVotePower - this.voteWeight;
	            
	            // If with all remaining potential votes, it's impossible to form a quorum maintain the current view
	            if (highestWeight + remainingPotentialVotes < this.voteThreshold)
	            {
	            	this.certificate = extend;
	            	return this.certificate;
	            }
	            
	            // Check if vote distribution makes quorum impossible
	            int proposalsWithWeight = 0;
	            long totalPledgedWeight = 0;
	            
		        for (Hash proposal : weights.keySet())
		        {
		            long weight = weights.get(proposal);
	                if (weight > 0) 
	                {
	                    proposalsWithWeight++;
	                    totalPledgedWeight += weight;
	                }
	            }
	            
	            // If the votes are spread across too many proposals to ever reach quorum
	            // This is true when removing any single proposal's votes would still leave
	            // enough total votes, but no single proposal could get enough
	            if (proposalsWithWeight >= 2 && totalPledgedWeight >= this.voteThreshold &&
	                this.voteWeight - highestWeight >= this.voteThreshold / 2)
	            {
	            	this.certificate = extend;
	            	return this.certificate;
	            }
	        }

	        // If no quorum and no impossibility detected, return null
	        if (selectedProposal == null)
	            return null;

	        // Have a quorum, collect the votes and create a certificate
	        final Bloom signers = new Bloom(0.000001, this.votes.size());
	        final List<BLSPublicKey> keys = new ArrayList<>(this.votes.size());
	        final List<BLSSignature> signatures = new ArrayList<>(this.votes.size());
	        for (final BlockVote blockVote : this.votes.values())
	        {
	            if (blockVote.getBlock().equals(selectedProposal) == false)
	                continue;

	            keys.add(blockVote.getOwner());
	            signers.add(blockVote.getOwner().getIdentity().toByteArray());
	            signatures.add(blockVote.getSignature());
	        }

	        final BLSPublicKey key = BLS12381.aggregatePublicKey(keys);
	        final BLSSignature signature = BLS12381.aggregateSignatures(signatures);
	        final QuorumCertificate certificate = new QuorumCertificate(selectedProposal, extend, head, signers, key, signature);
	        this.certificate = certificate;
	        return certificate;
	    }
	}
	
	@Override 
	public String toString()
	{
		String output = Long.toString(this.clock)+":"+this.view.getHeight();
		if (this.state.equals(State.COMPLETED))
			output += ":"+drift()+"ms";
		if (this.completedAt > 0)
			output += " "+this.completedAt+" "+getDuration()+"ms";
		
		output += " (PI/PN) "+this.proposed.size()+"/"+this.proposers.size()+" (PW/PT) "+this.proposalWeight+"/"+this.proposalThreshold+" (VW/VT) "+this.voteWeight+"/"+this.voteThreshold+" (TW) "+this.totalVotePower+" (TM) "+this.view.getCurrent()+" (V)";
		
		return output;
	}
}
