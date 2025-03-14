package org.radix.hyperscale.ledger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.eclipse.collections.api.factory.Sets;
import org.radix.hyperscale.Constants;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.crypto.Identity;
import org.radix.hyperscale.time.Time;

public class ProgressRound
{
	public enum State
	{
		NONE, PROPOSING, TRANSITION, VOTING, COMPLETED
	}
	
	private State state;
	private final long clock;
	private final Epoch epoch;
	private final long startedAt;

	private final long driftClock;
	private volatile long driftMilli;

	private final List<Hash> proposals;
	private final Set<Identity> proposed;
	private final Set<Identity> proposers;
	private final long proposalThreshold;
	private volatile long proposalWeight;
	private volatile long proposalsTimeout;
	private volatile int primaryProposed;

	private volatile long transitionTimeout;

	private final Set<Identity> voted;
	private final long voteThreshold;
	private volatile long voteWeight;
	private volatile long voteTimeout;
	
	private final long totalVotePower;
	private volatile long proposeStartAt;
	private volatile long transitionStartAt;
	private volatile long voteStartAt;
	private volatile long completedAt;
	
	ProgressRound(final long clock, final Set<Identity> proposers, final long proposersVotePower, final long totalVotePower, final long driftClock)
	{
		this.startedAt = Time.getSystemTime();
		this.completedAt = -1;

		this.state = State.NONE;
		this.clock = clock;
		this.driftClock = driftClock;
		this.totalVotePower = totalVotePower;
		this.epoch = Epoch.from(clock / Constants.BLOCKS_PER_EPOCH);

		this.voteTimeout = 0;
		this.voteWeight = 0;
		this.voteThreshold = ValidatorHandler.twoFPlusOne(totalVotePower);
		this.voted = Sets.mutable.<Identity>ofInitialCapacity(8).asSynchronized();

		this.primaryProposed = 0;
		this.proposalsTimeout = 0;
		this.proposed = Sets.mutable.<Identity>ofInitialCapacity(proposers.size()).asSynchronized();
		this.proposers = Sets.immutable.<Identity>ofAll(proposers).castToSet();
		this.proposals = new ArrayList<Hash>(proposers.size());
		this.proposalWeight = 0;
		this.proposalThreshold = ValidatorHandler.twoFPlusOne(proposersVotePower);
	
		this.transitionTimeout = 0;
	}

	public long clock() 
	{
		return this.clock;
	}

	public long driftClock() 
	{
		return this.driftClock;
	}

	public long driftMilli() 
	{
		if (this.voteStartAt == 0)
			return 0;
		
		return this.driftMilli - this.voteStartAt;
	}

	public Epoch epoch() 
	{
		return this.epoch;
	}

	public State getState()
	{
		return this.state;
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
	
	long calculateDriftAdjustment(int timeoutDuration)
	{
		int adjustedTimeoutDuration = timeoutDuration;
		
		// Shard is ahead
		if (this.driftClock > 0)
			adjustedTimeoutDuration = (int) (timeoutDuration / (this.driftClock+1));
		// Shard is behind 
		else if (this.driftClock < 0)
			adjustedTimeoutDuration = (int) (timeoutDuration * (Math.log(Math.abs(this.driftClock))+1));
		
		return adjustedTimeoutDuration;
	}
	
	State stepState()
	{
		if (this.state.equals(State.NONE))
		{
			this.state = State.PROPOSING;
			this.proposeStartAt = Time.getSystemTime();
			this.proposalsTimeout = this.proposeStartAt + calculateDriftAdjustment(Constants.PROPOSAL_PHASE_TIMEOUT_MS);
		}
		else if (this.state.equals(State.PROPOSING))
		{
			this.state = State.TRANSITION;
			this.transitionStartAt = Time.getSystemTime();
			this.transitionTimeout = this.transitionStartAt + calculateDriftAdjustment(Constants.TRANSITION_PHASE_TIMEOUT_MS);
		}
		else if (this.state.equals(State.TRANSITION))
		{
			this.state = State.VOTING;
			this.voteStartAt = Time.getSystemTime();
			this.voteTimeout = this.voteStartAt + calculateDriftAdjustment(Constants.VOTE_PHASE_TIMEOUT_MS);
		}
		else if (this.state.equals(State.VOTING))
		{
			this.state = State.COMPLETED;
			this.completedAt = Time.getSystemTime();
		}
		else
			throw new IllegalStateException("Progress round is COMPLETED");
		
		return this.state;
	}
	
	boolean vote(final Identity voter, final long votePower)
	{
		if (this.voted.add(voter) == false)
			return false;
		
		this.voteWeight += votePower;
		
		if (this.driftMilli != 0)
		{
			long delta = (System.currentTimeMillis() - this.driftMilli);
			this.driftMilli += (delta / 2); 
		}
		else
			this.driftMilli = System.currentTimeMillis();
		
		return true;
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
		return this.voteWeight >= this.voteThreshold;
	}

	public boolean isVoteLatent()
	{
		if (this.voteStartAt == 0)
			return false;
		
		return Time.getSystemTime() > this.voteStartAt + (Constants.MINIMUM_ROUND_DURATION_MILLISECONDS / 3);
	}

	public boolean isVoteTimedout()
	{
		if (this.voteTimeout == 0)
			return false;
		
		return Time.getSystemTime() > this.voteTimeout;
	}
	
	public boolean hasVoted(final Identity identity) 
	{
		return this.voted.contains(identity);
	}
	
	public boolean isTransitionLatent()
	{
		if (this.transitionStartAt == 0)
			return false;

		return Time.getSystemTime() > this.transitionStartAt + (Constants.MINIMUM_ROUND_DURATION_MILLISECONDS / 3);
	}

	public boolean isTransitionTimedout()
	{
		if (this.transitionTimeout == 0)
			return false;
		
		return Time.getSystemTime() > this.transitionTimeout;
	}

	boolean propose(final Hash proposal, final Identity identity, final long votePower)
	{
		// TODO penalties
		if (canPropose(identity) == false)
			return false;
		
		if (this.proposed.add(identity) == false)
			return false;

		this.proposals.add(proposal);
		this.proposalWeight += votePower;
		
		if (this.proposers.contains(identity))
			this.primaryProposed++;
		
		return true;
	}
	
	List<Hash> getProposals()
	{
		return Collections.unmodifiableList(new ArrayList<Hash>(this.proposals));
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
		return this.proposalWeight > this.proposalThreshold;
	}

	public boolean isProposalsLatent()
	{
		if (this.proposeStartAt == 0)
			return false;

		return Time.getSystemTime() > this.proposeStartAt + (Constants.MINIMUM_ROUND_DURATION_MILLISECONDS / 3);
	}

	public boolean isProposalsTimedout()
	{
		if (this.proposalsTimeout == 0)
			return false;
		
		return Time.getSystemTime() > this.proposalsTimeout;
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
		return this.primaryProposed == this.proposers.size();
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
	
	@Override 
	public String toString()
	{
		String output = Long.toString(this.clock)+":"+this.driftClock;
		if (this.state.equals(State.COMPLETED))
			output += ":"+driftMilli();
		if (this.completedAt > 0)
			output += " "+this.completedAt+" "+getDuration()+"ms";
		
		output += " (PI/PN) "+this.proposed.size()+"/"+this.proposers.size()+" (PW/PT) "+this.proposalWeight+"/"+this.proposalThreshold+" (VW/VT) "+this.voteWeight+"/"+this.voteThreshold+" (TW) "+this.totalVotePower+" (TM)";
		
		return output;
	}
}
