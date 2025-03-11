package org.radix.hyperscale.ledger.events;

import java.util.Objects;

import org.radix.hyperscale.events.Event;
import org.radix.hyperscale.ledger.ProgressRound;
import org.radix.hyperscale.ledger.ProgressRound.State;

public final class ProgressPhaseEvent implements Event 
{
	/** The progress round **/
	private final ProgressRound progressRound;

	/** The phase the progress round is currently in **/
	private final ProgressRound.State progressPhase;

	/** The local wall clock timestamp for this progress phase event **/
	private final long timestamp;
	
	public ProgressPhaseEvent(final ProgressRound progressRound)
	{
		this.progressRound = Objects.requireNonNull(progressRound, "Progress round is null");
		this.progressPhase = progressRound.getState();
		this.timestamp = System.currentTimeMillis();
	}
	
	public ProgressRound getProgressRound()
	{
		return this.progressRound;
	}
	
	public State getProgressPhase()
	{
		return this.progressPhase;
	}
	
	public long getTimestamp()
	{
		return this.timestamp;
	}
}
