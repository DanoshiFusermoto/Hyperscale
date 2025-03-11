package org.radix.hyperscale.ledger.events;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

import org.radix.hyperscale.events.SyncChangeEvent;
import org.radix.hyperscale.ledger.BlockHeader;
import org.radix.hyperscale.ledger.PendingAtom;
import org.radix.hyperscale.ledger.StateAccumulator;
import org.radix.hyperscale.ledger.StateVoteCollector;

public final class SyncAcquiredEvent extends SyncChangeEvent 
{
	private final BlockHeader head;
	private final StateAccumulator accumulator;
	private final Set<PendingAtom> atoms;
	private final Set<StateVoteCollector> stateVoteCollectors;
	
	public SyncAcquiredEvent(final BlockHeader head, final StateAccumulator accumulator, final Collection<PendingAtom> atoms, final Collection<StateVoteCollector> stateVoteCollectors)
	{
		super();
		
		this.head = Objects.requireNonNull(head, "Head is null");
		this.accumulator = Objects.requireNonNull(accumulator, "Accumulator is null");
		this.atoms = Collections.unmodifiableSet(new LinkedHashSet<PendingAtom>(Objects.requireNonNull(atoms, "Pending atoms is null")));
		this.stateVoteCollectors = Collections.unmodifiableSet(new LinkedHashSet<StateVoteCollector>(Objects.requireNonNull(stateVoteCollectors, "State vote collectors is null")));
	}

	public BlockHeader getHead() 
	{
		return this.head;
	}

	public StateAccumulator getAccumulator() 
	{
		return this.accumulator;
	}
	
	public Collection<PendingAtom> getAtoms()
	{
		return this.atoms;
	}
	
	public Collection<StateVoteCollector> getStateVoteCollectors()
	{
		return this.stateVoteCollectors;
	}
}
