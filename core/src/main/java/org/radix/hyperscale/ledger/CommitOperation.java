package org.radix.hyperscale.ledger;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;

import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.ledger.primitives.Atom;
import org.radix.hyperscale.ledger.sme.SubstateTransitions;
import org.radix.hyperscale.time.Time;

import java.util.Objects;

import com.google.common.collect.ImmutableList;

public class CommitOperation
{
	private final CommitDecision	decision;
	private final long 				timestamp;
	private final BlockHeader		head;
	private final Atom				atom;
	private final SubstateOpLog		substateOpLog;
	private final List<SubstateTransitions> transitions;
	private final List<Entry<StateAddress, Hash>> associations;
	
	public CommitOperation(final CommitDecision decision, final BlockHeader head, final Atom atom, 
			               final Collection<SubstateTransitions> transitions, final Collection<Entry<StateAddress, Hash>> associations, final SubstateOpLog substateOpLog)
	{
		this.decision = Objects.requireNonNull(decision, "Decision is null");
		this.head = Objects.requireNonNull(head, "Head is null");
		this.atom = Objects.requireNonNull(atom, "Atom is null");
		this.timestamp = Time.getLedgerTimeMS();

		if (this.decision.equals(CommitDecision.REJECT))
		{
			if (Objects.requireNonNull(transitions, "Substate transitions is null").isEmpty() == false)
				throw new IllegalArgumentException("Rejections must have not have substates for atom "+atom.getHash());
			if (Objects.requireNonNull(associations, "Associations is null").isEmpty() == false)
				throw new IllegalArgumentException("Rejections must have not have associations for atom "+atom.getHash());
			if (Objects.requireNonNull(substateOpLog, "Substate snapshot is null").isEmpty() == false)
				throw new IllegalArgumentException("Rejections must have not have populated substate snapshot for atom "+atom.getHash());

			this.substateOpLog = null;
			this.transitions = Collections.emptyList();
			this.associations = Collections.emptyList();
		}
		else
		{
			// TODO keep allowing empty commit operations?
			Objects.requireNonNull(substateOpLog, "Substate snapshot is null");
			Objects.requireNonNull(transitions, "Substate transitions is null");
			Objects.requireNonNull(associations, "Associations is null");

			this.substateOpLog = substateOpLog;
			this.transitions = transitions.isEmpty() ? Collections.emptyList() : ImmutableList.copyOf(transitions);
			this.associations = associations.isEmpty() ? Collections.emptyList() : ImmutableList.copyOf(associations);
		}
	}

	public CommitDecision getDecision()
	{
		return this.decision;
	}

	public long getTimestamp() 
	{
		return this.timestamp;
	}

	public BlockHeader getHead() 
	{
		return this.head;
	}

	public Atom getAtom() 
	{
		return this.atom;
	}
	
	public SubstateOpLog getSubstateOpLog()
	{
		return this.substateOpLog;
	}

	public List<SubstateTransitions> getSubstateTransitions()
	{
		return this.transitions;
	}

	public List<Entry<StateAddress, Hash>> getAssociations() 
	{
		return this.associations;
	}

	@Override
	public int hashCode() 
	{
		return this.head.hashCode();
	}

	@Override
	public boolean equals(Object object) 
	{
		if (object == null)
			return false;
		
		if (object == this)
			return true;
		
		if (object instanceof CommitOperation commitOperation)
		{
			if (this.head.equals(commitOperation.getHead()) == false)
				return false;
			
			if (this.atom.equals(commitOperation.getAtom()) == false)
				return false;

			return true;
		}
		
		return false;
	}

	@Override
	public String toString() 
	{
		return this.head.getHash()+" "+this.atom.getHash()+" ["+getSubstateTransitions().toString()+"] ("+(getAssociations().toString())+")";
	}
}
