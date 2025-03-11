package org.radix.hyperscale.ledger;

import java.util.Objects;

import org.radix.hyperscale.Context;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;
import org.radix.hyperscale.time.Time;

public final class AtomStatus
{
	private static final Logger atomStatusLog = Logging.getLogger("atomstatus");

	public enum State
	{
		NONE(0), PREPARED(1), ACCEPTED(2), PROVISIONING(3), PROVISIONED(4), EXECUTING(5), FINALIZING(6), FINALIZED(7), COMPLETED(8);
	
		private static int NUM_STATES = State.values().length;
		private final int index;
		
		State(int index)
		{
			this.index = index;
		}
		
		public int index()
		{
			return this.index;
		}
		
		public boolean before(final State state)
		{
			return this.index < state.index;
		}
	
		public boolean after(final State state)
		{
			return this.index > state.index;
		}
	}
	
	private final Context context;
	private final Hash atom;
	private final State[] history;
	private State state;
	private long timestamp;
	private Exception exception;
	
	AtomStatus(final Context context, final Hash atom)
	{
		this.context = Objects.requireNonNull(context, "Context is null");
		this.atom = Objects.requireNonNull(atom, "Atom hash is null");
		this.history = new State[State.NUM_STATES];
		this.timestamp = Time.getSystemTime();
		this.state = State.NONE;
	}
	
	@Override
	public String toString()
	{
		return this.state.name() +" @ "+this.timestamp;
	}
	
	synchronized State current()
	{
		return this.state;
	}
	
	public boolean was(final State state) 
	{
		Objects.requireNonNull(state, "Atom status state is null");
		if (this.state.equals(state))
			return true;
		
		return this.history[state.index] == null ? false : true;
	}
	
	long timestamp()
	{
		return this.timestamp;
	}
	
	public synchronized boolean current(final State state)
	{
		return this.state.equals(Objects.requireNonNull(state, "Atom status state is null"));
	}
	
	public synchronized boolean before(final State state)
	{
		return this.state.before(Objects.requireNonNull(state, "Atom status state is null"));
	}

	public synchronized boolean after(final State state)
	{
		return this.state.after(Objects.requireNonNull(state, "Atom status state is null"));
	}

	public synchronized Exception thrown()
	{
		return this.exception;
	}

	public synchronized void thrown(final Exception ex)
	{
		this.exception = Objects.requireNonNull(ex, "Exception is null");
		set(AtomStatus.State.FINALIZING);
	}

	 State set(final State state)
	 {
		Objects.requireNonNull(state, "Atom status state is null");
		
		final State oldState;
		synchronized(this)
		{
			if (this.state.equals(state))
				throw new IllegalStateException("State of pending atom "+this.atom+" is already set to "+state);
		
			// TODO review FINALIZED skipping needed by sync
			if (state.equals(AtomStatus.State.FINALIZED) == false && state.equals(AtomStatus.State.COMPLETED) == false && this.state.index() < state.index()-1 && this.exception == null)
				throw new IllegalStateException("Pending atom "+this.atom+" can not set to state "+state+" from "+this.state);
			
			this.timestamp = Time.getSystemTime();
			this.history[state.index] = state;
			
			oldState = this.state;
			this.state = state;
		}
		
		if (atomStatusLog.hasLevel(Logging.DEBUG) && this.atom.asLong() % 1000 == 0)
		{
			BlockHeader head = this.context.getLedger().getHead();
			atomStatusLog.debug(this.context.getName()+": Atom "+this.atom+" is now "+state+" at "+Time.getSystemTime()+":"+head.getHeight()+" "+head.getHash());
		}
		
		return oldState;
	}
}
