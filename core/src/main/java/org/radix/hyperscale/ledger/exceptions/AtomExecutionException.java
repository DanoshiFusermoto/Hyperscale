package org.radix.hyperscale.ledger.exceptions;

import java.util.Objects;

import org.radix.hyperscale.ledger.PendingAtom;

class AtomExecutionException extends Exception
{
	/**
	 * 
	 */
	private static final long serialVersionUID = -515119267346342997L;
	
	private final PendingAtom pendingAtom;
	private final Exception thrown;
	
	public AtomExecutionException(final PendingAtom pendingAtom, final Exception thrown)
	{
		super("Pending atom "+Objects.requireNonNull(pendingAtom, "Pending atom is null").getHash()+" threw execution exception", Objects.requireNonNull(thrown, "Thrown exception is null"));
		
		this.pendingAtom = pendingAtom;
		this.thrown = thrown;
	}
	
	public PendingAtom getPendingAtom()
	{
		return this.pendingAtom;
	}
	
	public Exception thrown()
	{
		return this.thrown;
	}
}
