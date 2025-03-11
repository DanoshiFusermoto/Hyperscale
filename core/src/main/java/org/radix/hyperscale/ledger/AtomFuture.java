package org.radix.hyperscale.ledger;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.radix.hyperscale.ledger.primitives.Atom;
import org.radix.hyperscale.ledger.primitives.AtomCertificate;

public final class AtomFuture extends CompletableFuture<AtomCertificate>
{
	private final Atom atom;
	
	public AtomFuture(final Atom atom)
	{
		this.atom = Objects.requireNonNull(atom, "Atom is null");
	}

	public Atom getAtom()
	{
		return this.atom;
	}
	
	public AtomCertificate getAtomCertificate() throws InterruptedException, ExecutionException
	{
		return this.get();
	}

}