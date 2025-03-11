package org.radix.hyperscale.ledger.exceptions;

import java.util.Objects;

import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.exceptions.ValidationException;

public class AtomNotFoundException extends ValidationException
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 6964432881299046502L;
	
	private final Hash atom;
	
	public AtomNotFoundException(String message, Hash atom)
	{
		super(message);

		Hash.notZero(atom, "Atom hash is ZERO");
		this.atom = atom;
	}

	public AtomNotFoundException(Hash atom)
	{
		this("The atom "+Objects.requireNonNull(atom)+" is not found", atom);
	}

	public Hash getAtom() 
	{
		return this.atom;
	}
}
