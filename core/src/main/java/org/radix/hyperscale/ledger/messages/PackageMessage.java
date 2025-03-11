package org.radix.hyperscale.ledger.messages;

import java.util.Objects;

import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.ledger.sme.PolyglotPackage;
import org.radix.hyperscale.network.messages.Message;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.SerializerId2;
import org.radix.hyperscale.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.messages.package")
public final class PackageMessage extends Message
{
	@JsonProperty("package")
	@DsonOutput(Output.ALL)
	private PolyglotPackage pakage;

	@JsonProperty("atom")
	@DsonOutput(Output.ALL)
	private Hash atom;

	@SuppressWarnings("unused")
	private PackageMessage()
	{
		super();
	}
	
	public PackageMessage(final Hash atom, final PolyglotPackage pakage)
	{
		super();
		
		Objects.requireNonNull(atom, "Atom is null");
		Hash.notZero(atom, "Atom is ZERO");
		Objects.requireNonNull(pakage, "Package is null");
		
		this.atom = atom;
		this.pakage = pakage;
	}

	public Hash getAtom()
	{
		return this.atom;
	}
	
	public PolyglotPackage getPackage()
	{
		return this.pakage;
	}
}
