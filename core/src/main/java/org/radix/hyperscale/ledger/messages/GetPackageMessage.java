package org.radix.hyperscale.ledger.messages;

import java.util.Objects;

import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.ledger.StateAddress;
import org.radix.hyperscale.network.messages.Message;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.SerializerId2;
import org.radix.hyperscale.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.messages.package.get")
public final class GetPackageMessage extends Message
{
	@JsonProperty("address")
	@DsonOutput(Output.ALL)
	private StateAddress address;

	@JsonProperty("atom")
	@DsonOutput(Output.ALL)
	private Hash atom;

	@SuppressWarnings("unused")
	private GetPackageMessage()
	{
		super();
	}
	
	public GetPackageMessage(final Hash atom, final StateAddress address)
	{
		super();
		
		Objects.requireNonNull(atom, "Atom is null");
		Hash.notZero(atom, "Atom is ZERO");
		
		this.atom = atom;
		this.address = Objects.requireNonNull(address, "State address is null");
	}

	public Hash getAtom()
	{
		return this.atom;
	}
	
	public StateAddress getAddress()
	{
		return this.address;
	}
}
