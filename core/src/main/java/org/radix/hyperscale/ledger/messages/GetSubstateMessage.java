package org.radix.hyperscale.ledger.messages;

import java.util.Objects;

import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.ledger.StateAddress;
import org.radix.hyperscale.network.messages.Message;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.SerializerId2;
import org.radix.hyperscale.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.messages.state.get")
public final class GetSubstateMessage extends Message
{
	@JsonProperty("address")
	@DsonOutput(Output.ALL)
	private StateAddress address;

	@JsonProperty("version")
	@DsonOutput(Output.ALL)
	private Hash version;

	@SuppressWarnings("unused")
	private GetSubstateMessage()
	{
		super();
	}
	
	public GetSubstateMessage(final StateAddress address, final Hash version)
	{
		super();
		
		this.address = Objects.requireNonNull(address, "State address is null");
		this.version = Objects.requireNonNull(version, "Version hash is null");
	}

	public StateAddress getAddress()
	{
		return this.address;
	}
	
	public Hash getVersion()
	{
		return this.version;
	}
}
