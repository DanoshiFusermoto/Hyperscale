package org.radix.hyperscale.ledger.sme;

import java.util.Objects;

import org.radix.hyperscale.common.BasicObject;
import org.radix.hyperscale.common.Primitive;
import org.radix.hyperscale.ledger.StateAddress;
import org.radix.hyperscale.ledger.StateAddressable;
import org.radix.hyperscale.ledger.StateContext;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@StateContext("package")
public abstract class Package extends BasicObject implements Primitive, StateAddressable
{
	@JsonProperty("address")
	@DsonOutput(Output.ALL)
	private StateAddress address;

	Package()
	{
		super();
	}

	protected Package(final StateAddress address)
	{
		Objects.requireNonNull(address, "Package state address is null");
		if (address.context().contentEquals(getClass().getAnnotation(StateContext.class).value()) == false)
			throw new IllegalArgumentException("Expected context '"+Package.class.getAnnotation(StateContext.class).value()+"' but is '"+address.context()+"'");
		
		this.address = address;
	}
	
	@Override
	public StateAddress getAddress()
	{
		return this.address;
	}
}