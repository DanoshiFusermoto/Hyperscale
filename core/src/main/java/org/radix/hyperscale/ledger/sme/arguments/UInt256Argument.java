package org.radix.hyperscale.ledger.sme.arguments;

import java.util.Objects;

import org.radix.hyperscale.ledger.sme.Argument;
import org.radix.hyperscale.ledger.sme.ArgumentContext;
import org.radix.hyperscale.utils.UInt256;

@ArgumentContext("uint256")
public class UInt256Argument implements Argument<UInt256>
{
	private final UInt256 integer;
	
	public UInt256Argument(long integer)
	{
		this(UInt256.from(integer));
	}

	public UInt256Argument(String integer)
	{
		this(UInt256.from(integer));
	}

	public UInt256Argument(UInt256 integer)
	{
		this.integer = Objects.requireNonNull(integer, "Integer is null");
	}
	
	public UInt256 get()
	{
		return this.integer;
	}
}