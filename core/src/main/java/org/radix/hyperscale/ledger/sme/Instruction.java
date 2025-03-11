package org.radix.hyperscale.ledger.sme;

public interface Instruction 
{
	@Override
	public int hashCode();
	
	@Override
	public boolean equals(Object other);
}
