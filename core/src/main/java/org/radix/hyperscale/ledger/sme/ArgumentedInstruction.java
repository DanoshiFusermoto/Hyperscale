package org.radix.hyperscale.ledger.sme;

import java.util.Arrays;
import java.util.Objects;

public abstract class ArgumentedInstruction implements Instruction
{
	static final Object[] EMPTY_ARGUMENTS = new Object[0];
	
	private final Object[] arguments;
	
	ArgumentedInstruction(final Object[] arguments)
	{
		Objects.requireNonNull(arguments, "Instruction arguments is null");
		
		this.arguments = Arrays.copyOf(arguments, arguments.length);
	}
	
	@Override
	public int hashCode() 
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((arguments == null) ? 0 : arguments.hashCode());
		return result;
	}

	@Override
	public boolean equals(final Object obj) 
	{
		if (this == obj)
			return true;
		
		if (obj == null)
			return false;
		
		if (obj instanceof ArgumentedInstruction other)
		{
			if (this.arguments == null) 
			{
				if (other.arguments != null)
					return false;
			} 
			else if (!Arrays.equals(arguments, other.arguments))
				return false;
			
			return true;
		}
		
		return false;
	}

	public final Object[] getArguments()
	{
		return this.arguments;
	}
}