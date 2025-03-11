package org.radix.hyperscale.ledger.sme;

import java.util.Objects;

import org.radix.hyperscale.crypto.ComputeKey;
import org.radix.hyperscale.crypto.Identity;
import org.radix.hyperscale.ledger.StateAddress;
import org.radix.hyperscale.ledger.StateLockMode;
import org.radix.hyperscale.ledger.Substate.NativeField;
import org.radix.hyperscale.ledger.primitives.Blob;
import org.radix.hyperscale.ledger.sme.exceptions.StateMachineExecutionException;

public final class Persist implements Instruction
{
	private final Blob blob;
	private final StateAddress blobAddress;
	private final Identity authority;

	public Persist(final Blob blob)
	{
		this(blob, ComputeKey.NULL.getIdentity());
	}

	public Persist(final Blob blob, final Identity authority)
	{
		Objects.requireNonNull(blob, "Blob is null");
		
		this.blob = blob;
		this.authority = authority;
		this.blobAddress = StateAddress.from(Blob.class, blob.getHash());
	}
	
	public Blob getBlob()
	{
		return this.blob;
	}
	
	public StateAddress getBlobAddress()
	{
		return this.blobAddress;
	}
	
	@Override
	public int hashCode() 
	{
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((authority == null) ? 0 : authority.hashCode());
		result = prime * result + ((blob == null) ? 0 : blob.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) 
	{
		if (this == obj)
			return true;
	
		if (!super.equals(obj))
			return false;
		
		if (getClass() != obj.getClass())
			return false;
		
		Persist other = (Persist) obj;
		if (authority == null) 
		{
			if (other.authority != null)
				return false;
		} 
		else if (!authority.equals(other.authority))
			return false;
		
		if (blob == null) 
		{
			if (other.blob != null)
				return false;
		} else if (!blob.equals(other.blob))
			return false;
		
		return true;
	}

	void lock(final StateMachine stateMachine) 
	{
		stateMachine.lock(getBlobAddress(), StateLockMode.WRITE);
	}
	
	void execute(final StateMachine stateMachine) throws StateMachineExecutionException, ReflectiveOperationException
	{
		stateMachine.assertCreate(getBlobAddress(), this.authority);
		stateMachine.set(getBlobAddress(), NativeField.ATOM, stateMachine.getPendingAtom().getHash(), this.authority);
		if (this.authority != ComputeKey.NULL.getIdentity())
			stateMachine.associate(getBlobAddress(), this.authority);
	}
}