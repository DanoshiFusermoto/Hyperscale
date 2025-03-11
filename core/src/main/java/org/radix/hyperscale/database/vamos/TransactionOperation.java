package org.radix.hyperscale.database.vamos;

import java.util.Objects;

import org.radix.hyperscale.utils.Numbers;

class TransactionOperation 
{
	private final Operation operation;
	private final InternalKey key;
	private final byte[] data;
	
	TransactionOperation(Operation operation, InternalKey key)
	{
		this(operation, key, null);
	}
	
	TransactionOperation(Operation operation, InternalKey key, byte[] data)
	{
		this(operation, key, data, 0, data.length);
	}

	TransactionOperation(Operation operation, InternalKey key, byte[] data, int dataOffset, int dataLength)
	{
		Objects.requireNonNull(operation, "Operation is null");
		Objects.requireNonNull(key, "Hash key is null");

		this.operation = operation;
		this.key = key;
		
		if (operation.equals(Operation.DELETE))
		{
			if (data != null)
				throw new IllegalArgumentException("Data must be null on a DELETE operation");
			
			this.data = null;
		}
		else
		{
			Objects.requireNonNull(data, "Data is null");
			Numbers.isZero(data.length, "Data is empty");
			
			this.data = new byte[dataLength];
			System.arraycopy(data, dataOffset, this.data, 0, dataLength);
		}
	}

	Operation getOperation() 
	{
		return this.operation;
	}

	InternalKey getKey() 
	{
		return this.key;
	}

	byte[] getData() 
	{
		return this.data;
	}
}
