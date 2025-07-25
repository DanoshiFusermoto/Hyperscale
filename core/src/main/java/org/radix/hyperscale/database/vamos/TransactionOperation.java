package org.radix.hyperscale.database.vamos;

import java.nio.ByteBuffer;
import java.util.Objects;

import org.radix.hyperscale.collections.PoolBorrower;
import org.radix.hyperscale.utils.Numbers;

class TransactionOperation implements PoolBorrower
{
	private final Environment environment;
	private final Operation operation;
	private final InternalKey key;
	private volatile ByteBuffer data;
	
	TransactionOperation(final Environment environment, final Operation operation, InternalKey key)
	{
		this(environment, operation, key, null);
	}
	
	TransactionOperation(final Environment environment, final Operation operation, final InternalKey key, final byte[] data)
	{
		this(environment, operation, key, data, 0, data.length);
	}

	TransactionOperation(final Environment environment, final Operation operation, final InternalKey key, final byte[] data, final int offset, final int length)
	{
		Objects.requireNonNull(environment, "Environment is null");
		Objects.requireNonNull(operation, "Operation is null");
		Objects.requireNonNull(key, "Hash key is null");

		this.environment = environment;
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
			
			if(length <= environment.getBufferPool().getMaxBufferSize())
				this.data = environment.getBufferPool().acquire(this, length);
			else
				this.data = ByteBuffer.allocate(length);
			
			this.data.put(data, offset, length);
			this.data.flip();
		}
	}
	
	@Override
	public void release()
	{
		if (this.data.limit() <= this.environment.getBufferPool().getMaxBufferSize())
			this.environment.getBufferPool().release(this, this.data);
		
		this.data = null;
	}

	Operation getOperation() 
	{
		return this.operation;
	}

	InternalKey getKey() 
	{
		return this.key;
	}

	ByteBuffer getData() 
	{
		return this.data;
	}
}
