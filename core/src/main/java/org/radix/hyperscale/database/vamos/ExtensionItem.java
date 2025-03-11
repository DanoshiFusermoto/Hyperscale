package org.radix.hyperscale.database.vamos;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

import org.radix.hyperscale.utils.Numbers;

class ExtensionItem extends ExtensionObject
{
	public final static int BYTES = ExtensionObject.BYTES + Long.BYTES + Long.BYTES + Long.BYTES + Long.BYTES + Byte.BYTES;

	/* Thread local ByteBuffer used when serializing I/O */ 
	private static final ThreadLocal<ByteBuffer> byteBuffer = ThreadLocal.withInitial(() -> ByteBuffer.allocate(BYTES));

	private final long 		id;
	private final long 		logPosition;
	private final long 		previous;
	private final long 		next;
	private volatile boolean deleted;
	
	static ExtensionItem from(byte[] bytes) throws IOException
	{
		Objects.requireNonNull(bytes, "Bytes is null");

		ByteBuffer buffer = byteBuffer.get(); 
		buffer.clear();
		buffer.put(bytes);
		buffer.flip();

		return new ExtensionItem(buffer);
	}

	ExtensionItem(final long id, final InternalKey key, final long extPosition, final long logPosition, final long previous, final long next)
	{
		this(id, key, extPosition, logPosition, previous, next, false);
	}

	ExtensionItem(final ByteBuffer buffer) throws IOException
	{
		super(InternalKey.from(Objects.requireNonNull(buffer, "Byte buffer is null").getInt(), buffer.getLong()), buffer.getLong());
		
		this.id = buffer.getLong();
		this.logPosition = buffer.getLong();
		this.previous = buffer.getLong();
		this.next = buffer.getLong();
		this.deleted = buffer.get() == 0 ? false : true;
		
		Numbers.isNegative(this.id, "ID is negative");
		Numbers.isNegative(this.logPosition, "Log position is negative");
		Numbers.lessThan(this.previous, -1, "Previous is negative");
		if (this.next != -1)
			Numbers.lessThan(this.next, this.previous, "Next is less than previous");
	}

	ExtensionItem(final long id, final InternalKey key, final long extPosition, final long logPosition, final long previous, final long next, boolean deleted)
	{
		super(key, extPosition);
		
		Numbers.isNegative(id, "ID is negative");
		Numbers.isNegative(logPosition, "Log position is negative");
		Numbers.lessThan(previous, -1, "Previous is negative");
		if (next != -1)
			Numbers.lessThan(next, previous, "Next is less than previous");

		this.id = id;
		this.logPosition = logPosition;
		this.previous = previous;
		this.next = next;
		this.deleted = deleted;
	}
	
	@Override
	public int hashCode() 
	{
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + (int) (id ^ (id >>> 32));
		return result;
	}

	@Override
	public boolean equals(Object obj) 
	{
		if (obj == null)
			return false;
		
		if (this == obj)
			return true;
	
		if (super.equals(obj) == false)
			return false;
		
		if (obj instanceof ExtensionItem other)
		{
			if (this.id != other.id)
				return false;
			if (this.deleted != other.deleted)
				return false;
			if (this.logPosition != other.logPosition)
				return false;
			if (this.next != other.next)
				return false;
			if (this.previous != other.previous)
				return false;
			
			return true;
		}

		return false;
	}
	
	@Override
	int size() 
	{
		return BYTES;
	}
	
	public long getID()
	{
		return this.id;
	}

	public long getLogPosition() 
	{
		return this.logPosition;
	}

	public long getPrevious() 
	{
		return this.previous;
	}

	public long getNext() 
	{
		return this.next;
	}

	public boolean isDeleted() 
	{
		return this.deleted;
	}
	
	@Override
	void write(final ByteBuffer output)
	{
		output.putInt(getKey().getDatabaseID());
		output.putLong(getKey().value());
		output.putLong(getExtPosition());
		output.putLong(this.id);
		output.putLong(this.logPosition);
		output.putLong(this.previous);
		output.putLong(this.next);
		output.put(this.deleted ? (byte) 0x1 : (byte) 0x0);
	}
	
	@Override
	byte[] toByteArray() throws IOException
	{
		ByteBuffer buffer = byteBuffer.get(); 
		buffer.clear();
		write(buffer);

		buffer.flip();
		byte[] bytes = new byte[buffer.limit()];
		buffer.get(bytes);
		return bytes;
	}

	@Override
	public String toString()
	{
		return super.toString()+" [id="+this.id+", logPosition="+this.logPosition+", previous="+this.previous+", next="+this.next+", deleted="+this.deleted+"]";
	}
}
