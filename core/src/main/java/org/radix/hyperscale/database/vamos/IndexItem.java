package org.radix.hyperscale.database.vamos;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

import org.radix.hyperscale.utils.Numbers;

class IndexItem 
{
	static final int BYTES = Byte.BYTES + InternalKey.BYTES + Long.BYTES;
	static final IndexItem VACANT = new IndexItem(); 

	/* Thread local ByteBuffer used when serializing I/O */ 
	private static final ThreadLocal<ByteBuffer> byteBuffer = ThreadLocal.withInitial(() -> ByteBuffer.allocate(BYTES));

	enum Type
	{
		DATA, EXTENSION
	}
	
	private final Type type;
	private final InternalKey key;
	private volatile long position;
	
	private final int hashCode;
	
	static final IndexItem from(byte[] bytes) throws IOException
	{
		ByteBuffer buffer = byteBuffer.get(); 
		buffer.clear();
		buffer.put(bytes);
		buffer.flip();

		return new IndexItem(buffer);
	}
	
	private IndexItem()
	{
		this.type = null;
		this.key = null;
		this.position = -1;
		this.hashCode = computeHashCode();
	}
	
	IndexItem(final ByteBuffer buffer) throws IOException
	{
		Objects.requireNonNull(buffer, "Byte buffer is null");

		int type = buffer.get();
		if (type == 0)
			this.type = Type.DATA;
		else if (type == 1)
			this.type = Type.EXTENSION;
		else 
			throw new IllegalArgumentException("Type "+type+" is unknown");
			
		this.key = InternalKey.from(buffer.getInt(), buffer.getLong());
		this.position = buffer.getLong();
		this.hashCode = computeHashCode();
	}
	
	IndexItem(final InternalKey key)
	{
		Objects.requireNonNull(key, "Key is null");
		this.key = key;
		this.position = -1;
		this.type = null;
		this.hashCode = computeHashCode();
	}

	IndexItem(final Type type, final InternalKey key, final long position)
	{
		Objects.requireNonNull(type, "Type is null");
		Objects.requireNonNull(key, "Key is null");
		Numbers.isNegative(position, "Data position is negative");

		this.type = type;
		this.key = key;
		this.position = position;
		this.hashCode = computeHashCode();
	}

	Type getType()
	{
		return this.type;
	}
	
	InternalKey getKey()
	{
		return this.key;
	}
	
	long getPosition()
	{
		return this.position;
	}
	
	void update(final IndexItem other)
	{
		if (this.hashCode != other.hashCode ||
			this.type.equals(other.type) == false ||
			this.key.equals(other.key) == false)
			throw new IllegalArgumentException("Can not update index item "+this+" with "+other);

		this.position = other.position;
	}

	void write(final ByteBuffer output)
	{
		if (this.type == Type.DATA)
			output.put((byte) 0x00);
		else if (this.type == Type.EXTENSION)
			output.put((byte) 0x01);
		else
			throw new IllegalArgumentException("Type "+this.type+" is unsupported");
				
		output.putInt(this.key.getDatabaseID());
		output.putLong(this.key.value());
		output.putLong(this.position);
	}
	
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

	private int computeHashCode() 
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + (this.key == null ? 0 : this.key.hashCode());
		result = prime * result + (this.type == null ? 0 : this.type.hashCode());
		return result;
	}
	
	@Override
	public int hashCode() 
	{
		return this.hashCode;
	}

	@Override
	public boolean equals(final Object obj) 
	{
		if (this == obj)
			return true;
	
		if (obj == null)
			return false;
		
		if (obj instanceof IndexItem other)
		{
			if (this.hashCode != other.hashCode)
				return false;
			if (this.key == null && other.key != null)
				return false;
			else if (this.key.equals(other.key) == false)
				return false;
			if (this.position != other.position)
				return false;
			if (this.type != other.type)
				return false;
		
			return true;
		}
		
		return false;
	}

	@Override
	public String toString() 
	{
		return "[type="+this.type+", key="+this.key+", position="+this.position+"]";
	}
}
