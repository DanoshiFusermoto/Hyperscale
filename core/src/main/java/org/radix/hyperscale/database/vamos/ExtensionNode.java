package org.radix.hyperscale.database.vamos;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

final class ExtensionNode extends ExtensionObject
{
	public final static int BYTES = ExtensionObject.BYTES + Long.BYTES + ExtensionItem.BYTES + Long.BYTES + ExtensionItem.BYTES + Byte.BYTES;

	/* Thread local ByteBuffer used when serializing I/O */ 
	private static final ThreadLocal<ByteBuffer> byteBuffer = ThreadLocal.withInitial(() -> ByteBuffer.allocate(BYTES));
	
	private volatile long firstPosition;
	private volatile long lastPosition;
	private volatile ExtensionItem firstItem;
	private volatile ExtensionItem lastItem;
	private volatile boolean deleted;
	
	static ExtensionNode from(byte[] bytes) throws IOException
	{
		Objects.requireNonNull(bytes, "Bytes is null");

		ByteBuffer buffer = byteBuffer.get(); 
		buffer.clear();
		buffer.put(bytes);
		buffer.flip();

		return new ExtensionNode(buffer);
	}
	
	ExtensionNode(ByteBuffer buffer) throws IOException
	{
		super(InternalKey.from(Objects.requireNonNull(buffer, "Byte buffer is null").getInt(), buffer.getLong()), buffer.getLong());
		
		this.firstPosition = buffer.getLong();
		this.firstItem = new ExtensionItem(buffer);
		this.lastPosition = buffer.getLong();
		this.lastItem = new ExtensionItem(buffer);
		this.deleted = buffer.get() == 0 ? false : true;
	}

	ExtensionNode(final InternalKey key, final long position)
	{
		super(key, position);

		this.firstPosition = -1;
		this.lastPosition = -1;
		this.deleted = false;
	}
	
	@Override
	int size() 
	{
		return BYTES;
	}

	boolean isDeleted()
	{
		return this.deleted;
	}
	
	void delete()
	{
		if (this.deleted)
			throw new IllegalStateException("Extension node "+getKey()+" is already deleted");
		
		this.deleted = true;
	}
	
	long getFirstPosition()
	{
		return this.firstPosition;
	}
	
	ExtensionItem getFirstItem()
	{
		return this.firstItem;
	}

	long getLastPosition()
	{
		return this.lastPosition;
	}

	ExtensionItem getLastItem()
	{
		return this.lastItem;
	}

	public void updateFirstItem(ExtensionItem item) 
	{
		this.firstItem = item;
		this.firstPosition = item.getExtPosition();
	}

	public void updateLastItem(ExtensionItem item) 
	{
		this.lastItem = item;
		this.lastPosition = item.getExtPosition();
	}

	void write(final ByteBuffer output)
	{
		output.putInt(getKey().getDatabaseID());
		output.putLong(getKey().value());
		output.putLong(getExtPosition());
		output.putLong(this.firstPosition);
		this.firstItem.write(output);
		output.putLong(this.lastPosition);
		this.lastItem.write(output);
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
		return super.toString()+" [firstPosition="+this.firstPosition+", lastPosition="+this.lastPosition+", firstItem="+this.firstItem+", lastItem="+this.lastItem+", deleted="+this.deleted+"]";
	}
}
