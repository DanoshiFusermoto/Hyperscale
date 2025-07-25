package org.radix.hyperscale.database.vamos;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.Objects;

import org.radix.hyperscale.collections.PoolBorrower;
import org.radix.hyperscale.utils.Numbers;
import org.xerial.snappy.Snappy;

class LogOperation implements PoolBorrower
{
	public static final int COMPRESSION_BUFFER_SIZE = 1<<20;  // 1MB compression buffer
	private static final ThreadLocal<ByteBuffer> compressionBuffer = ThreadLocal.withInitial(() -> ByteBuffer.allocate(COMPRESSION_BUFFER_SIZE));
	
	static final byte[] extractData(final ByteBuffer input) throws IOException
	{
		Objects.requireNonNull(input, "Input buffer is null");
		
		try
		{
			final long ID = input.getLong();
			final long logPosition = input.getLong();

			final long txID = input.getLong();
			final Operation operation = Operation.get(input.get());
			
			// EXTENSION //
			if (operation.equals(Operation.START_TX) == false && operation.equals(Operation.END_TX) == false)
				input.getLong();

			// KEY //
			if (operation.equals(Operation.START_TX) == false && operation.equals(Operation.END_TX) == false)
			{
				input.getInt();
				input.getLong();
			}
			
			// ANCESTOR //
			if (operation.equals(Operation.START_TX) == false && operation.equals(Operation.END_TX) == false && operation.equals(Operation.EXT_NODE) == false)
				input.getLong();

			// DATA //
			if (operation.equals(Operation.PUT) == true || operation.equals(Operation.PUT_NO_OVERWRITE) == true)
			{
				final boolean compressed = input.get() != 0 ? true : false;
				final int dataLength = input.getInt();
				final byte[] data = new byte[dataLength];
				input.get(data);
				if (compressed)
					return Snappy.uncompress(data);

				return data;
			}
			else
				throw new IOException("No data is present in log operation "+operation+" "+ID+":"+txID+" at position "+logPosition);
		}
		catch(BufferOverflowException ex)
		{
			throw new IOException(ex);
		}
	}
	
	private final long 		ID;
	private final long 		txID;
	private final long      logPosition; 
	private final long      extPosition; 
	private final Environment environment;

	private final InternalKey key;
	private final long 		ancestor;
	private final Operation operation;

	private final boolean 	compressed;
	private volatile ByteBuffer data;
	private transient byte[] uncompressed;
	
	LogOperation(final Environment environment, final long ID, final long logPosition, final long txID, Operation operation)
	{
		Objects.requireNonNull(environment, "Environment is null");
		Objects.requireNonNull(operation, "Operation is null");
		Numbers.isNegative(ID, "Log ID is negative");
		Numbers.isNegative(txID, "TX ID is negative");
		Numbers.isNegative(logPosition, "Log position is negative");
		
		if (operation.equals(Operation.START_TX) == false && operation.equals(Operation.END_TX) == false)
			throw new IllegalArgumentException("Invalid log operation "+operation+"; Must be "+Operation.START_TX+" or "+Operation.END_TX);

		this.environment = environment;
		this.ID = ID;
		this.txID = txID;
		this.logPosition = logPosition;
		this.extPosition = -1;
		this.compressed = false;
		this.key = null;
		this.data = null;
		this.ancestor = -1;
		this.operation = operation;
	}
	
	LogOperation(final Environment environment, final long ID, final long logPosition, final long txID, final InternalKey key, final long extPosition, final Operation operation)
	{
		Objects.requireNonNull(environment, "Environment is null");
		Objects.requireNonNull(operation, "Operation is null");
		Objects.requireNonNull(key, "Key is null");
		Numbers.isNegative(ID, "Log ID is negative");
		Numbers.isNegative(txID, "TX ID is negative");
		Numbers.isNegative(logPosition, "Log position is negative");
		Numbers.lessThan(extPosition, -1, "Ext position is negative");

		if (operation.equals(Operation.EXT_NODE) == false)
			throw new IllegalArgumentException("Invalid log operation "+operation+"; Must be "+Operation.EXT_NODE);

		this.environment = environment;
		this.ID = ID;
		this.txID = txID;
		this.logPosition = logPosition;
		this.extPosition = extPosition;
		this.key = key;
		this.compressed = false;
		this.data = null;
		this.ancestor = -1;
		this.operation = operation;
	}

	LogOperation(final Environment environment, final long ID, final long logPosition, final long txID, final TransactionOperation txOperation, long extPosition, final long ancestor) throws IOException
	{
		Objects.requireNonNull(environment, "Environment is null");
		Objects.requireNonNull(txOperation, "Operation is null");
		Numbers.isNegative(ID, "Log ID is negative");
		Numbers.isNegative(txID, "TX ID is negative");
		Numbers.isNegative(logPosition, "Log position is negative");
		Numbers.lessThan(extPosition, -1, "Ext position is negative");
		
		if (txOperation.getData() == null && txOperation.getOperation().equals(Operation.DELETE) == false)
			throw new IllegalArgumentException("Unsupported database operation "+txOperation.getOperation());

		if (txOperation.getData() != null && txOperation.getOperation().equals(Operation.PUT) == false && txOperation.getOperation().equals(Operation.PUT_NO_OVERWRITE) == false)
			throw new IllegalArgumentException("Unsupported database operation "+txOperation.getOperation());

		this.environment = environment;
		this.ID = ID;
		this.key = txOperation.getKey();
		this.txID = txID;
		this.logPosition = logPosition;
		this.extPosition = extPosition;

		// Copy the data from the TransactionOperation
		final ByteBuffer source = txOperation.getData();
		if (source.limit() <= environment.getBufferPool().getMaxBufferSize())
			this.data = environment.getBufferPool().acquire(this, source.limit());
		else
			this.data = ByteBuffer.allocate(source.limit());
		
		this.data.put(0, txOperation.getData(), 0, source.limit());
		this.data.position(source.limit());
		this.data.flip();
		
		boolean compressed = false;
		if (this.data.limit() >= environment.getConfig().getLogCompressionThreshold())
		{
			final int maxCompressedLength = Snappy.maxCompressedLength(this.data.limit());
			if (maxCompressedLength < this.data.limit())
			{
				final ByteBuffer compressionBuffer = maxCompressedLength > COMPRESSION_BUFFER_SIZE ? ByteBuffer.allocate(maxCompressedLength) : LogOperation.compressionBuffer.get().clear();
				final int compressedLength = Snappy.compress(this.data.array(), 0, this.data.limit(), compressionBuffer.array(), 0);
				
				this.data.put(0, compressionBuffer, 0, compressedLength);
				this.data.limit(compressedLength);
				this.data.flip();
				compressed = true;
			}
		}
		this.compressed = compressed;
		
		this.operation = txOperation.getOperation();
		this.ancestor = ancestor;
		
		if (this.ancestor == 0)
			return;
	}

	LogOperation(final Environment environment, final ByteBuffer input) throws IOException
	{
		Objects.requireNonNull(environment, "Environment is null");
		Objects.requireNonNull(input, "Input buffer is null");
		
		try
		{
			this.environment = environment;

			this.ID = input.getLong();
			this.logPosition = input.getLong();

			this.txID = input.getLong();
			this.operation = Operation.get(input.get());
			
			// EXTENSION //
			if (this.operation.equals(Operation.START_TX) == false && this.operation.equals(Operation.END_TX) == false)
				this.extPosition = input.getLong();
			else
				this.extPosition = -1;

			// KEY //
			if (this.operation.equals(Operation.START_TX) == false && this.operation.equals(Operation.END_TX) == false)
				this.key = InternalKey.from(input.getInt(), input.getLong());
			else
				this.key = null;
			
			// ANCESTOR //
			if (this.operation.equals(Operation.START_TX) == false && this.operation.equals(Operation.END_TX) == false && this.operation.equals(Operation.EXT_NODE) == false)
				this.ancestor = input.getLong();
			else
				this.ancestor = -1;

			// DATA //
			if (this.operation.equals(Operation.PUT) == true || this.operation.equals(Operation.PUT_NO_OVERWRITE) == true)
			{
				this.compressed = input.get() != 0 ? true : false;
				int dataLength = input.getInt();
				if (dataLength <= environment.getBufferPool().getMaxBufferSize())
					this.data = environment.getBufferPool().acquire(this, dataLength);
				else
					this.data = ByteBuffer.allocate(dataLength);
				this.data.put(0, input, input.position(), dataLength);
				this.data.position(dataLength);
				this.data.flip();
			}
			else
			{
				this.data = null;
				this.compressed = false;
			}
			
			if (ancestor == 0)
				return;
		}
		catch(BufferOverflowException ex)
		{
			throw new IOException(ex);
		}
	}
	
	@Override
	public void release()
	{
		if (this.data != null && this.data.limit() <= environment.getBufferPool().getMaxBufferSize())
			this.environment.getBufferPool().release(this, this.data);
		
		this.data = null;
	}

	long getID()
	{
		return this.ID;
	}
	
	long getLogPosition()
	{
		return this.logPosition;
	}

	long getTXID()
	{
		return this.txID;
	}

	InternalKey getKey()
	{
		return this.key;
	}
	
	long getAncestorPointer()
	{
		return this.ancestor;
	}
	
	Operation getOperation()
	{
		return this.operation;
	}
	
	boolean isExtension()
	{
		return this.extPosition > -1;
	}
	
	long getExtensionPosition()
	{
		return this.extPosition;
	}

	int length()
	{
		int length = Byte.BYTES + Long.BYTES + Long.BYTES + Long.BYTES;
		
		// EXTENSION //
		if (this.operation.equals(Operation.START_TX) == false && this.operation.equals(Operation.END_TX) == false)
			length += Long.BYTES;

		// KEY //
		if (this.operation.equals(Operation.START_TX) == false && this.operation.equals(Operation.END_TX) == false)
			length += InternalKey.BYTES;
		
		// ANCESTOR //
		if (this.operation.equals(Operation.START_TX) == false && this.operation.equals(Operation.END_TX) == false && this.operation.equals(Operation.EXT_NODE) == false)
			length += Long.BYTES;
		
		// DATA //
		if (this.operation.equals(Operation.PUT) == true || this.operation.equals(Operation.PUT_NO_OVERWRITE) == true)
			length += Byte.BYTES + Integer.BYTES + this.data.limit();

		return length;
	}
	
	synchronized byte[] getData() throws IOException
	{
		if (this.data == null)
			return null;

		if (this.uncompressed == null)
		{
			if (this.compressed)
			{
				int uncompressedLength = Snappy.uncompressedLength(this.data.array(), 0, this.data.limit());
				this.uncompressed = new byte[uncompressedLength];
				Snappy.uncompress(this.data.array(), 0, this.data.limit(), uncompressed, 0);
			}
			else
			{
				this.uncompressed = new byte[this.data.limit()];
				System.arraycopy(data.array(), 0, this.uncompressed, 0, this.data.limit());
			}
		}
		
		return this.uncompressed;
	}
	
	void write(final ByteBuffer output) throws IOException
	{
		output.putLong(this.ID);
		output.putLong(this.logPosition);
		output.putLong(this.txID);
		output.put((byte) this.operation.type());
		
		// EXTENSION //
		if (this.operation.equals(Operation.START_TX) == false && this.operation.equals(Operation.END_TX) == false)
			output.putLong(this.extPosition);

		// KEY //
		if (this.operation.equals(Operation.START_TX) == false && this.operation.equals(Operation.END_TX) == false)
		{
			output.putInt(this.key.getDatabaseID());
			output.putLong(this.key.value());
		}
		
		// ANCESTOR //
		if (this.operation.equals(Operation.START_TX) == false && this.operation.equals(Operation.END_TX) == false && this.operation.equals(Operation.EXT_NODE) == false)
			output.putLong(this.ancestor);

		// DATA //
		if (this.operation.equals(Operation.PUT) == true || this.operation.equals(Operation.PUT_NO_OVERWRITE) == true)
		{
			output.put(this.compressed ? (byte) 1 : (byte) 0);
			output.putInt(this.data.limit());
			output.put(this.data.array(), 0, this.data.limit());
		}
	}
}
