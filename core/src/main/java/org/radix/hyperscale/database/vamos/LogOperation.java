package org.radix.hyperscale.database.vamos;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.Objects;

import org.radix.hyperscale.utils.Numbers;
import org.xerial.snappy.Snappy;

class LogOperation 
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

	private final InternalKey key;
	private final long 		ancestor;
	private final byte[] 	data;
	private final int		dataLength;
	private final boolean 	compressed;
	private final Operation operation;

	private transient byte[] uncompressed;
	
	LogOperation(final Environment environment, final long ID, final long logPosition, final long txID, Operation operation)
	{
		Objects.requireNonNull(operation, "Operation is null");
		Numbers.isNegative(ID, "Log ID is negative");
		Numbers.isNegative(txID, "TX ID is negative");
		Numbers.isNegative(logPosition, "Log position is negative");
		
		if (operation.equals(Operation.START_TX) == false && operation.equals(Operation.END_TX) == false)
			throw new IllegalArgumentException("Invalid log operation "+operation+"; Must be "+Operation.START_TX+" or "+Operation.END_TX);

		this.ID = ID;
		this.txID = txID;
		this.logPosition = logPosition;
		this.extPosition = -1;
		this.compressed = false;
		this.key = null;
		this.data = null;
		this.dataLength = -1;
		this.ancestor = -1;
		this.operation = operation;
	}
	
	LogOperation(final Environment environment, final long ID, final long logPosition, final long txID, final InternalKey key, final long extPosition, final Operation operation)
	{
		Objects.requireNonNull(operation, "Operation is null");
		Objects.requireNonNull(key, "Key is null");
		Numbers.isNegative(ID, "Log ID is negative");
		Numbers.isNegative(txID, "TX ID is negative");
		Numbers.isNegative(logPosition, "Log position is negative");
		Numbers.lessThan(extPosition, -1, "Ext position is negative");

		if (operation.equals(Operation.EXT_NODE) == false)
			throw new IllegalArgumentException("Invalid log operation "+operation+"; Must be "+Operation.EXT_NODE);

		this.ID = ID;
		this.txID = txID;
		this.logPosition = logPosition;
		this.extPosition = extPosition;
		this.key = key;
		this.compressed = false;
		this.data = null;
		this.dataLength = -1;
		this.ancestor = -1;
		this.operation = operation;
	}

	LogOperation(final Environment environment, final long ID, final long logPosition, final long txID, final TransactionOperation txOperation, long extPosition, final long ancestor) throws IOException
	{
		Objects.requireNonNull(txOperation, "Operation is null");
		Numbers.isNegative(ID, "Log ID is negative");
		Numbers.isNegative(txID, "TX ID is negative");
		Numbers.isNegative(logPosition, "Log position is negative");
		Numbers.lessThan(extPosition, -1, "Ext position is negative");
		
		if (txOperation.getData() == null && txOperation.getOperation().equals(Operation.DELETE) == false)
			throw new IllegalArgumentException("Unsupported database operation "+txOperation.getOperation());

		if (txOperation.getData() != null && txOperation.getOperation().equals(Operation.PUT) == false && txOperation.getOperation().equals(Operation.PUT_NO_OVERWRITE) == false)
			throw new IllegalArgumentException("Unsupported database operation "+txOperation.getOperation());

		this.ID = ID;
		this.key = txOperation.getKey();
		this.txID = txID;
		this.logPosition = logPosition;
		this.extPosition = extPosition;

		byte[] data = txOperation.getData();
		int dataLength = txOperation.getDataLength();
		boolean compressed = false;
		if (dataLength >= environment.getConfig().getLogCompressionThreshold())
		{
			final int maxCompressedLength = Snappy.maxCompressedLength(dataLength);
			if (maxCompressedLength >= dataLength)
			{
				final ByteBuffer compressionBuffer = maxCompressedLength > COMPRESSION_BUFFER_SIZE ? ByteBuffer.allocate(maxCompressedLength) : LogOperation.compressionBuffer.get().clear();
				final int compressedLength = Snappy.compress(data, 0, dataLength, compressionBuffer.array(), 0);
				
				System.arraycopy(compressionBuffer.array(), 0, data, 0, compressedLength);
				dataLength = compressedLength;
				compressed = true;
			}
		}

		this.data = data;
		this.dataLength = dataLength;
		this.compressed = compressed;
		
		this.operation = txOperation.getOperation();
		this.ancestor = ancestor;
		
		if (this.ancestor == 0)
			return;
	}

	LogOperation(final ByteBuffer input) throws IOException
	{
		Objects.requireNonNull(input, "Input buffer is null");
		
		try
		{
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
				this.dataLength = input.getInt();
				this.data = new byte[this.dataLength];
				input.get(this.data);
			}
			else
			{
				this.data = null;
				this.dataLength = -1;
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
			length += Byte.BYTES + Integer.BYTES + this.data.length;

		return length;
	}
	
	synchronized byte[] getData() throws IOException
	{
		if (this.data == null)
			return null;

		if (this.uncompressed == null)
		{
			if (this.compressed)
				this.uncompressed = Snappy.uncompress(this.data);
			else
				this.uncompressed = this.data;
		}
		
		return this.uncompressed;
	}
	
	int getDataLength()
	{
		return this.dataLength;
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
			output.putInt(this.dataLength);
			output.put(this.data, 0, this.dataLength);
		}
	}
}
