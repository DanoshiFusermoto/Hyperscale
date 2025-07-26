package org.radix.hyperscale.database;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import org.apache.commons.lang3.mutable.MutableDouble;
import org.radix.hyperscale.Context;
import org.radix.hyperscale.Service;
import org.radix.hyperscale.exceptions.ServiceException;
import org.radix.hyperscale.exceptions.StartupException;
import org.radix.hyperscale.exceptions.TerminationException;
import org.radix.hyperscale.executors.Executor;
import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;
import org.radix.hyperscale.utils.Bytes;
import org.radix.hyperscale.utils.Longs;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseNotFoundException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;

public final class SystemMetaData extends DatabaseStore implements Service
{
	private static final Logger log = Logging.getLogger();
	private static final int MAX_LENGTH = 256;

	private final Context context;
	private Map<String, Object> metaDataCache = new ConcurrentHashMap<>(1<<10);
	private Database metaDataDB = null;
	private Future<?> flush;
	
	public SystemMetaData(final Context context)
	{
		super(Objects.requireNonNull(context).getDatabaseEnvironment());
		this.context = context;
	}
	
	@Override
	public void start() throws StartupException
	{
		try
		{
			if (this.context.getConfiguration().getCommandLine("clean", false) == true)
				clean();

			DatabaseConfig config = new DatabaseConfig();
			config.setAllowCreate(true);
			config.setTransactional(true);
	
			this.metaDataDB = getEnvironment().openDatabase(null, "meta_data", config);

			load();
		}
		catch (Exception ex)
		{
			throw new RuntimeException(ex);
		}

		this.flush = Executor.getInstance().scheduleWithFixedDelay(new Runnable()
		{
			@Override
			public void run()
			{
				try
				{
					flush();
				}
				catch (DatabaseException e)
				{
					log.error(e.getMessage(), e);
				}
			}
		}, 30, 30, TimeUnit.SECONDS);
	}

	@Override
	public void stop() throws TerminationException
	{
		try
		{
			close();
		}
		catch (IOException ioex)
		{
			throw new TerminationException(ioex);
		}
	}


	@Override
	public void clean() throws ServiceException
	{
		Transaction transaction = null;

		try
		{
			transaction = getEnvironment().beginTransaction(null, new TransactionConfig().setReadUncommitted(true));
			getEnvironment().truncateDatabase(transaction, "meta_data", false);
			transaction.commit();
			this.metaDataCache.clear();
		}
		catch (DatabaseNotFoundException dsnfex)
		{
			if (transaction != null)
				transaction.abort();

			log.warn(dsnfex.getMessage());
		}
		catch (Exception ex)
		{
			if (transaction != null)
				transaction.abort();

			throw new ServiceException(ex);
		}
	}

	@Override
	public void close() throws IOException
	{
		if (this.flush != null)
			this.flush.cancel(false);

		super.close();

		this.metaDataDB.close();
	}

	@Override
	public synchronized void flush() throws DatabaseException
	{
		final Transaction transaction = getEnvironment().beginTransaction(null, null);
		try
        {
			final DatabaseEntry key = new DatabaseEntry();
			final DatabaseEntry value = new DatabaseEntry();
			for (Map.Entry<String, Object> entry : this.metaDataCache.entrySet())
			{
				key.setData(entry.getKey().getBytes(StandardCharsets.UTF_8));
				Class<?> valueClass = entry.getValue().getClass();
				final byte[] bytes;
				if (valueClass.equals(String.class)) 
				{
					String stringValue = (String) entry.getValue();
					byte[] stringBytes = stringValue.getBytes(StandardCharsets.UTF_8);
					bytes = new byte[1 + stringBytes.length];
					bytes[0] = 'S';
					System.arraycopy(stringBytes, 0, bytes, 1, stringBytes.length);
				} 
				else if (valueClass.equals(MutableDouble.class)) 
				{
					long longValue = ((MutableDouble) entry.getValue()).longValue();
					bytes = new byte[1 + Long.BYTES];
					bytes[0] = 'L';
					Longs.copyTo(longValue, bytes, 1);
				}
				else if (valueClass.equals(byte[].class)) 
				{
					byte[] bytesValue = (byte[]) entry.getValue();
					bytes = new byte[1 + bytesValue.length];
					bytes[0] = 'B';
					System.arraycopy(bytesValue, 0, bytes, 1, bytesValue.length);
				} 
				else 
					throw new IllegalArgumentException("Unknown value type: " + valueClass.getName());

				value.setData(bytes);
				this.metaDataDB.put(transaction, key, value);
			}
			
			transaction.commit();
		}
		catch (Exception e)
		{
			transaction.abort();
			throw new DatabaseException(e);
		}
	}

	public boolean has(final String name)
	{
		Objects.requireNonNull(name, "Name is null");
		if (name.length() == 0)
			throw new IllegalArgumentException("Name length is zero");
		if (name.length() > MAX_LENGTH)
			throw new IllegalArgumentException("Name length is greater than "+MAX_LENGTH);
		
		return this.metaDataCache.containsKey(name);
	}

	public String get(final String name, final String option)
	{
		Objects.requireNonNull(name, "Name is null");
		if (name.length() == 0)
			throw new IllegalArgumentException("Name length is zero");
		if (name.length() > MAX_LENGTH)
			throw new IllegalArgumentException("Name length is greater than "+MAX_LENGTH);

		Objects.requireNonNull(option, "Option is null");
		if (option.length() == 0)
			throw new IllegalArgumentException("Option length is zero");
		if (option.length() > MAX_LENGTH)
			throw new IllegalArgumentException("Option length is greater than "+MAX_LENGTH);

		Object value = this.metaDataCache.get(name);

		if (value == null)
			return option;

		return asString(value);
	}

	public long get(final String name, final long option)
	{
		Objects.requireNonNull(name, "Name is null");
		if (name.length() == 0)
			throw new IllegalArgumentException("Name length is zero");
		if (name.length() > MAX_LENGTH)
			throw new IllegalArgumentException("Name length is greater than "+MAX_LENGTH);

		final Object value = this.metaDataCache.get(name);
		if (value == null)
			return option;

		return asLong(value);
	}

	public double get(final String name, final double option)
	{
		Objects.requireNonNull(name, "Name is null");
		if (name.length() == 0)
			throw new IllegalArgumentException("Name length is zero");
		if (name.length() > MAX_LENGTH)
			throw new IllegalArgumentException("Name length is greater than "+MAX_LENGTH);

		final Object value = this.metaDataCache.get(name);
		if (value == null)
			return option;

		return asDouble(value);
	}

	public byte[] get(final String name, final byte[] option)
	{
		Objects.requireNonNull(name, "Name is null");
		if (name.length() == 0)
			throw new IllegalArgumentException("Name length is zero");
		if (name.length() > MAX_LENGTH)
			throw new IllegalArgumentException("Name length is greater than "+MAX_LENGTH);

		Objects.requireNonNull(option, "Option is null");
		if (option.length == 0)
			throw new IllegalArgumentException("Option length is zero");
		if (option.length > MAX_LENGTH)
			throw new IllegalArgumentException("Option length is greater than "+MAX_LENGTH);

		final Object value = this.metaDataCache.get(name);
		if (value == null)
			return option;

		return asBytes(value);
	}

	public long increment(final String name)
	{
		return increment(name, 1l);
	}

	public long increment(final String name, final long increment)
	{
		Objects.requireNonNull(name, "Name is null");
		if (name.length() == 0)
			throw new IllegalArgumentException("Name length is zero");
		if (name.length() > MAX_LENGTH)
			throw new IllegalArgumentException("Name length is greater than "+MAX_LENGTH);
		
		final MutableDouble value = (MutableDouble) this.metaDataCache.computeIfAbsent(name, k -> new MutableDouble(0));
		synchronized(value)
		{
			value.add(increment);
		}

		return value.longValue();
	}

	public long increment(final String name, final double increment)
	{
		Objects.requireNonNull(name, "Name is null");
		if (name.length() == 0)
			throw new IllegalArgumentException("Name length is zero");
		if (name.length() > MAX_LENGTH)
			throw new IllegalArgumentException("Name length is greater than "+MAX_LENGTH);
		
		final MutableDouble value = (MutableDouble) this.metaDataCache.computeIfAbsent(name, k -> new MutableDouble(0));
		synchronized(value)
		{
			value.add(increment);
		}

		return value.longValue();
	}

	public long decrement(final String name)
	{
		return decrement(name, 1l);
	}

	public long decrement(final String name, final long decrement)
	{
		Objects.requireNonNull(name, "Name is null");
		if (name.length() == 0)
			throw new IllegalArgumentException("Name length is zero");
		if (name.length() > MAX_LENGTH)
			throw new IllegalArgumentException("Name length is greater than "+MAX_LENGTH);

		final MutableDouble value = (MutableDouble) this.metaDataCache.computeIfAbsent(name, k -> new MutableDouble(0));
		synchronized(value)
		{
			value.subtract(decrement);
		}
		
		return value.longValue();
	}

	public long decrement(final String name, final double decrement)
	{
		Objects.requireNonNull(name, "Name is null");
		if (name.length() == 0)
			throw new IllegalArgumentException("Name length is zero");
		if (name.length() > MAX_LENGTH)
			throw new IllegalArgumentException("Name length is greater than "+MAX_LENGTH);

		final MutableDouble value = (MutableDouble) this.metaDataCache.computeIfAbsent(name, k -> new MutableDouble(0));
		synchronized(value)
		{
			value.subtract(decrement);
		}
		
		return value.longValue();
	}

	public void put(final String name, final String value)
	{
		Objects.requireNonNull(name, "Name is null");
		if (name.length() == 0)
			throw new IllegalArgumentException("Name length is zero");
		if (name.length() > MAX_LENGTH)
			throw new IllegalArgumentException("Name length is greater than "+MAX_LENGTH);

		Objects.requireNonNull(value, "Value is null");
		if (value.length() == 0)
			throw new IllegalArgumentException("Value length is zero");
		if (value.length() > MAX_LENGTH)
			throw new IllegalArgumentException("Value length is greater than "+MAX_LENGTH);

		this.metaDataCache.put(name, value);
	}

	public void put(final String name, final long value)
	{
		Objects.requireNonNull(name, "Name is null");
		if (name.length() == 0)
			throw new IllegalArgumentException("Name length is zero");
		if (name.length() > MAX_LENGTH)
			throw new IllegalArgumentException("Name length is greater than "+MAX_LENGTH);

		((MutableDouble) this.metaDataCache.computeIfAbsent(name, k -> new MutableDouble(0))).setValue(value);
	}

	public void put(final String name, final double value)
	{
		Objects.requireNonNull(name, "Name is null");
		if (name.length() == 0)
			throw new IllegalArgumentException("Name length is zero");
		if (name.length() > MAX_LENGTH)
			throw new IllegalArgumentException("Name length is greater than "+MAX_LENGTH);

		((MutableDouble) this.metaDataCache.computeIfAbsent(name, k -> new MutableDouble(0))).setValue(value);
	}

	public void put(final String name, final byte[] value)
	{
		Objects.requireNonNull(name, "Name is null");
		if (name.length() == 0)
			throw new IllegalArgumentException("Name length is zero");
		if (name.length() > MAX_LENGTH)
			throw new IllegalArgumentException("Name length is greater than "+MAX_LENGTH);

		Objects.requireNonNull(value, "Value is null");
		if (value.length == 0)
			throw new IllegalArgumentException("Value length is zero");
		if (value.length > MAX_LENGTH)
			throw new IllegalArgumentException("Value length is greater than "+MAX_LENGTH);

		// Take a defensive copy
		this.metaDataCache.put(name, value.clone());
	}
	
	public void forEach(final BiConsumer <? super String, ? super Object> consumer)
	{
		this.metaDataCache.forEach(consumer);
	}

	/**
	 * Gets the meta data from the DB
	 *
	 * @throws DatabaseException
	 */
	private void load() throws DatabaseException
	{
		try (Cursor cursor = this.metaDataDB.openCursor(null, null))
        {
			DatabaseEntry key = new DatabaseEntry();
			DatabaseEntry value = new DatabaseEntry();

			this.metaDataCache.clear();

			while (cursor.getNext(key, value, LockMode.DEFAULT) == OperationStatus.SUCCESS)
			{
				String keyString = Bytes.toString(key.getData()).toLowerCase();
				byte[] bytes = value.getData();
				byte[] newBytes = new byte[bytes.length - 1];
				System.arraycopy(bytes, 1, newBytes, 0, newBytes.length);
				switch (bytes[0]) {
				case 'S':
					this.metaDataCache.put(keyString, Bytes.toString(newBytes));
					break;
				case 'L':
					this.metaDataCache.put(keyString, new MutableDouble(Longs.fromByteArray(newBytes)));
					break;
				case 'B':
					this.metaDataCache.put(keyString, newBytes);
					break;
				default:
					throw new IllegalArgumentException("Unknown type byte: " + bytes[0]);
				}
			}
		}
		catch (Exception e)
		{
			throw new DatabaseException(e);
		}
	}

	private static String asString(final Object value)
	{
		Objects.requireNonNull(value, "Value is null");
		if (value instanceof String string)
			return string;

		throw new IllegalArgumentException("Value is not a string");
	}

	private static long asLong(final Object value)
	{
		Objects.requireNonNull(value, "Value is null");
		if (value instanceof MutableDouble mutable)
			return mutable.longValue();
		
		throw new IllegalArgumentException("Value is not a number");
	}

	private static double asDouble(final Object value)
	{
		Objects.requireNonNull(value, "Value is null");
		if (value instanceof MutableDouble mutable)
			return mutable.doubleValue();
		
		throw new IllegalArgumentException("Value is not a number");
	}

	private static byte[] asBytes(final Object value)
	{
		Objects.requireNonNull(value, "Value is null");
		if (value instanceof byte[] bytes)
			return bytes;
		
		throw new IllegalArgumentException("Value is not a byte[]");
	}
}
