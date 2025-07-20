package org.radix.hyperscale.database;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.radix.hyperscale.Context;
import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;
import org.radix.hyperscale.utils.Bytes;

import com.sleepycat.je.CacheMode;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;

public final class DatabaseEnvironment
{
	private static final Logger log = Logging.getLogger();
	
	private final static boolean ENV_MINIMAL_CACHE = true;
	private static Set<DatabaseEnvironment> environments = Collections.synchronizedSet(new HashSet<DatabaseEnvironment>());

	// Database checkpointing and cleaning 
	private static Thread environmentCheckpointThread;
	private static Runnable environmentCheckpointRunnable;
	
	static 
	{
		DatabaseEnvironment.environmentCheckpointRunnable = new Runnable() 
		{
			@Override
			public void run() 
			{
				while(1==1)
				{
					long start = System.currentTimeMillis();
					List<DatabaseEnvironment> environments = new ArrayList<DatabaseEnvironment>(DatabaseEnvironment.environments);
					for (DatabaseEnvironment environment : environments)
					{
						try
						{
							if (environment.context.getConfiguration().get("ledger.pruning", true) == true)
							{
								if(environment.getEnvironment().isClosed() == false && DatabaseEnvironment.environments.contains(environment) == true)
									environment.getEnvironment().cleanLog();
							}
							
							environment.getEnvironment().checkpoint(null);
						}
						catch(Exception ex)
						{
							log.fatal(environment.context.getName()+": Database environment checkpoint failed",ex);
						}
					}
					
					try 
					{
						long sleepFor = TimeUnit.MINUTES.toMillis(15) - (System.currentTimeMillis() - start);
						if (sleepFor > 0)
							Thread.sleep(sleepFor);
					} 
					catch (InterruptedException e) 
					{
						// DONT CARE
						Thread.currentThread().interrupt();
					}
				}
			}
		};
		
		DatabaseEnvironment.environmentCheckpointThread = new Thread(DatabaseEnvironment.environmentCheckpointRunnable, "Database checkpoint");
		DatabaseEnvironment.environmentCheckpointThread.start();
	}
	
	// Database maintenance
	private static Thread environmentMaintenanceThread;
	private static Runnable environmentMaintenanceRunnable;
	
	static 
	{
		DatabaseEnvironment.environmentMaintenanceRunnable = new Runnable() 
		{
			@Override
			public void run() 
			{
				while(1==1)
				{
					long start = System.currentTimeMillis();
					List<DatabaseEnvironment> environments = new ArrayList<DatabaseEnvironment>(DatabaseEnvironment.environments);
					for (DatabaseEnvironment environment : environments)
					{
						try
						{
							environment.getEnvironment().evictMemory();
						}
						catch(Exception ex)
						{
							log.fatal(environment.context.getName()+": Database environment maintenance failed",ex);
						}
					}
					
					try 
					{
						long sleepFor = TimeUnit.MINUTES.toMillis(1) - (System.currentTimeMillis() - start);
						if (sleepFor > 0)
							Thread.sleep(sleepFor);
					} 
					catch (InterruptedException e) 
					{
						// DONT CARE
						Thread.currentThread().interrupt();
					}
				}
			}
		};
		
		DatabaseEnvironment.environmentMaintenanceThread = new Thread(DatabaseEnvironment.environmentMaintenanceRunnable, "Database maintenance");
		DatabaseEnvironment.environmentMaintenanceThread.start();
	}

	private final Context		context;
	private final Map<Class<?>, DatabaseStore> 	databases = new HashMap<>();

	private Database 	metaDatabase;
	private Environment	environment;
	
    public DatabaseEnvironment(final Context context, final File home) 
    { 
    	this.context = Objects.requireNonNull(context, "Context is null");
    	
		home.mkdirs();

		System.setProperty("je.disable.java.adler32", "true");

		EnvironmentConfig environmentConfig = new EnvironmentConfig();
		environmentConfig.setTransactional(true);
		environmentConfig.setAllowCreate(true);
		environmentConfig.setLockTimeout(30, TimeUnit.SECONDS);
		environmentConfig.setDurability(Durability.COMMIT_NO_SYNC);
		
		if (Boolean.parseBoolean(System.getProperty("testing.unit", "false")) == true || 
			Boolean.parseBoolean(System.getProperty("testing.integration", "false")) == true || 
			Boolean.parseBoolean(System.getProperty("testing.db.memory", "false")) == true)
			environmentConfig.setConfigParam(EnvironmentConfig.LOG_MEM_ONLY, "true");

//		environmentConfig.setConfigParam(EnvironmentConfig.ENV_DUP_CONVERT_PRELOAD_ALL, "false");
		
		long logFileSize     = 250000000l;
		long logFileMinClean = 1000000000l;
		environmentConfig.setConfigParam(EnvironmentConfig.LOG_FILE_MAX, String.valueOf(logFileSize));
		environmentConfig.setConfigParam(EnvironmentConfig.LOG_FILE_CACHE_SIZE, "1000");
		environmentConfig.setConfigParam(EnvironmentConfig.LOG_FLUSH_SYNC_INTERVAL, "60 s");
		environmentConfig.setConfigParam(EnvironmentConfig.LOG_FLUSH_NO_SYNC_INTERVAL, "15 s");
		
		environmentConfig.setConfigParam(EnvironmentConfig.NODE_MAX_ENTRIES, "256");
		environmentConfig.setConfigParam(EnvironmentConfig.TREE_MAX_EMBEDDED_LN, "16");
		environmentConfig.setConfigParam(EnvironmentConfig.LOG_FAULT_READ_SIZE, "32768");
		environmentConfig.setConfigParam(EnvironmentConfig.LOG_ITERATOR_READ_SIZE, "1048576");

		// EVICTOR PARAMS //
		environmentConfig.setConfigParam(EnvironmentConfig.ENV_RUN_EVICTOR, "false");
		environmentConfig.setConfigParam(EnvironmentConfig.EVICTOR_MAX_THREADS, "1"); // TODO check one is enough, otherwise -> String.valueOf(Math.max(1, Runtime.getRuntime().availableProcessors() / 4)));
		environmentConfig.setConfigParam(EnvironmentConfig.EVICTOR_CRITICAL_PERCENTAGE, "10"); // TODO make sure this is accounted for in cache sizing!
		
		// CLEANER PARAMS //
		environmentConfig.setConfigParam(EnvironmentConfig.ENV_RUN_CLEANER, "false");
		if (this.context.getConfiguration().get("ledger.pruning", true) == true)
		{
			environmentConfig.setConfigParam(EnvironmentConfig.CLEANER_WAKEUP_INTERVAL, "3600 s");
			environmentConfig.setConfigParam(EnvironmentConfig.CLEANER_BYTES_INTERVAL, String.valueOf(1l<<32l));
			environmentConfig.setConfigParam(EnvironmentConfig.CLEANER_MIN_AGE, String.valueOf((int) (logFileMinClean / logFileSize)));
			environmentConfig.setConfigParam(EnvironmentConfig.CLEANER_LOOK_AHEAD_CACHE_SIZE, "2097152");
			environmentConfig.setConfigParam(EnvironmentConfig.CLEANER_READ_SIZE, "1048576");
			environmentConfig.setConfigParam(EnvironmentConfig.CLEANER_MIN_UTILIZATION, "60");
			environmentConfig.setConfigParam(EnvironmentConfig.CLEANER_MIN_FILE_UTILIZATION, "33");
		}

		// CHECKPOINTER PARAMS //
		environmentConfig.setConfigParam(EnvironmentConfig.ENV_RUN_CHECKPOINTER, "false");
		environmentConfig.setConfigParam(EnvironmentConfig.CHECKPOINTER_BYTES_INTERVAL, String.valueOf(1l<<28l));

		// Additional Environment functions which are invoked manually
		environmentConfig.setConfigParam(EnvironmentConfig.ENV_RUN_VERIFIER, "false");

		if (ENV_MINIMAL_CACHE)
		{
			long cacheSize = 1<<24;
			environmentConfig.setCacheMode(CacheMode.DEFAULT);
			environmentConfig.setCacheSize(cacheSize);
		}
		else
		{
			long cacheSize = (long) (Runtime.getRuntime().maxMemory()*0.125) / this.context.getConfiguration().getCommandLine("contexts", 1);
			environmentConfig.setCacheSize(cacheSize);
		}

		this.environment = new Environment(home, environmentConfig);
        DatabaseEnvironment.environments.add(this);

		DatabaseConfig primaryConfig = new DatabaseConfig();
		primaryConfig.setAllowCreate(true);
		primaryConfig.setTransactional(true);

		this.metaDatabase = getEnvironment().openDatabase(null, "environment.meta_data", primaryConfig);
	}

	public void close()
	{
        flush();
        
        if (DatabaseEnvironment.environments.remove(this) == false)
        	log.fatal(this.context.getName()+": Database environment not in global database environments");

        Collection<DatabaseStore> databases = new ArrayList<>(this.databases.values());
        for (DatabaseStore database : databases)
        {
			try
        	{
				database.close();
			}
        	catch (Exception ex)
			{
        		log.error("Failure stopping database "+database.getClass().getName(), ex);
			}
        }

        this.metaDatabase.close();
		this.metaDatabase = null;

       	this.environment.close();
       	this.environment = null;
	}

	public Environment getEnvironment()
	{
		return this.environment;
	}

	public void flush()
	{
        for (DatabaseStore database : this.databases.values())
        {
            try { database.flush(); } catch (Exception ex)
            {
            	log.error("Flushing "+database.getClass().getName()+" failed", ex);
    		}
        }
	}

	public void register(final DatabaseStore database)
	{
		Objects.requireNonNull(database, "Database to register is null");
		
		if (this.databases.containsKey(database.getClass()) == false)
			this.databases.put(database.getClass(), database);
	}

	public boolean isRegistered(final DatabaseStore database) 
	{
		Objects.requireNonNull(database, "Database is null");
		return this.databases.containsKey(database.getClass());
	}

	public void deregister(final DatabaseStore database)
	{
		Objects.requireNonNull(database, "Database to deregister is null");
        this.databases.remove(database.getClass());
	}

	public OperationStatus put(final Transaction transaction, final String resource, final String key, final byte[] value)
	{
		return this.put(transaction, resource, new DatabaseEntry(key.getBytes()), new DatabaseEntry(value));
	}

	public OperationStatus put(final Transaction transaction, final String resource, final String key, final DatabaseEntry value)
	{
		return this.put(transaction, resource, new DatabaseEntry(key.getBytes()), value);
	}

	public OperationStatus put(final Transaction transaction, final String resource, final DatabaseEntry key, final DatabaseEntry value)
	{
		Objects.requireNonNull(resource, "Resource is null");
		if (resource.length() == 0)
			throw new IllegalArgumentException("Resource length is 0");

		Objects.requireNonNull(key, "Key is null");
		Objects.requireNonNull(key.getData(), "Key data is null");
		if (key.getData().length == 0)
			throw new IllegalArgumentException("Key data length is 0");

		Objects.requireNonNull(value, "Value is null");
		Objects.requireNonNull(value.getData(), "Value data is null");
		if (value.getData().length == 0)
			throw new IllegalArgumentException("Value data length is 0");

		// Create a key specific to the database //
		key.setData(Bytes.concatenate(resource.getBytes(StandardCharsets.UTF_8), key.getData()));

		return this.metaDatabase.put(transaction, key, value);
	}

	public byte[] get(final String resource, final String key)
	{
		DatabaseEntry value = new DatabaseEntry();

		if (this.get(resource, new DatabaseEntry(key.getBytes()), value) == OperationStatus.SUCCESS)
			return value.getData();

		return null;
	}

	public OperationStatus get(final String resource, final String key, final DatabaseEntry value)
	{
		return this.get(resource, new DatabaseEntry(key.getBytes()), value);
	}

	public OperationStatus get(final String resource, final DatabaseEntry key, final DatabaseEntry value)
	{
		Objects.requireNonNull(resource, "Resource is null");
		if (resource.length() == 0)
			throw new IllegalArgumentException("Resource length is 0");

		Objects.requireNonNull(key, "Key is null");
		Objects.requireNonNull(key.getData(), "Key data is null");
		if (key.getData().length == 0)
			throw new IllegalArgumentException("Key data length is 0");

		// Create a key specific to the database //
		key.setData(Bytes.concatenate(resource.getBytes(StandardCharsets.UTF_8), key.getData()));
		
		return this.metaDatabase.get(null, key, value, LockMode.READ_UNCOMMITTED);
	}
	
	public Transaction beginTransaction(final Transaction parent, final TransactionConfig config)
	{
		return this.environment.beginTransaction(parent, config);
	}
	
	public Database openDatabase(final Transaction transaction, final String name, final DatabaseConfig config)
	{
		return this.environment.openDatabase(transaction, name, config);
	}

	public SecondaryDatabase openSecondaryDatabase(final Transaction transaction, final String name, final Database database, final SecondaryConfig config)
	{
		return this.environment.openSecondaryDatabase(transaction, name, database, config);
	}

	public long truncateDatabase(final Transaction transaction, final String name, final boolean count)
	{
		return this.environment.truncateDatabase(transaction, name, count);
	}
}
