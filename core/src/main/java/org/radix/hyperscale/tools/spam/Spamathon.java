package org.radix.hyperscale.tools.spam;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.Range;
import org.radix.hyperscale.Plugin;
import org.radix.hyperscale.executors.Executable;
import org.radix.hyperscale.ledger.ShardGroupID;
import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;

final public class Spamathon
{
	private static final Logger spammerLog = Logging.getLogger("spammer");
	static
	{
		spammerLog.setLevels(Logging.ERROR | Logging.FATAL | Logging.INFO | Logging.WARN);
	}

	private static final int MAX_SPAM_EXECUTION_THREADS = 32;
	private static Spamathon instance;
	
	public static Spamathon getInstance()
	{
		if (instance == null)
			instance = new Spamathon();
		
		return instance;
	}
	
	private final int numSpamExecutors;
	private final ExecutorService spamExecutor;
	private volatile Executable spammer = null;
	
	private final Map<String, Class<? extends Spammer>> spammers = new HashMap<String, Class<? extends Spammer>>();
	
	private Spamathon()
	{ 
		// Load all the internal spam plugins
		Plugin.loadTypeHierarchy("org.radix.hyperscale", Spammer.class, clazz -> {
			if (Spammer.class.isAssignableFrom(clazz) == false)
			{
				spammerLog.error("Class "+clazz+" is not a Spammer plugin");
				return;
			}
			
			@SuppressWarnings("unchecked")
			Class<? extends Spammer> spammer = (Class<? extends Spammer>) clazz;
			SpamConfig spamConfig = spammer.getAnnotation(SpamConfig.class);
			if (spamConfig == null)
			{
				spammerLog.error("Loading of spammer failed: No spam config annotation "+spammer);
				return;
			}
			
			if (Spamathon.this.spammers.putIfAbsent(spamConfig.name(), spammer) != null)
				spammerLog.error("Spammer plugin is already loaded: "+spammer);
		});
		
		this.numSpamExecutors = Math.max(1, Math.min(MAX_SPAM_EXECUTION_THREADS, Runtime.getRuntime().availableProcessors()));
		this.spamExecutor = Executors.newFixedThreadPool(this.numSpamExecutors, new ThreadFactory() 
		{
			private final AtomicInteger counter = new AtomicInteger(0);
				
			@Override
			public Thread newThread(Runnable r) 
			{
				Thread thread = new Thread(r, "spam-thread-"+this.counter.getAndIncrement());
				thread.setPriority(3);
				thread.setDaemon(true);
				return thread;
			}
		});
	}

	int getExecutorCount()
	{
		return this.numSpamExecutors;
	}
	
	ExecutorService getExecutorService()
	{
		if (this.numSpamExecutors <= 1)
			return null;
		
		return this.spamExecutor;
	}
	
	public boolean isSpamming()
	{
		return this.spammer == null ? false : true;
	}
	
	public void completed(Executable spammer)
	{
		if (this.spammer == spammer)
			this.spammer = null;
	}
	
	public void cancel()
	{
		if (this.spammer == null)
			throw new IllegalStateException("No spammer currently running");
		
		if (this.spammer.cancel() == false)
			throw new IllegalStateException("Current spam session could not be cancelled");
	}
	
	public Class<? extends Spammer> get(String name)
	{
		return this.spammers.get(name);
	}
	
	@SuppressWarnings("unchecked")
	public <T extends Spammer> T spam(final String name, final int iterations, final int rate, final Range<Integer> saturation, 
									  final double shardFactor, final ShardGroupID targetShard, String[] options) throws InstantiationException, NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException 
	{
		if (Spamathon.getInstance().isSpamming())
	        throw new IllegalStateException("Already an instance of spammer running");

	    Class<? extends Spammer> spammerClass = this.spammers.get(name.toLowerCase());
	    if (spammerClass == null)
	        throw new InstantiationException("Spammer plugin is unknown: " + name);

	    Spammer spammer;
	    final Map<String, Object> optionsMap = parseOptions(options);
	    if (optionsMap.isEmpty()) 
	    {
	        try 
	        {
	            // Try the constructor without options map first
	            final Constructor<? extends Spammer> constructor = spammerClass.getDeclaredConstructor(Spamathon.class, int.class, int.class, Range.class, double.class, ShardGroupID.class);
	            spammer = constructor.newInstance(this, iterations, rate, saturation, shardFactor, targetShard);
	        } 
	        catch (NoSuchMethodException e) 
	        {
	            // Fall back to constructor with options map
	            final Constructor<? extends Spammer> constructor = spammerClass.getDeclaredConstructor(Spamathon.class, int.class, int.class, Range.class, double.class, ShardGroupID.class, Map.class);
	            spammer = constructor.newInstance(this, iterations, rate, saturation, shardFactor, targetShard, optionsMap);
	        }
	    } 
	    else 
	    {
	        try 
	        {
	            // Try constructor with options map first
	            final Constructor<? extends Spammer> constructor = spammerClass.getDeclaredConstructor(Spamathon.class, int.class, int.class, Range.class, double.class, ShardGroupID.class, Map.class);
	            spammer = constructor.newInstance(this, iterations, rate, saturation, shardFactor, targetShard, optionsMap);
	        } 
	        catch (NoSuchMethodException e) 
	        {
	            throw new NoSuchMethodException("No suitable constructor found for "+spammerClass.getName()+": Expected constructor with options map parameter");
	        }
	    }	    

	    final Future<?> spamFuture = this.spamExecutor.submit(spammer);		
	    spammer.setFuture(spamFuture);
		this.spammer = spammer;

		return (T) spammer;
	}

	private Map<String, Object> parseOptions(String[] options) 
	{
		final Map<String, Object> result = new HashMap<>();

		if (options == null || options.length == 0)
			return result;

		for (int i = 0; i < options.length; i++) 
		{
			String option = options[i];

			// Handle singleton options (boolean flags)
			if (i == options.length - 1 || options[i + 1].startsWith("-") || options[i + 1].startsWith("--")) 
			{
				result.put(option, true);
			} 
			else 
			{
				// Handle option pairs (name + value)
				String value = options[++i];

				// Try to convert value to appropriate type
				Object typedValue = convertValue(value);
				
				// Remove the leading '-' or '--' on the option name
				if (option.startsWith("-")) 
					result.put(option.substring(1), typedValue);
				else if (option.startsWith("--"))
					result.put(option.substring(2), typedValue);
				else
					result.put(option, typedValue);
			}
		}

		return result;
	}

	private Object convertValue(String value) 
	{
		try 
		{
			return Integer.parseInt(value);
		} 
		catch (NumberFormatException e) 
		{
			// Not an integer
		}

		// Try double
		try 
		{
			return Double.parseDouble(value);
		} 
		catch (NumberFormatException e) 
		{
			// Not a double
		}

		// Try boolean
		if (value.equalsIgnoreCase("true") || value.equalsIgnoreCase("false")) 
			return Boolean.parseBoolean(value);

		// Default to string
		return value;
	}
}
