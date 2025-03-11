package org.radix.hyperscale;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;

import org.java_websocket.WebSocketImpl;
import org.radix.hyperscale.crypto.CryptoException;
import org.radix.hyperscale.crypto.KeyPair;
import org.radix.hyperscale.crypto.bls12381.BLSKeyPair;
import org.radix.hyperscale.database.DatabaseEnvironment;
import org.radix.hyperscale.database.SystemMetaData;
import org.radix.hyperscale.database.vamos.Environment;
import org.radix.hyperscale.database.vamos.EnvironmentConfig;
import org.radix.hyperscale.events.Events;
import org.radix.hyperscale.exceptions.ServiceException;
import org.radix.hyperscale.exceptions.StartupException;
import org.radix.hyperscale.exceptions.TerminationException;
import org.radix.hyperscale.executors.Executor;
import org.radix.hyperscale.executors.ScheduledExecutable;
import org.radix.hyperscale.ledger.Ledger;
import org.radix.hyperscale.ledger.ShardGroupID;
import org.radix.hyperscale.ledger.ShardMapper;
import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;
import org.radix.hyperscale.network.Network;
import org.radix.hyperscale.node.LocalNode;
import org.radix.hyperscale.node.Node;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.Serialization;
import org.radix.hyperscale.serialization.DsonOutput.Output;
import org.radix.hyperscale.utils.Numbers;
import org.radix.hyperscale.utils.TimeSeriesStatistics;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

public final class Context implements Service
{
	private static final Logger log = Logging.getLogger ();
	
	private static final Map<String, Context> contexts = Collections.synchronizedMap(new HashMap<String, Context>());
	private static final AtomicInteger incrementer = new AtomicInteger(0);
	private static Context _default = null;
	
	public static final Context create() throws StartupException
	{
		return Context.create("node-"+contexts.size(), Configuration.getDefault());
	}

	public static final Context create(final Configuration config) throws StartupException
	{
		return Context.create("node-"+contexts.size(), Objects.requireNonNull(config, "Configuration is null"));
	}

	public static final Context createAndStart() throws StartupException
	{
		return createAndStart("node-"+contexts.size(), Configuration.getDefault());
	}

	public static final Context createAndStart(final Configuration config) throws StartupException
	{
		return createAndStart("node-"+contexts.size(), config);
	}

	public static final Context createAndStart(final String name, final Configuration config) throws StartupException
	{
		Context context = create(name.toLowerCase(), config);
		context.start();
		return context;
	}
	
	public static final Collection<Context> createAndStart(final int count, final String name, final Configuration configuration) throws StartupException
	{
		List<Context> contexts = new ArrayList<Context>();
		try
		{
			for (int cc = 0 ; cc < count ; cc++)
			{
				// Create a key if required and contexts is more than 1
				// TODO make this more intelligent with options
				if (count > 1)
				{
					int nodesPerShardGroup = Math.max(1, count / Universe.getDefault().shardGroupCount());
					ShardGroupID shardGroupID = ShardGroupID.from(cc / nodesPerShardGroup);
					
					try
					{
						final File keyFile = new File(name.toLowerCase()+"-"+incrementer.get()+".key");
						final BLSKeyPair BLSKey;
						if (keyFile.exists() == false)
							BLSKey = null;
						else
							BLSKey = BLSKeyPair.fromFile(keyFile, false);
						
						if (BLSKey == null || (BLSKey != null && ShardMapper.toShardGroup(BLSKey.getIdentity(), Universe.getDefault().shardGroupCount()).equals(shardGroupID) == false))
						{
							if (BLSKey == null || Universe.getDefault().getValidators().stream().anyMatch(bp -> bp.equals(BLSKey.getIdentity())) == false)
							{
								BLSKeyPair validatorKey;
								do
								{
									validatorKey = new BLSKeyPair();
									if (shardGroupID.equals(ShardMapper.toShardGroup(validatorKey.getIdentity(), Universe.getDefault().shardGroupCount())) == false)
										validatorKey = null;
								}
								while(validatorKey == null);
			
								KeyPair.toFile(keyFile, validatorKey);
							}
						}
					}
					catch (CryptoException cex)
					{
						throw new StartupException("Existing identity key pair for "+name.toLowerCase()+"-"+incrementer.get()+" may be corrupt", cex);
					}
					catch (IOException ioex)
					{
						throw new StartupException("Could not generate context identity key pairs", ioex);
					}
				}
				
				Context context = Context.create(name.toLowerCase()+"-"+incrementer.get(), configuration);
				context.start();
				contexts.add(context);
			}
			
			return contexts;
		}
		catch (StartupException stex)
		{
			for (Context context : contexts)
			{
				try
				{
					context.stop();
				}
				catch (TerminationException tex)
				{
					log.fatal(tex);
				}
			}
			
			throw stex;
		}
	}
	
	public static final Context create(final String name, final Configuration configuration) throws StartupException
	{
		Objects.requireNonNull(name);
		Objects.requireNonNull(configuration);
		
		synchronized(contexts)
		{
			if (contexts.containsKey(name.toLowerCase()))
				throw new IllegalStateException("Context "+name.toLowerCase()+" already created");
			
			Configuration contextConfig = new Configuration(Objects.requireNonNull(configuration));
			contextConfig.set("network.port", configuration.get("network.port", Universe.getDefault().getPort())+incrementer.get());
			contextConfig.set("network.udp", configuration.get("network.udp", Universe.getDefault().getPort())+incrementer.get());
			contextConfig.set("websocket.port", configuration.get("websocket.port", WebSocketImpl.DEFAULT_PORT)+incrementer.get());
			contextConfig.set("ledger.synced.always", Boolean.getBoolean("singleton"));
			
			if (incrementer.get() > 0)
			{
				Set<String> seedSet = new HashSet<String>();

				if (configuration.has("network.seeds"))
				{
					String[] seeds = configuration.get("network.seeds", "").split(",");
					for (String seed : seeds)
						seedSet.add(seed);
				}
				
				if (seedSet.isEmpty())
					seedSet.add(contextConfig.get("network.address", "127.0.0.1"));
				
				contextConfig.set("network.seeds", String.join(",", seedSet));
			}

			LocalNode node;
			try
			{
				node = LocalNode.load(name.toLowerCase(), contextConfig, true);
				log.out(name.toLowerCase()+": Init node "+node.getIdentity().toString(12));
			}
			catch (CryptoException e)
			{
				throw new StartupException(e);
			}
			
			for (Context context : contexts.values())
				if (context.node.getIdentity().equals(node.getIdentity()))
					throw new IllegalStateException("Context "+name.toLowerCase()+":"+node+" already created");
			
			Context context;
			try
			{
				context = new Context(name.toLowerCase(), node, contextConfig);
				log.out(name.toLowerCase()+": Init context for node "+node.getIdentity().toString(12));
			}
			catch (IOException e)
			{
				throw new StartupException(e);
			}
			
			Context.incrementer.incrementAndGet();
			contexts.put(name.toLowerCase(), context);
			
			if (Context._default == null)
				Context._default = context;
			
			return context;
		}
	}

	public static final void stop(final Context context) throws TerminationException
	{
		Objects.requireNonNull(context, "Context is null");
		context.stop();
		
		contexts.remove(context.getName());
		if (Context._default == context)
			Context._default = null;
	}

	public static final void stopAll() throws TerminationException
	{
		synchronized(contexts)
		{
			for (Context context : contexts.values())
				Objects.requireNonNull(context).stop();
		
			contexts.clear();
			Context._default = null;
		}
	}

	public static final void setDefault(final Context context)
	{
		Objects.requireNonNull(context, "Context is null");
		Context._default = context;
	}

	public static final Context get()
	{
		return Context._default;
	}

	public static final Context get(String name)
	{
		return contexts.get(name.toLowerCase());
	}
	
	public static final Collection<Context> getAll()
	{
		return Collections.unmodifiableCollection(new ArrayList<Context>(contexts.values()));
	}
	
	public static void clear()
	{
		contexts.clear();
	}
	
	// FIXME: This is a pretty horrible way of ensuring unit tests are stable,
	// as freeMemory() can return varying numbers between calls.
	// This is adjusted via reflection in the unit tests to be something that
	// returns a constant.
	private static LongSupplier freeMemory = () -> Runtime.getRuntime().freeMemory();
	private static LongSupplier maxMemory = () -> Runtime.getRuntime().maxMemory();
	private static LongSupplier totalMemory = () -> Runtime.getRuntime().totalMemory();

	private final String 			name;
	private final LocalNode			node;
	private final Configuration		configuration;
	private final Events			events;
	private final Network			network;
	private final Ledger			ledger;
	private final DatabaseEnvironment environment;
	
	// Statistics //
	private final SystemMetaData 	metaData;
	private final Map<String, TimeSeriesStatistics> timeSeries;
	
	private final Environment vamos;
	
	private boolean isStarted = false;
	private boolean isStopped = false;
	private long startedAt = 0;
	private long stoppedAt = 0;
	private transient Future<?> metaDataTaskFuture;

	/** A null Context used for unit testing */
	@SuppressWarnings("unused")
	private Context(final Configuration configuration)
	{
		this.name = "null";
		this.configuration = Objects.requireNonNull(configuration, "Configuration is null");
		this.node = null;
		this.environment = null;
		this.events = null;
		this.metaData = null;
		this.network = null;
		this.ledger = null;
		this.vamos = null;
		this.timeSeries = null;
	}
	
	public Context(final String name, final LocalNode node, final Configuration configuration) throws IOException
	{
		Objects.requireNonNull(name, "Context name is null");
		Numbers.isZero(name.length(), "Name is empty");
		this.name = name.toLowerCase();
		this.configuration = Objects.requireNonNull(configuration, "Configuration is null");
		this.node = Objects.requireNonNull(node, "Local node is null");
		this.environment = new DatabaseEnvironment(this, new File(System.getProperty("user.dir")+File.separatorChar+"database"+File.separatorChar+name));
		
		if (configuration.get("system.stress.memory", Boolean.FALSE))
		{
			EnvironmentConfig vamosConfig = new EnvironmentConfig();
			vamosConfig.hasLogCache(true);
			vamosConfig.setLogCacheMaxEntries(1<<20);
			vamosConfig.setLogCacheMaxItemSize(1<<16);
			vamosConfig.setIndexItemCacheMaxEntries(1<<24);
			
			this.vamos = new Environment(new File(System.getProperty("user.dir")+File.separatorChar+"database"+File.separatorChar+name+File.separatorChar+"vamos"), vamosConfig);
		}
		else
		{
			EnvironmentConfig vamosConfig = new EnvironmentConfig();
			vamosConfig.setLogCompressionThreshold(1<<16);
			vamosConfig.hasIndexItemCache(true);
			vamosConfig.setIndexItemCacheMaxEntries(1<<16);
			vamosConfig.setIndexNodeCount(1<<18);
			vamosConfig.setIndexNodesPerFile(1<<16);
			vamosConfig.setIndexNodeCacheMaxEntries(1<<18);
			this.vamos = new Environment(new File(System.getProperty("user.dir")+File.separatorChar+"database"+File.separatorChar+name+File.separatorChar+"vamos"), vamosConfig);
		}
		
		this.events = new Events(this);
		this.network = new Network(this);
		this.ledger = new Ledger(this);

		this.metaData = new SystemMetaData(this);
		this.timeSeries = Collections.synchronizedMap(new HashMap<>());
		
		if (Boolean.parseBoolean(System.getProperty("testing.unit", "false")) == true)
			node.setSynced(true);
	}
	
	@Override
	public void start() throws StartupException
	{
		if (this.isStarted)
			throw new StartupException("Context "+this.getName()+" is already started");
	
		log.info(this.name+": Starting context ...");
		
		try
		{
			this.events.start();
			this.metaData.start();		
			if (this.metaData.has("node.local")) 
			{
				try 
				{
					byte[] nodeBytes = this.metaData.get("node.local", (byte[]) null);
					if (nodeBytes == null)
						throw new IllegalStateException("Expected node.local bytes but got null");
					
					Node persisted = Serialization.getInstance().fromDson(nodeBytes, Node.class);
	
					if (persisted.getIdentity().equals(Context.this.node.getIdentity()) == false) // TODO what happens if has changed?  Dump everything?
						log.warn("Node key has changed from "+persisted.getIdentity()+" to "+Context.this.node.getIdentity());
					
					Context.this.node.fromPersisted(persisted);
				} 
				catch (IOException ex) 
				{
					log.error("Could not load persisted system state from SystemMetaData", ex);
				}
			}
			
			this.network.start();
			this.ledger.start();
			
			this.node.setHead(this.ledger.getHead());
			this.metaDataTaskFuture = Executor.getInstance().scheduleAtFixedRate(new ScheduledExecutable(1, 1, TimeUnit.SECONDS)
			{
				@Override
				public void execute()
				{
					try 
					{
						byte[] nodeBytes = Serialization.getInstance().toDson(Context.this.getNode(), Output.PERSIST);
						Context.this.metaData.put("node.local", nodeBytes);
					} 
					catch (IOException e) 
					{
						log.error("Could not persist local node state", e);
					}
				}
			});
		}
		finally
		{
			this.isStopped = false;
			this.stoppedAt = 0;
			this.isStarted = true;
			this.startedAt = System.currentTimeMillis();
		}
	}
	
	@Override
	public void stop() throws TerminationException
	{
		try
		{
			if (this.isStopped)
				throw new TerminationException("Context "+this.getName()+" is already stopped");
	
			this.network.stop();
			this.ledger.stop();
			this.metaDataTaskFuture.cancel(false);
			this.metaData.stop();
			this.events.stop();
			this.vamos.close();
			this.environment.close();
		}
		finally
		{
			this.isStarted = false;
			this.isStopped = true;
			this.stoppedAt = System.currentTimeMillis();
		}
	}
	
	@Override
	public void clean() throws ServiceException
	{
		this.ledger.clean();
		this.network.clean();
		this.metaData.clean();
	}
	
	public long startedAt()
	{
		return this.startedAt;
	}

	public long stoppedAt()
	{
		return this.stoppedAt;
	}
	
	public boolean isStarted()
	{
		return this.isStarted;
	}

	public boolean isStopped()
	{
		return this.isStopped;
	}

	public long getUptime()
	{
		if (this.stoppedAt < this.startedAt)
			return System.currentTimeMillis() - this.startedAt;
		else
			return this.stoppedAt - this.startedAt;
	}

	public Configuration getConfiguration()
	{
		return this.configuration;
	}

	public Network getNetwork()
	{
		return this.network;
	}
	
	public Events getEvents()
	{
		return this.events;
	}

	public LocalNode getNode()
	{
		return this.node;
	}

	public Ledger getLedger()
	{
		return this.ledger;
	}

	public SystemMetaData getMetaData()
	{
		return this.metaData;
	}

	public DatabaseEnvironment getDatabaseEnvironment()
	{
		return this.environment;
	}
	
	public Environment getVamosEnvironment()
	{
		return this.vamos;
	}
	
	public TimeSeriesStatistics getTimeSeries(final String name)
	{
		synchronized(this.timeSeries)
		{
			return this.timeSeries.computeIfAbsent(name, n -> new TimeSeriesStatistics());
		}
	}

	// Property "ledger" - 1 getter
	// No really obvious way of doing this better
	@JsonProperty("ledger")
	@DsonOutput(Output.API)
	Map<String, Object> getJsonLedger() 
	{
		SystemMetaData smd = this.metaData;

		Map<String, Object> latency = ImmutableMap.of(
			"path", smd.get("ledger.latency.path", 0),
			"persist", smd.get("ledger.latency.persist", 0)
		);

		Map<String, Object> faults = ImmutableMap.of(
			"tears", smd.get("ledger.faults.tears", 0),
			"assists", smd.get("ledger.faults.assists", 0),
			"stitched", smd.get("ledger.faults.stitched", 0),
			"failed", smd.get("ledger.faults.failed", 0)
		);

		return ImmutableMap.<String, Object>builder().
			put("processed", smd.get("ledger.processed", 0)).
			put("processing", smd.get("ledger.processing", 0)).
			put("stored", smd.get("ledger.stored", 0)).
			put("storedPerShard", smd.get("ledger.storedPerShard", "0")).
			put("storing", smd.get("ledger.storing", 0)).
			put("storingPerShard", smd.get("ledger.storingPerShard", 0)).
			put("storing.peak", smd.get("ledger.storing.peak", 0)).
			put("checksum", smd.get("ledger.checksum", 0)).
			put("latency", latency).
			put("faults", faults).build();
	}

	// Property "global" - 1 getter
	@JsonProperty("global")
	@DsonOutput(Output.API)
	Map<String, Object> getJsonGlobal() 
	{
		return ImmutableMap.of(
			"stored", this.metaData.get("ledger.network.stored", 0),
			"processing", this.metaData.get("ledger.network.processing", 0),
			"storing", this.metaData.get("ledger.network.storing", 0)
		);
	}

	// Property "events" - 1 getter
	@JsonProperty("events")
	@DsonOutput(Output.API)
	Map<String, Object> getJsonEvents() 
	{
		SystemMetaData smd = this.metaData;

		Map<String, Object> processed = ImmutableMap.of(
			"synchronous", smd.get("events.processed.synchronous", 0L),
			"asynchronous", smd.get("events.processed.asynchronous", 0L)
		);

		return ImmutableMap.of(
			"processed", processed,
			"processing", smd.get("events.processing", 0L),
			"broadcast",  smd.get("events.broadcast", 0L),
			"queued", smd.get("events.queued", 0L),
			"dequeued", smd.get("events.dequeued", 0L)
		);
	}

	// Property "messages" - 1 getter
	// No obvious improvements here
	@JsonProperty("messages")
	@DsonOutput(Output.API)
	Map<String, Object> getJsonMessages() {
		Map<String, Object> outbound = ImmutableMap.of(
				"sent", this.metaData.get("messages.outbound.sent", 0),
				"processed", this.metaData.get("messages.outbound.processed", 0),
				"pending", this.metaData.get("messages.outbound.pending", 0),
				"aborted", this.metaData.get("messages.outbound.aborted", 0));
		Map<String, Object> inbound = ImmutableMap.of(
				"processed", this.metaData.get("messages.inbound.processed", 0),
				"received", this.metaData.get("messages.inbound.received", 0),
				"pending", this.metaData.get("messages.inbound.pending", 0),
				"discarded", this.metaData.get("messages.inbound.discarded", 0));
		return ImmutableMap.of(
				"inbound", inbound,
				"outbound", outbound);
	}

	// Property "memory" - 1 getter
	// No obvious improvements here
	@JsonProperty("memory")
	@DsonOutput(Output.API)
	Map<String, Object> getJsonMemory() 
	{
		return ImmutableMap.of(
				"free", freeMemory.getAsLong(),
				"total", totalMemory.getAsLong(),
				"max", maxMemory.getAsLong());
	}

	// Property "processors" - 1 getter
	// No obvious improvements here
	@JsonProperty("processors")
	@DsonOutput(Output.API)
	int getJsonProcessors() 
	{
		return Runtime.getRuntime().availableProcessors();
	}
	
	@Override
	public final String getName()
	{
		return this.name;
	}
	
	@Override
	public String toString()
	{
		return this.name+":"+this.node.getIdentity();
	}
}
