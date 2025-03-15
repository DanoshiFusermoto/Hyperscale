package org.radix.hyperscale.ledger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.commons.lang3.mutable.MutableLong;
import org.eclipse.collections.api.factory.Sets;
import org.radix.hyperscale.Constants;
import org.radix.hyperscale.Context;
import org.radix.hyperscale.Service;
import org.radix.hyperscale.common.Primitive;
import org.radix.hyperscale.concurrency.MonitoredReadWriteLock;
import org.radix.hyperscale.crypto.CryptoException;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.crypto.Hashable;
import org.radix.hyperscale.database.DatabaseException;
import org.radix.hyperscale.events.EventListener;
import org.radix.hyperscale.events.SyncLostEvent;
import org.radix.hyperscale.events.SynchronousEventListener;
import org.radix.hyperscale.exceptions.StartupException;
import org.radix.hyperscale.exceptions.TerminationException;
import org.radix.hyperscale.ledger.BlockHeader.InventoryType;
import org.radix.hyperscale.ledger.events.AtomExceptionEvent;
import org.radix.hyperscale.ledger.events.AtomProvisionedEvent;
import org.radix.hyperscale.ledger.events.BlockCommittedEvent;
import org.radix.hyperscale.ledger.events.PackageLoadedEvent;
import org.radix.hyperscale.ledger.events.SyncAcquiredEvent;
import org.radix.hyperscale.ledger.messages.GetPackageMessage;
import org.radix.hyperscale.ledger.messages.PackageMessage;
import org.radix.hyperscale.ledger.messages.SyncAcquiredMessage;
import org.radix.hyperscale.ledger.sme.Component;
import org.radix.hyperscale.ledger.sme.Package;
import org.radix.hyperscale.ledger.sme.PolyglotPackage;
import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;
import org.radix.hyperscale.network.AbstractConnection;
import org.radix.hyperscale.network.ConnectionState;
import org.radix.hyperscale.network.GossipFetcher;
import org.radix.hyperscale.network.GossipInventory;
import org.radix.hyperscale.network.GossipReceiver;
import org.radix.hyperscale.network.MessageProcessor;
import org.radix.hyperscale.network.StandardConnectionFilter;
import org.radix.hyperscale.network.messages.InventoryMessage;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.eventbus.Subscribe;
import com.sleepycat.je.OperationStatus;

public final class PackageHandler implements Service
{
	private static final Logger packageLog = Logging.getLogger("packages");
	private static final Logger syncLog = Logging.getLogger("sync");
	
	private final Context context;
	
	private final Map<Hash, PolyglotPackage> uncommitted = Collections.synchronizedMap(new LinkedHashMap<Hash, PolyglotPackage>());
	private final Map<StateAddress, PolyglotPackage> cached = Collections.synchronizedMap(new HashMap<StateAddress, PolyglotPackage>());
	private final Multimap<StateAddress, PendingAtom> pending = Multimaps.synchronizedMultimap(HashMultimap.create());

	private final ReentrantReadWriteLock lock;
	
	PackageHandler(final Context context)
	{
		this.context = Objects.requireNonNull(context);
		
		this.lock = new MonitoredReadWriteLock(this.context.getName()+" Package Handler Lock", true);
	}

	@Override
	public void start() throws StartupException 
	{
		// Gossip pipeline for packages is currently only used for sync.
		// Perhaps in the future its worth to align the package handler with all other primitives and use
		// the gossip pipeline for regular delivery between validators in the sane shard group.
		this.context.getNetwork().getGossipHandler().register(PolyglotPackage.class, new GossipInventory() 
		{
			@Override
			public Collection<Hash> required(final Class<? extends Primitive> type, final Collection<Hash> items, final AbstractConnection connection) throws IOException
			{
				if (type.equals(PolyglotPackage.class) == false)
				{
					packageLog.error(PackageHandler.this.context.getName()+": Polyglot package type expected but got "+type);
					return Collections.emptyList();
				}
				
				if (PackageHandler.this.context.getNode().isSynced() == false)
					return Collections.emptyList();
				
				final List<Hash> required = new ArrayList<Hash>(items);
				final Iterator<Hash> requiredIterator = required.iterator();
				synchronized(PackageHandler.this.uncommitted)
				{
					while(requiredIterator.hasNext())
					{
						if (PackageHandler.this.uncommitted.containsKey(requiredIterator.next()))
							requiredIterator.remove();
					}
				}
				required.removeAll(PackageHandler.this.context.getLedger().getLedgerStore().has(required, type));

				return required;
			}
		});

		this.context.getNetwork().getGossipHandler().register(PolyglotPackage.class, new GossipReceiver<PolyglotPackage>() 
		{
			@Override
			public void receive(final Collection<PolyglotPackage> polyglotPackages, final AbstractConnection connection) throws IOException, CryptoException
			{
				if (PackageHandler.this.context.getNode().isSynced() == false)
					return;
				
				final BlockHeader head = PackageHandler.this.context.getLedger().getHead();
				for (final PolyglotPackage polyglotPackage : polyglotPackages)
				{
					if (ShardMapper.equal(PackageHandler.this.context.getLedger().numShardGroups(), polyglotPackage.getAddress(), PackageHandler.this.context.getNode().getIdentity()) == true)
					{
						packageLog.warn(PackageHandler.this.context.getName()+": Received polyglot package for local shard "+polyglotPackage);
						// 	Disconnected and ban
						continue;
					}
					
					if (packageLog.hasLevel(Logging.DEBUG))
						packageLog.debug(PackageHandler.this.context.getName()+": Received "+polyglotPackage);
					
					PackageHandler.this.lock.writeLock().lock();
					try
					{
						final PolyglotPackage cachedPackage = PackageHandler.this.cached.putIfAbsent(polyglotPackage.getAddress(), polyglotPackage);
						if (cachedPackage != null)
							continue;

						if (PackageHandler.this.context.getLedger().getLedgerStore().has(polyglotPackage.getHash(), PolyglotPackage.class))
							continue;
						
						if (PackageHandler.this.context.getLedger().getLedgerStore().store(polyglotPackage).equals(OperationStatus.SUCCESS))
							PackageHandler.this.uncommitted.put(polyglotPackage.getHash(), cachedPackage);
						else
							packageLog.error(PackageHandler.this.context.getName()+": Failed to store "+polyglotPackage+" from "+connection);
					}
					finally
					{
						PackageHandler.this.lock.writeLock().unlock();
					}
				}
			}
		});
					
		this.context.getNetwork().getGossipHandler().register(PolyglotPackage.class, new GossipFetcher<PolyglotPackage>() 
		{
			@Override
			public Collection<PolyglotPackage> fetch(final Collection<Hash> items, final AbstractConnection connection) throws IOException
			{
				final Set<Hash> toFetch = Sets.mutable.ofAll(items);
				final List<PolyglotPackage> fetched = new ArrayList<PolyglotPackage>(items.size());
				synchronized(PackageHandler.this.uncommitted)
				{
					final Iterator<Hash> toFetchIterator = toFetch.iterator();
					while(toFetchIterator.hasNext())
					{
						final Hash item = toFetchIterator.next();
						final PolyglotPackage cachedPackage = PackageHandler.this.uncommitted.get(item);
						if (cachedPackage == null)
							continue;
						
						fetched.add(cachedPackage); 
						toFetch.remove(item);
					}
				}
				PackageHandler.this.context.getLedger().getLedgerStore().get(toFetch, PolyglotPackage.class, (h, p) -> { fetched.add(p); toFetch.remove(h); });
				
				if (toFetch.isEmpty() == false)
					toFetch.forEach(h -> packageLog.error(PackageHandler.this.context.getName()+": Requested polyglot package "+h+" not found"));
				
				return fetched;
			}
		});

		
		this.context.getNetwork().getMessaging().register(GetPackageMessage.class, this.getClass(), new MessageProcessor<GetPackageMessage>()
		{
			@Override
			public void process(final GetPackageMessage getPackageMessage, final AbstractConnection connection)
			{
				try
				{
					if (packageLog.hasLevel(Logging.DEBUG))
						packageLog.debug(PackageHandler.this.context.getName()+": Package request of "+getPackageMessage.getAddress()+" from "+connection);

					final int numShardGroups = PackageHandler.this.context.getLedger().numShardGroups();
					final ShardGroupID localShardGroupID = ShardMapper.toShardGroup(PackageHandler.this.context.getNode().getIdentity(), numShardGroups);
					final ShardGroupID packageShardGroupID = ShardMapper.toShardGroup(getPackageMessage.getAddress(), numShardGroups);
					
					if (localShardGroupID.equals(packageShardGroupID) == false)
					{
						packageLog.error(PackageHandler.this.context.getName()+": Request from "+connection+" for package "+getPackageMessage.getAddress()+" in shard group "+packageShardGroupID+" but local is "+localShardGroupID);
						// TODO disconnect and ban?  Asking for blueprints we don't have
						return;
					}
	
					// TODO write lock check
					final PolyglotPackage polyglotPackage;
					final PolyglotPackage cachedPackage = PackageHandler.this.cached.get(getPackageMessage.getAddress());
					if (cachedPackage == null)
					{
						polyglotPackage = PackageHandler.this.context.getLedger().getLedgerStore().get(getPackageMessage.getAddress().scope(), PolyglotPackage.class);
						if (polyglotPackage == null)
							throw new IOException("Polyglot package not found for "+getPackageMessage.getAddress());
					}
					else
						polyglotPackage = cachedPackage;
					
					final PackageMessage packageMessage = new PackageMessage(getPackageMessage.getAtom(), polyglotPackage);
					PackageHandler.this.context.getNetwork().getMessaging().send(packageMessage, connection);
				}
				catch (Exception ex)
				{
					packageLog.error(PackageHandler.this.context.getName()+": ledger.messages.package.get "+connection, ex);
				}
			}
		});
		
		this.context.getNetwork().getMessaging().register(PackageMessage.class, this.getClass(), new MessageProcessor<PackageMessage>()
		{
			@Override
			public void process(final PackageMessage packageMessage, final AbstractConnection connection)
			{
				try
				{
					if (PackageHandler.this.context.getNode().isSynced() == false)
						return;

					if (packageLog.hasLevel(Logging.DEBUG))
						packageLog.debug(PackageHandler.this.context.getName()+": Received package "+packageMessage.getPackage().getAddress()+" from "+connection);

					// TODO validation of package
					
					Collection<PendingAtom> pendingAtoms;
					PackageHandler.this.lock.writeLock().lock();
					try
					{
						pendingAtoms = PackageHandler.this.pending.removeAll(packageMessage.getPackage().getAddress());
					}
					finally
					{
						PackageHandler.this.lock.writeLock().unlock();
					}

					for (PendingAtom pendingAtom : pendingAtoms)
					{
						try
						{
							load(packageMessage.getPackage(), pendingAtom, true);
						} 
						catch (Exception ex) 
						{
							PackageHandler.this.context.getEvents().post(new AtomExceptionEvent(pendingAtom, ex));
						}
					}
				}
				catch (Exception ex)
				{
					packageLog.error(PackageHandler.this.context.getName()+": ledger.messages.package " + connection, ex);
				}
			}
		});
		
		// SYNC //
		this.context.getNetwork().getMessaging().register(SyncAcquiredMessage.class, this.getClass(), new MessageProcessor<SyncAcquiredMessage>()
		{
			@Override
			public void process(final SyncAcquiredMessage syncAcquiredMessage, final AbstractConnection connection)
			{
				if (ShardMapper.equal(PackageHandler.this.context.getLedger().numShardGroups(), connection.getNode().getIdentity(), PackageHandler.this.context.getNode().getIdentity()) == false)
				{
					packageLog.error(PackageHandler.this.context.getName()+": Received SyncAcquiredMessage from "+connection+" in shard group "+ShardMapper.toShardGroup(connection.getNode().getIdentity(), PackageHandler.this.context.getLedger().numShardGroups())+" but local is "+ShardMapper.toShardGroup(PackageHandler.this.context.getNode().getIdentity(), PackageHandler.this.context.getLedger().numShardGroups()));
					// Disconnect and ban?
					return;
				}

				try
				{
					if (packageLog.hasLevel(Logging.DEBUG))
						packageLog.debug(PackageHandler.this.context.getName()+": Package handler sync inventory request from "+connection);
					
					final Set<Hash> packageInventory = new LinkedHashSet<Hash>();
					final MutableLong syncInventoryHeight = new MutableLong(Math.max(1, syncAcquiredMessage.getHead().getHeight() - Constants.SYNC_INVENTORY_HEAD_OFFSET));
					while (syncInventoryHeight.getValue() <= PackageHandler.this.context.getLedger().getHead().getHeight())
					{
						final Epoch epoch = Epoch.from(syncInventoryHeight.getValue() / Constants.BLOCKS_PER_EPOCH);
						final int numShardGroups = PackageHandler.this.context.getLedger().numShardGroups(epoch);
						final ShardGroupID remoteShardGroupID = ShardMapper.toShardGroup(connection.getNode().getIdentity(), numShardGroups);

						PackageHandler.this.context.getLedger().getLedgerStore().getSyncInventory(syncInventoryHeight.getValue(), PolyglotPackage.class).forEach(ir -> {
							if (syncInventoryHeight.getValue() <= syncAcquiredMessage.getHead().getHeight())
								return;
							
							ShardGroupID pakageShardGroupID = ShardMapper.toShardGroup(ir.<PolyglotPackage>get().getAddress(), numShardGroups);
							if (pakageShardGroupID.equals(remoteShardGroupID))
								return;

							packageInventory.add(ir.getHash());
						});
						
						syncInventoryHeight.increment();
					}

					if (syncLog.hasLevel(Logging.DEBUG))
						syncLog.debug(PackageHandler.this.context.getName()+": Broadcasting packages "+packageInventory.size()+" / "+packageInventory+" to "+connection);
					else
						syncLog.log(PackageHandler.this.context.getName()+": Broadcasting "+packageInventory.size()+" packages to "+connection);

					while(packageInventory.isEmpty() == false)
					{
						InventoryMessage packageInventoryMessage = new InventoryMessage(packageInventory, 0, Math.min(Constants.MAX_BROADCAST_INVENTORY_ITEMS, packageInventory.size()), PolyglotPackage.class);
						PackageHandler.this.context.getNetwork().getMessaging().send(packageInventoryMessage, connection);
						packageInventory.removeAll(packageInventoryMessage.asInventory().stream().map(ii -> ii.getHash()).collect(Collectors.toList()));
					}
				}
				catch (Exception ex)
				{
					packageLog.error(PackageHandler.this.context.getName()+": ledger.messages.sync.acquired " + connection, ex);
				}
			}
		});


		this.context.getEvents().register(this.asyncPackageListener);
		this.context.getEvents().register(this.syncPackageListener);
		this.context.getEvents().register(this.syncChangeListener);
		this.context.getEvents().register(this.syncBlockListener);
	}

	@Override
	public void stop() throws TerminationException 
	{
		this.context.getEvents().unregister(this.asyncPackageListener);
		this.context.getEvents().unregister(this.syncPackageListener);
		this.context.getEvents().unregister(this.syncBlockListener);
		this.context.getEvents().unregister(this.syncChangeListener);
		this.context.getNetwork().getMessaging().deregisterAll(this.getClass());
	}
	
	void load(final StateAddress packageAddress, final PendingAtom pendingAtom)
	{
		Objects.requireNonNull(pendingAtom, "Pending atom is null");
		Objects.requireNonNull(pendingAtom, "Package address is null");
		
		if (packageLog.hasLevel(Logging.DEBUG))
			packageLog.debug(PackageHandler.this.context.getName()+": Loading package "+packageAddress+" for atom "+pendingAtom.getHash());

		PolyglotPackage polyglotPackage = null;
		PackageHandler.this.lock.writeLock().lock();
		try
		{
			final PolyglotPackage cachedPackage = PackageHandler.this.cached.get(packageAddress);
			if (cachedPackage == null)
			{
				polyglotPackage = PackageHandler.this.context.getLedger().getLedgerStore().get(packageAddress.scope(), PolyglotPackage.class);
				
				if (polyglotPackage != null)
					PackageHandler.this.cached.put(packageAddress, polyglotPackage);
			}
			else
				polyglotPackage = cachedPackage;

			if (polyglotPackage == null)
			{
				final Epoch epoch = PackageHandler.this.context.getLedger().getEpoch();
				final int numShardGroups = PackageHandler.this.context.getLedger().numShardGroups(epoch);
				final ShardGroupID localShardGroupID = ShardMapper.toShardGroup(PackageHandler.this.context.getNode().getIdentity(), numShardGroups);
				final ShardGroupID packageShardGroupID = ShardMapper.toShardGroup(packageAddress, numShardGroups);

				if (localShardGroupID.equals(packageShardGroupID))
					throw new DatabaseException("Package "+packageAddress+" is not found");
				
				if (PackageHandler.this.context.getLedger().isSynced())
				{
					if (PackageHandler.this.pending.containsEntry(packageAddress, pendingAtom) == false)
					{
						if (PackageHandler.this.pending.containsKey(packageAddress) == false)
						{
							StandardConnectionFilter connectionFilter = StandardConnectionFilter.build(PackageHandler.this.context).setStates(ConnectionState.CONNECTED).setShardGroupID(packageShardGroupID).setSynced(true);
							List<AbstractConnection> remoteConnections = PackageHandler.this.context.getNetwork().get(connectionFilter);
							if (remoteConnections.isEmpty())
								throw new IOException("No remote peers available to read package "+packageAddress+" for atom "+pendingAtom.getHash());
						       					
							GetPackageMessage getPackageMessage = new GetPackageMessage(pendingAtom.getHash(), packageAddress);
							PackageHandler.this.context.getNetwork().getMessaging().send(getPackageMessage, remoteConnections.get(0));
						}
										
						PackageHandler.this.pending.put(packageAddress, pendingAtom);
					}
				}
			}

			if (polyglotPackage != null)
				load(polyglotPackage, pendingAtom, false);
		} 
		catch (Exception ex) 
		{
			PackageHandler.this.context.getEvents().post(new AtomExceptionEvent(pendingAtom, ex));
		}
		finally
		{
			PackageHandler.this.lock.writeLock().unlock();
		}
	}
	
	// TODO this is actually nasty, but to refactor it out requires a lot of work due to the original
	// design choices made on event handling for pending atoms.
	//
	// If checkProvisioned is false, means this load is a synchronous load and do not need to fire the message.
	// if checkProvisioned is true, this was an async load (likely from a remote source) and the provisioned check 
	// for the atom must happen and the event fired as provisioned state also considers packages. 
	private void load(PolyglotPackage pakage, PendingAtom pendingAtom, boolean checkProvisioned)
	{
		boolean postProvisionedEvent = pendingAtom.isProvisioned() == false;

		pendingAtom.load(pakage);
		this.context.getEvents().post(new PackageLoadedEvent(pakage));
		
		if (checkProvisioned && pendingAtom.isProvisioned() && postProvisionedEvent)
			this.context.getEvents().post(new AtomProvisionedEvent(pendingAtom));
	}
	
	Collection<PolyglotPackage> get(final Collection<Hash> packages) throws IOException
	{
		return this.context.getLedger().getLedgerStore().get(packages, PolyglotPackage.class);
	}
	
	Collection<PolyglotPackage> uncommitted()
	{

        this.lock.readLock().lock();
		try
		{
            final List<PolyglotPackage> uncommitted = new ArrayList<>(this.uncommitted.values());
			return Collections.unmodifiableList(uncommitted);
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}
	
	PolyglotPackage uncommitted(final Hash pakage)
	{
		Objects.requireNonNull(pakage, "Package hash is null");
		Hash.notZero(pakage, "Package hash is ZERO");

		this.lock.readLock().lock();
		try
		{
			return this.uncommitted.get(pakage);
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}
	
	List<PolyglotPackage> uncommitted(final int limit, final Predicate<Hashable> excluder)
	{
		final List<PolyglotPackage> uncommittedPackages = new ArrayList<PolyglotPackage>();
		
		this.lock.readLock().lock();
		try
		{
			for (final PolyglotPackage cachedPackage : this.uncommitted.values())
			{
				if (excluder.test(cachedPackage))
					continue;

				uncommittedPackages.add(cachedPackage);
				if (uncommittedPackages.size() == limit)
					break;
			}
		}
		finally
		{
			this.lock.readLock().unlock();
		}
		
		return uncommittedPackages;
	}
	
	// SYNC BLOCK LISTENER //
	private SynchronousEventListener syncBlockListener = new SynchronousEventListener()
	{
		@Subscribe
		public void on(final BlockCommittedEvent blockCommittedEvent)
		{
			if (blockCommittedEvent.getPendingBlock().getHeader().getInventorySize(InventoryType.PACKAGES) == 0)
				return;
			
			final Epoch epoch = Epoch.from(blockCommittedEvent.getPendingBlock().getHeader());
			final int numShardGroups = PackageHandler.this.context.getLedger().numShardGroups(epoch);
			final ShardGroupID localShardGroupID = ShardMapper.toShardGroup(PackageHandler.this.context.getNode().getIdentity(), numShardGroups);

			PackageHandler.this.lock.writeLock().lock();
			try
			{
				for (final PolyglotPackage committedPackage : blockCommittedEvent.getPendingBlock().<PolyglotPackage>get(InventoryType.PACKAGES))
				{
					final PolyglotPackage cachedPackage = PackageHandler.this.uncommitted.remove(committedPackage.getHash());
					if (cachedPackage == null)
					{
						packageLog.error(PackageHandler.this.context.getName()+": Package "+committedPackage.getHash()+":"+committedPackage.getAddress()+" not found in UNCOMMITTED");
						continue;
					}
					
					if (committedPackage.getAddress().context().equals(Package.class.getAnnotation(StateContext.class).value()))
						context.getMetaData().increment("ledger.packages.blueprints");
					else if (committedPackage.getAddress().context().equals(Component.class.getAnnotation(StateContext.class).value()))
						context.getMetaData().increment("ledger.packages.components");
					
					final ShardGroupID packageShardGroupID = ShardMapper.toShardGroup(committedPackage.getAddress(), numShardGroups);
					if (packageShardGroupID.equals(localShardGroupID))
						PackageHandler.this.cached.put(committedPackage.getAddress(), committedPackage);

					if (packageLog.hasLevel(Logging.DEBUG))
						packageLog.info(PackageHandler.this.context.getName()+": Committed package "+committedPackage);
				}
			}
			finally
			{
				PackageHandler.this.lock.writeLock().unlock();
			}
		}
	};
	
	// SYNC PACKAGE LISTENER //
	private SynchronousEventListener syncPackageListener = new SynchronousEventListener()
	{
		@Subscribe
		public void on(final PackageLoadedEvent packageLoadedEvent) throws IOException
		{
			if (packageLoadedEvent.getPackage() instanceof PolyglotPackage)
			{
				final PolyglotPackage polyglotPackage = packageLoadedEvent.getPackage();
				PackageHandler.this.lock.writeLock().lock();
				try
				{
					final PolyglotPackage cachedPackage = PackageHandler.this.cached.get(polyglotPackage.getAddress());
					if (cachedPackage != null)
						return;

					PackageHandler.this.cached.put(polyglotPackage.getAddress(), polyglotPackage);
				}
				finally
				{
					PackageHandler.this.lock.writeLock().unlock();
				}
			}
		}
	};

	
	// ASYNC PACKAGE LISTENER //
	private EventListener asyncPackageListener = new EventListener()
	{
		@Subscribe
		public void on(final PackageLoadedEvent packageLoadedEvent) throws IOException
		{
			if (packageLoadedEvent.getPackage() instanceof PolyglotPackage)
			{
				final PolyglotPackage polyglotPackage = packageLoadedEvent.getPackage();
				PackageHandler.this.lock.writeLock().lock();
				try
				{
					if (PackageHandler.this.context.getLedger().getLedgerStore().has(polyglotPackage.getHash(), PolyglotPackage.class))
						return;
					
					if (PackageHandler.this.context.getLedger().getLedgerStore().store(polyglotPackage).equals(OperationStatus.SUCCESS))
					{
						final PolyglotPackage cachedPackage = PackageHandler.this.cached.get(polyglotPackage.getAddress());
						if (cachedPackage == null)
							throw new IllegalStateException("Expected polyglot package "+polyglotPackage.getAddress()+" in cache when tagging as uncommitted");

						PackageHandler.this.uncommitted.put(polyglotPackage.getHash(), cachedPackage);
					}
					else
						throw new DatabaseException("Failed to store "+polyglotPackage);
				}
				finally
				{
					PackageHandler.this.lock.writeLock().unlock();
				}
			}
		}
	};

	// SYNC CHANGE LISTENER //
	private SynchronousEventListener syncChangeListener = new SynchronousEventListener()
	{
		@Subscribe
		public void on(final SyncAcquiredEvent event)
		{
			PackageHandler.this.lock.writeLock().lock();
			try
			{
				packageLog.log(PackageHandler.this.context.getName()+": Sync status acquired, preparing package handler");
				PackageHandler.this.uncommitted.clear();
				PackageHandler.this.cached.clear();
				PackageHandler.this.pending.clear();
			}
			finally
			{
				PackageHandler.this.lock.writeLock().unlock();
			}
		}
		
		@Subscribe
		public void on(final SyncLostEvent event) 
		{
			PackageHandler.this.lock.writeLock().lock();
			try
			{
				packageLog.log(PackageHandler.this.context.getName()+": Sync status lost, flushing package handler");
				PackageHandler.this.uncommitted.clear();
				PackageHandler.this.cached.clear();
				PackageHandler.this.pending.clear();
			}
			finally
			{
				PackageHandler.this.lock.writeLock().unlock();
			}
		}
	};
}
