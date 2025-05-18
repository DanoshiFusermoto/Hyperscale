package org.radix.hyperscale.ledger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;

import org.eclipse.collections.api.factory.Sets;
import org.radix.hyperscale.Constants;
import org.radix.hyperscale.Context;
import org.radix.hyperscale.Service;
import org.radix.hyperscale.Universe;
import org.radix.hyperscale.collections.MappedBlockingQueue;
import org.radix.hyperscale.common.Primitive;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.crypto.Hashable;
import org.radix.hyperscale.crypto.Identity;
import org.radix.hyperscale.events.EventListener;
import org.radix.hyperscale.events.SyncLostEvent;
import org.radix.hyperscale.events.SynchronousEventListener;
import org.radix.hyperscale.exceptions.StartupException;
import org.radix.hyperscale.exceptions.TerminationException;
import org.radix.hyperscale.exceptions.ValidationException;
import org.radix.hyperscale.executors.LatchedProcessor;
import org.radix.hyperscale.ledger.AtomStatus.State;
import org.radix.hyperscale.ledger.BlockHeader.InventoryType;
import org.radix.hyperscale.ledger.Substate.NativeField;
import org.radix.hyperscale.ledger.events.AtomAcceptableEvent;
import org.radix.hyperscale.ledger.events.AtomAcceptedEvent;
import org.radix.hyperscale.ledger.events.AtomAcceptedTimeoutEvent;
import org.radix.hyperscale.ledger.events.AtomCertificateEvent;
import org.radix.hyperscale.ledger.events.AtomCommitEvent;
import org.radix.hyperscale.ledger.events.AtomCommitTimeoutEvent;
import org.radix.hyperscale.ledger.events.AtomExceptionEvent;
import org.radix.hyperscale.ledger.events.AtomExecutableEvent;
import org.radix.hyperscale.ledger.events.AtomExecuteLatentEvent;
import org.radix.hyperscale.ledger.events.AtomExecutedEvent;
import org.radix.hyperscale.ledger.events.AtomExecutionTimeoutEvent;
import org.radix.hyperscale.ledger.events.AtomPrepareTimeoutEvent;
import org.radix.hyperscale.ledger.events.AtomPreparedEvent;
import org.radix.hyperscale.ledger.events.AtomProvisionedEvent;
import org.radix.hyperscale.ledger.events.AtomTimeoutEvent;
import org.radix.hyperscale.ledger.events.BlockCommittedEvent;
import org.radix.hyperscale.ledger.events.SyncAcquiredEvent;
import org.radix.hyperscale.ledger.exceptions.SyncStatusException;
import org.radix.hyperscale.ledger.messages.SubmitAtomsMessage;
import org.radix.hyperscale.ledger.messages.SyncAcquiredMessage;
import org.radix.hyperscale.ledger.messages.SyncAcquiredMessageProcessor;
import org.radix.hyperscale.ledger.primitives.Atom;
import org.radix.hyperscale.ledger.primitives.AtomCertificate;
import org.radix.hyperscale.ledger.primitives.StateOutput;
import org.radix.hyperscale.ledger.sme.ManifestException;
import org.radix.hyperscale.ledger.timeouts.AcceptTimeout;
import org.radix.hyperscale.ledger.timeouts.AtomTimeout;
import org.radix.hyperscale.ledger.timeouts.CommitTimeout;
import org.radix.hyperscale.ledger.timeouts.ExecutionLatentTimeout;
import org.radix.hyperscale.ledger.timeouts.ExecutionTimeout;
import org.radix.hyperscale.ledger.timeouts.PrepareTimeout;
import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;
import org.radix.hyperscale.network.AbstractConnection;
import org.radix.hyperscale.network.GossipFetcher;
import org.radix.hyperscale.network.GossipFilter;
import org.radix.hyperscale.network.GossipInventory;
import org.radix.hyperscale.network.GossipReceiver;
import org.radix.hyperscale.network.MessageProcessor;

import com.google.common.eventbus.Subscribe;
import com.sleepycat.je.OperationStatus;

public class AtomHandler implements Service, LedgerInterface
{
	private static final Logger syncLog = Logging.getLogger("sync");
	private static final Logger atomsLog = Logging.getLogger("atoms");
	
	private static final int ATOMS_PROCESS_BATCH_SIZE = 64;
	
	private final Context context;
	
	private final Map<Hash, PendingAtom> pendingAtoms = new ConcurrentHashMap<Hash, PendingAtom>(1<<10);

	private final Map<Hash, Long> latent = Collections.synchronizedMap(new LinkedHashMap<>(1024));
	private final Map<Hash, Long> unaccepted = Collections.synchronizedMap(new LinkedHashMap<>(1024));
	private final Map<Hash, PendingAtom> acceptable = Collections.synchronizedMap(new LinkedHashMap<>(1024));
	private final Map<Hash, PendingAtom> certificates = Collections.synchronizedMap(new LinkedHashMap<>(1024));
	private final Map<Hash, PendingAtom> executable = Collections.synchronizedMap(new LinkedHashMap<>(1024));
	private final Map<Hash, PendingAtom> unexecuted = Collections.synchronizedMap(new LinkedHashMap<>(1024));
	private final Map<Hash, PendingAtom> uncommitted = Collections.synchronizedMap(new LinkedHashMap<>(1024));

	private final MappedBlockingQueue<Hash, Atom> atomsToProvisionQueue;
	private final MappedBlockingQueue<Hash, PendingAtom> atomsToPrepareQueue;
	
	private final Lock[] loadlocks;
	
	private LatchedProcessor atomProcessor = new LatchedProcessor(1, TimeUnit.SECONDS)
	{
		private long nextTimeoutPoll = 0l;
		
		@Override
		public void process()
		{
			if (AtomHandler.this.context.getNode().isSynced() == false)
				return;
						
			final Epoch epoch = AtomHandler.this.context.getLedger().getEpoch();
			final int numShardGroups = AtomHandler.this.context.getLedger().numShardGroups();

			_provisionAtoms(epoch, numShardGroups);
			_prepareAtoms(epoch, numShardGroups);
			
			// Poll for timeouts per second
			if (System.currentTimeMillis() >= this.nextTimeoutPoll)
			{
				_tryScheduledTimeouts();
				this.nextTimeoutPoll = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(1);
			}
		}

		@Override
		public void onError(Throwable thrown) 
		{
			atomsLog.fatal(AtomHandler.this.context.getName()+": Error processing atom queue", thrown);
		}
		
		@Override
		public void onTerminated()
		{
			atomsLog.log(AtomHandler.this.context.getName()+": Atom processor is terminated");
		}
	};
	
	AtomHandler(final Context context)
	{
		this.context = Objects.requireNonNull(context);
		this.atomsToPrepareQueue = new MappedBlockingQueue<Hash, PendingAtom>(this.context.getConfiguration().get("ledger.atom.queue", 1<<14));
		this.atomsToProvisionQueue = new MappedBlockingQueue<Hash, Atom>(this.context.getConfiguration().get("ledger.atom.queue", 1<<14));
		this.loadlocks = new Lock[this.context.getConfiguration().get("ledger.atom.handler.loadlocks", 1<<10)];
		for(int i = 0 ; i < loadlocks.length ; i++)
			this.loadlocks[i] = new ReentrantLock();
	}

	@Override
	public void start() throws StartupException 
	{
		// ATOMS //
		this.context.getNetwork().getGossipHandler().register(Atom.class, new GossipFilter<Atom>(this.context) 
		{
			@Override
			public Set<ShardGroupID> filter(final Atom atom)
			{
				final PendingAtom pendingAtom = AtomHandler.this.pendingAtoms.get(atom.getHash());
				if (pendingAtom == null)
					throw new IllegalStateException("Expected pending atom "+atom.getHash()+" to exist for broadcast filter");
				
				return ShardMapper.toShardGroups(pendingAtom, StateLockMode.WRITE, AtomHandler.this.context.getLedger().numShardGroups());
			}
		});
		
		this.context.getNetwork().getGossipHandler().register(Atom.class, new GossipInventory() 
		{
			@Override
			public Collection<Hash> required(final Class<? extends Primitive> type, final Collection<Hash> items, final AbstractConnection connection) throws IOException
			{
				if (type.equals(Atom.class) == false)
				{
					atomsLog.error(AtomHandler.this.context.getName()+": Atom type expected but got "+type);
					return Collections.emptyList();
				}
				
				final List<Hash> required = new ArrayList<Hash>(items.size());
				required.removeAll(AtomHandler.this.atomsToProvisionQueue.contains(required));

				final Iterator<Hash> itemsIterator = items.iterator();
				while(itemsIterator.hasNext())
				{
					final Hash item = itemsIterator.next();
					if (AtomHandler.this.atomsToPrepareQueue.contains(item))
						continue;
					
					final PendingAtom pendingAtom = AtomHandler.this.pendingAtoms.get(item);
					if (pendingAtom != null && pendingAtom.getAtom() != null)
						continue;
					
					if (AtomHandler.this.context.getLedger().getLedgerStore().has(item, type))
						continue;

					if (AtomHandler.this.status(item).equals(State.NONE) == false)
						continue;
					
					required.add(item);
				}
				
				return required;
			}
		});

		this.context.getNetwork().getGossipHandler().register(Atom.class, new GossipReceiver<Atom>() 
		{
			@Override
			public void receive(final Collection<Atom> atoms, final AbstractConnection connection) throws InterruptedException
			{
				if (atomsLog.hasLevel(Logging.INFO))
					atoms.forEach(a -> atomsLog.log(AtomHandler.this.context.getName()+": Received atom "+a.getHash()));

				Collection<Atom> submitted = AtomHandler.this.submit(atoms, true);
				if (submitted.size() != atoms.size())
					atomsLog.warn(AtomHandler.this.context.getName()+": Expected to submit "+atoms.size()+" atoms but only submitted "+submitted.size());
			}
		});

		this.context.getNetwork().getGossipHandler().register(Atom.class, new GossipFetcher<Atom>() 
		{
			@Override
			public Collection<Atom> fetch(final Collection<Hash> items, final AbstractConnection connection) throws IOException
			{
				List<Hash> toFetch = new ArrayList<Hash>(items);
				List<Atom> fetched = new ArrayList<Atom>();

				AtomHandler.this.atomsToProvisionQueue.getAll(toFetch, (h, p) -> { fetched.add(p); toFetch.remove(h); });

				Iterator<Hash> toFetchIterator = toFetch.iterator();
				while(toFetchIterator.hasNext())
				{
					Hash item = toFetchIterator.next();
					PendingAtom pendingAtom = AtomHandler.this.atomsToPrepareQueue.get(item);
					if (pendingAtom == null || pendingAtom.getAtom() == null)
					{
						pendingAtom = AtomHandler.this.pendingAtoms.get(item);
						if (pendingAtom == null || pendingAtom.getAtom() == null)
							continue;
					}
					
					fetched.add(pendingAtom.getAtom());
					toFetchIterator.remove();
				}
				
				AtomHandler.this.context.getLedger().getLedgerStore().get(toFetch, Atom.class, (h, p) -> { fetched.add(p); toFetch.remove(h); });

				if (toFetch.isEmpty() == false)
					toFetch.forEach(h -> atomsLog.error(AtomHandler.this.context.getName()+": Requested atom "+h+" not found"));
				
				return fetched;
			}
		});
				
		this.context.getNetwork().getMessaging().register(SyncAcquiredMessage.class, this.getClass(), new SyncAcquiredMessageProcessor(this.context)
		{
			@Override
			public void process(final SyncAcquiredMessage syncAcquiredMessage, final AbstractConnection connection)
			{
				try
				{
					if (atomsLog.hasLevel(Logging.DEBUG))
						atomsLog.debug(AtomHandler.this.context.getName()+": Atom pool inventory request from "+connection);

					// TODO will cause problems when pool is very big
					final Set<Hash> deferredPendingAtomInventory = new LinkedHashSet<Hash>();
					AtomHandler.this.pendingAtoms.forEach((hash, pendingAtom) -> {
						if (pendingAtom.getStatus().before(AtomStatus.State.PREPARED))
							return;
						
						if (syncAcquiredMessage.containsPending(pendingAtom.getHash()) == true)
							return;

						deferredPendingAtomInventory.add(pendingAtom.getHash());
					});

					int broadcastedPendingAtomCount = 0;
					final Set<Hash> pendingAtomInventory = new LinkedHashSet<Hash>();
					long syncInventoryHeight = Math.max(1, syncAcquiredMessage.getHead().getHeight() - Constants.SYNC_INVENTORY_HEAD_OFFSET);
					while (syncInventoryHeight <= AtomHandler.this.context.getLedger().getHead().getHeight())
					{
						AtomHandler.this.context.getLedger().getLedgerStore().getSyncInventory(syncInventoryHeight, Atom.class).forEach(ir -> {
							if (ir.getClockEnd() <= syncAcquiredMessage.getHead().getHeight())
								return;

							if (syncAcquiredMessage.containsPending(ir.getHash()) == true)
								return;
							
							pendingAtomInventory.add(ir.getHash());
							deferredPendingAtomInventory.remove(ir.getHash());
						});
						
						broadcastedPendingAtomCount += broadcastSyncInventory(pendingAtomInventory, Atom.class, Constants.MAX_REQUEST_INVENTORY_ITEMS, connection);

						syncInventoryHeight++;
					}
					
					// Send any remaining (also now send the deferred)
					broadcastedPendingAtomCount += broadcastSyncInventory(pendingAtomInventory, Atom.class, 0, connection);
					broadcastedPendingAtomCount += broadcastSyncInventory(deferredPendingAtomInventory, Atom.class, 0, connection);

					syncLog.log(AtomHandler.this.context.getName()+": Broadcasted "+broadcastedPendingAtomCount+" atom to "+connection);
				}
				catch (Exception ex)
				{
					atomsLog.error(AtomHandler.this.context.getName()+": ledger.messages.atom.get.pool " + connection, ex);
				}
			}
		});
		
		this.context.getNetwork().getMessaging().register(SubmitAtomsMessage.class, this.getClass(), new MessageProcessor<SubmitAtomsMessage>()
		{
			@Override
			public void process(final SubmitAtomsMessage submitAtomsMessage, final AbstractConnection connection)
			{
				try
				{
					if (atomsLog.hasLevel(Logging.DEBUG))
						atomsLog.debug(AtomHandler.this.context.getName()+": Received "+submitAtomsMessage.getAtoms().size()+" atoms for submission from "+connection);

					if (AtomHandler.this.context.getNode().isSynced() == false)
						return;
					
					submit(submitAtomsMessage.getAtoms(), false);
					
//					final Collection<Hash> inventory = submitAtomsMessage.getAtoms().stream().map(a -> a.getHash()).collect(Collectors.toSet());
//					final Collection<Hash> required = AtomHandler.this.context.getNetwork().getGossipHandler().required(inventory, Atom.class, connection);
//					if (required.isEmpty())
//						return;
					
//					submit(submitAtomsMessage.getAtoms().stream().filter(a -> required.contains(a.getHash())).collect(Collectors.toList()), false);
				}
				catch (Exception ex)
				{
					atomsLog.error(AtomHandler.this.context.getName()+": ledger.messages.submit.atom " + connection, ex);
				}
			}
		});

		this.context.getEvents().register(this.syncChangeListener);
		this.context.getEvents().register(this.syncAtomListener);
		this.context.getEvents().register(this.asyncAtomListener);
		this.context.getEvents().register(this.syncBlockListener);

		Thread atomProcessorThread = new Thread(this.atomProcessor);
		atomProcessorThread.setDaemon(true);
		atomProcessorThread.setName(this.context.getName()+" Atom Processor");
		atomProcessorThread.start();
	}

	@Override
	public void stop() throws TerminationException 
	{
		this.atomProcessor.terminate(true);

		this.context.getEvents().unregister(this.syncBlockListener);
		this.context.getEvents().unregister(this.asyncAtomListener);
		this.context.getEvents().unregister(this.syncAtomListener);
		this.context.getEvents().unregister(this.syncChangeListener);
		this.context.getNetwork().getMessaging().deregisterAll(this.getClass());
	}
	
	// PROCESSING PIPELINE //
	private void _provisionAtoms(final Epoch epoch, final int numShardGroups)
	{
		if (this.atomsToProvisionQueue.isEmpty() == false)
		{
			final List<Atom> atomsToProvision = this.atomsToProvisionQueue.getMany(AtomHandler.ATOMS_PROCESS_BATCH_SIZE);
			for (int i = 0 ; i < atomsToProvision.size() ; i++)
			{
				final Atom atom = atomsToProvision.get(i);
				
				if (atomsLog.hasLevel(Logging.DEBUG))
					atomsLog.debug(this.context.getName()+": Provisioning atom "+atom.getHash());
	
				PendingAtom pendingAtom = null;
				try
				{
					pendingAtom = get(atom.getHash());
					if (pendingAtom == null)
					{
						atomsLog.error(this.context.getName()+": Atom "+atom.getHash()+" could not be initialized");
						continue;
					}
					
					if (pendingAtom.getStatus().before(AtomStatus.State.PREPARED) == false)
					{
						atomsLog.warn(this.context.getName()+": Atom "+atom.getHash()+" is already loaded & prepared with status "+pendingAtom.getStatus());
						continue;
					}

					pendingAtom.setAtom(atom);
				
					if (atom.getHash().leadingZeroBits() < Universe.get().getPrimitivePOW() && atom.hasAuthority(Universe.get().getCreator()) == false)
						throw new ValidationException("Atom POW of "+atom.getHash().leadingZeroBits()+" does not meet leading bits requirement of "+Universe.get().getPrimitivePOW());
					
					if (atom.verify() == false)
						throw new ValidationException("Atom failed signature verification");

					if (this.context.getLedger().getLedgerStore().store(atom).equals(OperationStatus.SUCCESS))
						this.atomsToPrepareQueue.put(pendingAtom.getHash(), pendingAtom);
					else
						atomsLog.warn(this.context.getName()+": Failed to store atom "+pendingAtom.getHash());
				}
				catch (Exception ex)
				{
					atomsLog.error(this.context.getName()+": Error provisioning atom for " + atom.getHash(), ex);
					
					if (pendingAtom != null)
						this.context.getEvents().post(new AtomExceptionEvent(pendingAtom, ex));
				}
				finally
				{
					if (this.atomsToProvisionQueue.remove(atom.getHash()) == null)
						atomsLog.error(this.context.getName()+": Atom "+atom.getHash()+" not removed from provisioning queue");
				}
			}
			
			if (this.atomsToProvisionQueue.isEmpty() == false)
				this.atomProcessor.signal();
		}
	}
	
	private void _prepareAtoms(final Epoch epoch, final int numShardGroups)
	{
		if (this.atomsToPrepareQueue.isEmpty() == false)
		{
			final ShardGroupID localShardGroupID = ShardMapper.toShardGroup(this.context.getNode().getIdentity(), numShardGroups);
			final List<PendingAtom> atomsToPrepare = this.atomsToPrepareQueue.getMany(AtomHandler.ATOMS_PROCESS_BATCH_SIZE);
			for (int i = 0 ; i < atomsToPrepare.size() ; i++)
			{
				final PendingAtom pendingAtom = atomsToPrepare.get(i);
				try
				{
					if (atomsLog.hasLevel(Logging.DEBUG))
						atomsLog.debug(this.context.getName()+": Validating and preparing atom "+pendingAtom.getHash());
	
					if (pendingAtom.isForcePrepareTimeout())
						continue;
						
					pendingAtom.getAtom().validate();
						
					if (atomsLog.hasLevel(Logging.DEBUG))
						atomsLog.debug(this.context.getName()+": Preparing atom "+pendingAtom.getHash());
						
					pendingAtom.prepare();
						
					final Set<ShardGroupID> shardGroupIDs = ShardMapper.toShardGroups(pendingAtom, StateLockMode.WRITE, numShardGroups);
					if (this.context.getNetwork().getGossipHandler().broadcast(pendingAtom.getAtom(), shardGroupIDs) == false)
						atomsLog.warn(this.context.getName()+": Failed to broadcast atom "+pendingAtom.getHash()+" to shard groups "+shardGroupIDs);
	
					if (shardGroupIDs.contains(localShardGroupID) == false)
					{
						if (atomsLog.hasLevel(Logging.DEBUG))
							atomsLog.warn(this.context.getName()+": Received atom "+pendingAtom.getHash()+" NOT for local shard group");
						
						this.pendingAtoms.remove(pendingAtom.getHash(), pendingAtom);
					}
					else 
					{
						if (atomsLog.hasLevel(Logging.DEBUG))
							atomsLog.debug(this.context.getName()+": Received atom "+pendingAtom.getHash()+" for local shard group");
						
						this.context.getEvents().post(new AtomPreparedEvent(pendingAtom));
					}
				}
				catch (Exception ex)
				{
					atomsLog.error(this.context.getName()+": Error preparing atom for "+pendingAtom.getHash(), ex);
					
					if (pendingAtom != null)
						this.context.getEvents().post(new AtomExceptionEvent(pendingAtom, ex));
				}
				finally
				{
					if (this.atomsToPrepareQueue.remove(pendingAtom.getHash()) == null)
						atomsLog.error(this.context.getName()+": Atom "+pendingAtom.getHash()+" not removed from prepare queue");
				}
			}

			if (this.atomsToPrepareQueue.isEmpty() == false)
				this.atomProcessor.signal();
		}
	}

	private void _tryScheduledTimeouts()
	{
		final long timestamp = System.currentTimeMillis();

		for (final PendingAtom pendingAtom : this.pendingAtoms.values())
		{
			if (pendingAtom.isCompleted())
				continue;

			try
			{
				final AtomTimeout timeout = pendingAtom.tryTimeout(timestamp);
				if (timeout != null)
				{
//					if(atomsLog.hasLevel(Logging.DEBUG))
						atomsLog.info(this.context.getName()+": Timeout "+timeout.getHash()+" "+timeout.getClass().getSimpleName()+" is triggered at "+timestamp+" for "+pendingAtom.getHash()+" with status "+pendingAtom.getStatus().current());
	
					AtomHandler.this.context.getEvents().post(new AtomTimeoutEvent(pendingAtom, timeout));
				}
			}
			catch (Exception ex)
			{
				atomsLog.error(this.context.getName()+": Error processing atom timeout for "+pendingAtom.getHash(), ex);
			}
		}
	}

	// METHODS //
	public int size() 
	{
		return this.pendingAtoms.size();
	}

	public Collection<PendingAtom> unaccepted()
	{
		final List<PendingAtom> unaccepted = new ArrayList<PendingAtom>(this.unaccepted.size());
		
		synchronized(this.unaccepted)
		{
			this.unaccepted.keySet().forEach(h -> {
				final PendingAtom pendingAtom = this.pendingAtoms.get(h);
				if (pendingAtom == null)
					return;
				
				unaccepted.add(pendingAtom);
			});
		}
		
		return unaccepted;
	}

	public Collection<PendingAtom> latent()
	{
		final List<PendingAtom> latent = new ArrayList<PendingAtom>(this.latent.size());

		synchronized(this.latent)
		{
			this.latent.keySet().forEach(h -> {
				final PendingAtom pendingAtom = this.pendingAtoms.get(h);
				if (pendingAtom == null)
					return;
				
				latent.add(pendingAtom);
			});
		}
		
		return latent;
	}

	public Collection<PendingAtom> executable()
	{
		synchronized(this.executable)
		{
			return Sets.immutable.<PendingAtom>ofAll(this.executable.values()).castToSet();
		}
	}

	public Collection<PendingAtom> unexecuted()
	{
		synchronized(this.unexecuted)
		{
			return Sets.immutable.<PendingAtom>ofAll(this.unexecuted.values()).castToSet();
		}
	}
	
	public Collection<PendingAtom> uncommitted()
	{
		synchronized(this.uncommitted)
		{
			return Sets.immutable.<PendingAtom>ofAll(this.uncommitted.values()).castToSet();
		}
	}
	
	public List<PendingAtom> acceptable(final int limit, final Predicate<Hashable> excluder)
	{
		final List<PendingAtom> acceptable;
		final List<PendingAtom> relocate;
		
		synchronized(this.acceptable)
		{
			acceptable = new ArrayList<PendingAtom>(this.acceptable.size() < limit ? this.acceptable.size() : limit);
			relocate = new ArrayList<PendingAtom>(this.acceptable.size() < limit ? this.acceptable.size() : limit);

			for (final PendingAtom pendingAtom : this.acceptable.values())
			{
				if (pendingAtom.isAccepted())
					continue;
				
				if (excluder.test(pendingAtom))
				{
					relocate.add(pendingAtom);
					continue;
				}
				
				acceptable.add(pendingAtom);
				if (acceptable.size() == limit)
					break;
			}
			
			// Move selection to tail
			if (1==0)
			{
				for (int i = 0 ; i < relocate.size() ; i++)
				{
					final PendingAtom pendingAtom = relocate.get(i);
					if (this.acceptable.remove(pendingAtom.getHash(), pendingAtom))
						this.acceptable.put(pendingAtom.getHash(), pendingAtom);
				}
			}
		}
		
		// Oldest first
		acceptable.sort((pa1, pa2) -> {
			long compare = (pa1.getAcceptableAt() - pa2.getAcceptableAt());
			if (compare != 0)
				return compare < 0 ? -1 : 1;

			return pa1.getHash().compareTo(pa2.getHash());
		});
		
		return acceptable;
	}


	public List<PendingAtom> unaccepted(final int limit, final Predicate<Hashable> excluder)
	{
		final List<PendingAtom> unaccepted;
		
		synchronized(this.unaccepted)
		{
			unaccepted = new ArrayList<PendingAtom>(this.unaccepted.size() < limit ? this.unaccepted.size() : limit);

			for (final Hash atom : this.unaccepted.keySet())
			{
				final PendingAtom pendingAtom = this.pendingAtoms.get(atom);
				if (pendingAtom == null)
					continue;
				
				final AtomTimeout timeout = pendingAtom.getTimeout(AcceptTimeout.class);
				if (timeout == null)
					// TODO Warn or throw?
					continue;

				if (timeout.isActive() == false)
					continue;
				
				if (pendingAtom.isAccepted())
					continue;

				if (excluder.test(pendingAtom))
					continue;
				
				unaccepted.add(pendingAtom);
				if (unaccepted.size() == limit)
					break;
			}
		}
		
		return unaccepted;
	}

	public List<PendingAtom> executable(final int limit, final Predicate<Hashable> excluder)
	{
		final List<PendingAtom> executable;
		final List<PendingAtom> relocate;
		
		synchronized(this.executable)
		{
			executable = new ArrayList<PendingAtom>(this.executable.size() < limit ? this.executable.size() : limit);
			relocate = new ArrayList<PendingAtom>(this.executable.size() < limit ? this.executable.size() : limit);

			for (final PendingAtom pendingAtom : this.executable.values())
			{
				if (pendingAtom.isExecuteSignalled() || pendingAtom.isExecuted())
					continue;
				
				if (pendingAtom.isForceLatentExecution() && pendingAtom.isExecuteLatentSignalled() == false)
					continue;

				if (excluder.test(pendingAtom))
				{
					relocate.add(pendingAtom);
					continue;
				}
				
				executable.add(pendingAtom);
				if (executable.size() == limit)
					break;
			}
			
			// Move selection to tail
			if (1==0)
			{
				for (int i = 0 ; i < relocate.size() ; i++)
				{
					final PendingAtom pendingAtom = relocate.get(i);
					if (this.executable.remove(pendingAtom.getHash(), pendingAtom))
						this.executable.put(pendingAtom.getHash(), pendingAtom);
				}
			}
		}
		
		return executable;
	}
	
	public List<PendingAtom> latent(final int limit, final Predicate<Hashable> excluder)
	{
		final List<PendingAtom> latent;
		
		synchronized(this.latent)
		{
			latent = new ArrayList<PendingAtom>(this.latent.size() < limit ? this.latent.size() : limit);

			for (final Hash atom : this.latent.keySet())
			{
				final PendingAtom pendingAtom = this.pendingAtoms.get(atom);
				if (pendingAtom == null)
					// TODO need to clean up on this or throw?
					continue;

				if (pendingAtom.isExecuteLatentSignalled())
					continue;

				if (pendingAtom.isExecuteSignalled())
					continue;

				if (pendingAtom.isExecuted())
					continue;

				if (excluder.test(pendingAtom))
					continue;
				
				latent.add(pendingAtom);
				if (latent.size() == limit)
					break;
			}
		}

		return latent;
	}

	/**
	 * Selects an {@link AtomCertificate} set up to a maximum limit and obeying the provided exclusions.
	 * <br><br>
	 * The exclusion collection may contain both AtomCertificate AND Atom hashes 
	 * 
	 * @param limit
	 * @param exclusions
	 * @return
	 */
	public List<PendingAtom> completed(final int limit, final Predicate<Hashable> excluder)
	{
		final List<PendingAtom> completed;
		final List<PendingAtom> relocate;
		
		synchronized(this.certificates)
		{
			completed = new ArrayList<PendingAtom>(this.certificates.size() < limit ? this.certificates.size() : limit);
			relocate = new ArrayList<PendingAtom>(this.certificates.size() < limit ? this.certificates.size() : limit);

			for (final PendingAtom pendingAtom : this.certificates.values())
			{
				final AtomCertificate certificate = pendingAtom.getCertificate();
				if (certificate == null)
					// TODO Warn or throw?
					continue;
				
				if (pendingAtom.isCompleted())
					continue;
				
				if (excluder.test(certificate) || excluder.test(pendingAtom))
				{
					relocate.add(pendingAtom);
					continue;
				}

				completed.add(pendingAtom);
				if (completed.size() == limit)
					break;
			}
			
			// Move selection to tail
			if (1==0)
			{
				for (int i = 0 ; i < relocate.size() ; i++)
				{
					final PendingAtom pendingAtom = relocate.get(i);
					if (this.certificates.remove(pendingAtom.getHash(), pendingAtom))
						this.certificates.put(pendingAtom.getHash(), pendingAtom);
				}
			}
		}
		
		return completed;
	}

	public List<PendingAtom> unexecuted(final int limit, final Predicate<Hashable> excluder)
	{
		final List<PendingAtom> unexecuted;

		synchronized(this.unexecuted)
		{
			unexecuted = new ArrayList<PendingAtom>(this.unexecuted.size() < limit ? this.unexecuted.size() : limit);

			for (final PendingAtom pendingAtom : this.unexecuted.values())
			{
				final AtomTimeout timeout = pendingAtom.getTimeout(ExecutionTimeout.class);
				if (timeout == null)
					// TODO Warn or throw?
					continue;

				if (timeout.isActive() == false)
					continue;

				if (pendingAtom.isExecuted())
					continue;

				if (pendingAtom.isCompleted())
					continue;

				if (excluder.test(timeout) || excluder.test(pendingAtom))
					continue;
						
				unexecuted.add(pendingAtom);
				if (unexecuted.size() == limit)
					break;
			}
		}
			
		return unexecuted;
	}

	public List<PendingAtom> uncommitted(final int limit, final Predicate<Hashable> excluder)
	{
		final List<PendingAtom> uncommitted;
		
		synchronized(this.uncommitted)
		{
			uncommitted = new ArrayList<PendingAtom>(this.uncommitted.size() < limit ? this.uncommitted.size() : limit);

			for (final PendingAtom pendingAtom : this.uncommitted.values())
			{
				final AtomTimeout timeout = pendingAtom.getTimeout(CommitTimeout.class);
				if (timeout == null)
					// TODO Warn or throw?
					continue;
				
				if (timeout.isActive() == false)
					continue;
				
				if (pendingAtom.isCompleted())
					continue;
				
				if (excluder.test(timeout) || excluder.test(pendingAtom))
					continue;

				uncommitted.add(pendingAtom);
				if (uncommitted.size() == limit)
					break;
			}
		}
			
		return uncommitted;
	}

	/**
	 * Returns all pending atoms.
	 * 
	 * @throws  
	 */
	public Collection<PendingAtom> getAll()
	{
		// Should be safe to just collect as pendingAtoms is a ConcurrentMap
		return new ArrayList<PendingAtom>(this.pendingAtoms.values());
	}
	
	/**
	 * Returns a collection of existing pending atoms or creates them providing that they are not timed out or committed.
	 * 
	 * @param atoms The atom hashes
	 * @throws IOException
	 * @throws  
	 */
	Collection<PendingAtom> get(final Collection<Hash> atoms) throws IOException
	{
		final Set<PendingAtom> pendingAtoms = new HashSet<PendingAtom>();
		for (final Hash hash : atoms)
		{
			final PendingAtom pendingAtom = get(hash);
			if (pendingAtom == null)
			{
				atomsLog.warn(this.context.getName()+": Duplicate request for pending atom "+hash);
				continue;
			}
			
			pendingAtoms.add(pendingAtom);
		}
		
		return pendingAtoms;
	}

	PendingAtom certificate(final Hash certificate)
	{
		Objects.requireNonNull(certificate, "Atom certificate hash is null");
		Hash.notZero(certificate, "Atom certificate hash is ZERO");
		
		return this.certificates.get(certificate);
	}
	
	Collection<PendingAtom> certificate(final Collection<Hash> certificates)
	{
		final Set<PendingAtom> pendingAtoms = new HashSet<PendingAtom>();
		for (final Hash hash : certificates)
		{
			final PendingAtom pendingAtom = this.certificates.get(hash);
			if (pendingAtom == null)
				continue;
			
			pendingAtoms.add(pendingAtom);
		}
		
		return pendingAtoms;
	}

	PendingAtom unexecuted(final Hash timeout)
	{
		Objects.requireNonNull(timeout, "Atom timeout hash is null");
		Hash.notZero(timeout, "Atom timeout hash is ZERO");
		
		return this.unexecuted.get(timeout);
	}
	
	Collection<PendingAtom> unexecuted(final Collection<Hash> timeouts)
	{
		final Set<PendingAtom> pendingAtoms = new HashSet<PendingAtom>();
		for (final Hash hash : timeouts)
		{
			final PendingAtom pendingAtom = this.unexecuted.get(hash);
			if (pendingAtom == null)
				continue;
			
			pendingAtoms.add(pendingAtom);
		}
		
		return pendingAtoms;
	}

	PendingAtom uncommitted(final Hash timeout)
	{
		Objects.requireNonNull(timeout, "Atom timeout hash is null");
		Hash.notZero(timeout, "Atom timeout hash is ZERO");
		
		return this.uncommitted.get(timeout);
	}
	
	Collection<PendingAtom> uncommitted(final Collection<Hash> timeouts)
	{
		final Set<PendingAtom> pendingAtoms = new HashSet<PendingAtom>();
		for (final Hash hash : timeouts)
		{
			final PendingAtom pendingAtom = this.uncommitted.get(hash);
			if (pendingAtom == null)
				continue;
			
			pendingAtoms.add(pendingAtom);
		}
		
		return pendingAtoms;
	}

	boolean has(final Hash atom)
	{
		Objects.requireNonNull(atom, "Atom hash is null");
		Hash.notZero(atom, "Atom hash is zero");

		// TODO dont think this is needed now
		if (this.context.getLedger().isSynced() == false)
			throw new SyncStatusException("Ledger sync state is false!  AtomHandler::has called");

		return this.pendingAtoms.containsKey(atom);
	}
	
	/**
	 * Returns an existing pending atom or creates it providing that it is not timed out or committed.
	 * 
	 * @param atom The atom hash
	 * @throws IOException
	 * @throws  
	 */
	
	PendingAtom get(final Hash atom) throws IOException
	{
		Objects.requireNonNull(atom, "Atom hash is null");
		Hash.notZero(atom, "Atom hash is zero");

		if (this.context.getLedger().isSynced() == false)
			throw new SyncStatusException("Ledger sync state is false!  AtomHandler::get called");

		final Lock loadLock = AtomHandler.this.loadlocks[Math.abs(atom.hashCode() % AtomHandler.this.loadlocks.length)];
		loadLock.lock();
		try
		{
			PendingAtom pendingAtom = this.pendingAtoms.get(atom);
			if (pendingAtom != null)
				return pendingAtom;

			SubstateCommit substateCommit = null;
			BlockHeader persistedBlock = null;
			
			// TODO Atoms are only persisted if they complete the pipeline resulting in either commitment or failure (inc exceptions)
			//      Therefore all the "persisted atom" logic here is perhaps redundant, or at least, simplified.
			final Atom persistedAtom = this.context.getLedger().getLedgerStore().get(atom, Atom.class);
			if (persistedAtom != null)
			{
				substateCommit = this.context.getLedger().getLedgerStore().search(StateAddress.from(Atom.class, atom));
				if (substateCommit != null)
				{
					if (Universe.get().getGenesis().getHash().equals(substateCommit.getSubstate().get(NativeField.BLOCK)))
						throw new IllegalStateException("Called AtomHandler::get with genesis atom hash "+atom);

					if (substateCommit.getSubstate().get(NativeField.CERTIFICATE) != null)
					{
						if (atomsLog.hasLevel(Logging.INFO))
							atomsLog.warn(this.context.getName()+": Atom "+atom+" is completed and has a certificate");
						
						return null;
					}
						
					if (substateCommit.getSubstate().get(NativeField.TIMEOUT) != null)
					{
						if (atomsLog.hasLevel(Logging.INFO))
							atomsLog.warn(this.context.getName()+": Atom "+atom+" is timed out");
						
						return null;
					}

					if (substateCommit.getSubstate().get(NativeField.BLOCK) != null)
					{
						persistedBlock = this.context.getLedger().get(substateCommit.getSubstate().get(NativeField.BLOCK), BlockHeader.class);
						if (persistedBlock == null)
							throw new IllegalStateException("Expected to find block "+substateCommit.getSubstate().get(NativeField.BLOCK)+" containing atom "+atom);
					}
				}
			}

			try
			{
				pendingAtom = new PendingAtom(this.context, atom);
				if (persistedAtom != null)
				{
					pendingAtom.setAtom(persistedAtom);
					
					if (atomsLog.hasLevel(Logging.INFO))
						atomsLog.info(this.context.getName()+": Pending atom "+atom+" instance created");
				}
				else if (atomsLog.hasLevel(Logging.INFO))
					atomsLog.info(this.context.getName()+": Pending atom "+atom+" reference created");
			}
			catch (ManifestException mex)
			{
				throw new IOException(mex);
			}
			
			this.pendingAtoms.put(atom, pendingAtom);
			return pendingAtom;
		}
		finally
		{
			loadLock.unlock();
		}
	}
	
	/**
	 * Returns an existing pending atom or creates it providing that it is not timed out or committed.
	 * 
	 * @param atom The atom hash
	 * @throws IOException
	 * @throws  
	 */
	
	AtomStatus.State status(final Hash atom) throws IOException
	{
		Objects.requireNonNull(atom, "Atom hash is null");
		Hash.notZero(atom, "Atom hash is zero");

		if (this.context.getLedger().isSynced() == false)
			throw new SyncStatusException("Ledger sync state is false!  AtomHandler::status called");

		final Lock loadLock = AtomHandler.this.loadlocks[Math.abs(atom.hashCode() % AtomHandler.this.loadlocks.length)];
		loadLock.lock();
		try
		{
			final PendingAtom pendingAtom = this.pendingAtoms.get(atom);
			if (pendingAtom != null)
				return pendingAtom.getStatus().current();

			final SubstateCommit substateCommit = this.context.getLedger().getLedgerStore().search(StateAddress.from(Atom.class, atom));
			if (substateCommit != null)
			{
				if (Universe.get().getGenesis().getHash().equals(substateCommit.getSubstate().get(NativeField.BLOCK)))
					throw new IllegalStateException("Called AtomHandler::get with genesis atom hash "+atom);

				if (substateCommit.getSubstate().get(NativeField.CERTIFICATE) != null || substateCommit.getSubstate().get(NativeField.TIMEOUT) != null)
					return AtomStatus.State.COMPLETED;
			}

			return AtomStatus.State.NONE;
		}
		finally
		{
			loadLock.unlock();
		}
	}

	private void push(final PendingAtom pendingAtom, final BlockHeader head)
	{
		Objects.requireNonNull(pendingAtom, "Pending atom for injection is null");
		
		if (pendingAtom.getStatus().current(AtomStatus.State.ACCEPTED) == false)
			throw new IllegalStateException(this.context.getName()+": Pending atom "+pendingAtom.getHash()+" for injection must be in ACCEPTED state");
		
		// Dont use the atom handler lock here.  
		// Should be safe to just insert as pendingAtoms is a ConcurrentMap
		if (this.pendingAtoms.putIfAbsent(pendingAtom.getHash(), pendingAtom) != null)
			throw new IllegalStateException(this.context.getName()+": Pending atom "+pendingAtom.getHash()+" is already present");
		
		// Manage timeout generation or scheduling
/*		AtomTimeout timeout = pendingAtom.getTimeout();
		if (timeout == null)
			timeout = pendingAtom.tryTimeout(head.getTimestamp());
		
		if (timeout == null)
			scheduleTimeout(pendingAtom, pendingAtom.getNextTimeoutAt());
		else
			this.context.getEvents().post(new AtomTimeoutEvent(pendingAtom, timeout));*/

		if (atomsLog.hasLevel(Logging.INFO))
			atomsLog.info(AtomHandler.this.context.getName()+": Pushed pending atom "+pendingAtom.getHash());
	}
	
	private void remove(final PendingAtom pendingAtom)
	{
		Objects.requireNonNull(pendingAtom, "Pending atom to remove is null");
		
		pendingAtom.completed();

		this.pendingAtoms.remove(pendingAtom.getHash());
		this.latent.remove(pendingAtom.getHash());
		this.executable.remove(pendingAtom.getHash());
		this.unaccepted.remove(pendingAtom.getHash());
		this.acceptable.remove(pendingAtom.getHash());

		this.atomsToPrepareQueue.remove(pendingAtom.getHash());
		this.atomsToProvisionQueue.remove(pendingAtom.getHash());
		
		if (pendingAtom.getAtom() != null)
		{
			this.context.getMetaData().increment("ledger.atom.processed");
			this.context.getMetaData().increment("ledger.atom.latency", pendingAtom.getAtom().getAge(TimeUnit.MILLISECONDS));
		}
			
		if (pendingAtom.getCertificate() != null)
		{
			if (this.certificates.remove(pendingAtom.getCertificate().getHash(), pendingAtom) == false)
				atomsLog.error(AtomHandler.this.context.getName()+": Certificate "+pendingAtom.getCertificate().getHash()+" for pending atom "+pendingAtom.getHash()+" was not removed");
			
			this.context.getMetaData().increment("ledger.atomcertificate.processed");
			this.context.getMetaData().increment("ledger.atomcertificate.latency", pendingAtom.getCertificate().getAge(TimeUnit.MILLISECONDS));
		}
			
		if (pendingAtom.getTimeout() != null)
		{
			this.context.getMetaData().increment("ledger.atomtimeout.processed");
			this.context.getMetaData().increment("ledger.atomtimeout.latency", pendingAtom.getTimeout().getAge(TimeUnit.MILLISECONDS));

			if (pendingAtom.getTimeout(ExecutionTimeout.class) != null)
			{
				if (this.unexecuted.remove(pendingAtom.getTimeout(ExecutionTimeout.class).getHash(), pendingAtom) == false)
					atomsLog.error(AtomHandler.this.context.getName()+": Execution timeout "+pendingAtom.getTimeout(ExecutionTimeout.class).getHash()+" for pending atom "+pendingAtom.getHash()+" was not removed");
			}
	
			if (pendingAtom.getTimeout(CommitTimeout.class) != null)
			{
				if (this.uncommitted.remove(pendingAtom.getTimeout(CommitTimeout.class).getHash(), pendingAtom) == false)
					atomsLog.error(AtomHandler.this.context.getName()+": Commit timeout "+pendingAtom.getTimeout(CommitTimeout.class).getHash()+" for pending atom "+pendingAtom.getHash()+" was not removed");
			}
		}

		if (atomsLog.hasLevel(Logging.DEBUG))
			atomsLog.debug(AtomHandler.this.context.getName()+": Removed pending atom "+pendingAtom);
	}
	
	boolean submit(final Atom atom, final boolean force)
	{
		Objects.requireNonNull(atom, "Atom is null");
		
		if (this.context.getNode().isSynced() == false)
			throw new UnsupportedOperationException("Node is not in sync with network");
		
		// TODO need a deterministic way to do this
		if (force == false && this.pendingAtoms.size() > Constants.ATOM_DISCARD_AT_PENDING_LIMIT)
			return false;
		
		if (this.atomsToProvisionQueue.putIfAbsent(atom.getHash(), atom) != null)
		{
			atomsLog.warn(AtomHandler.this.context.getName()+": Atom "+atom.getHash()+" is already pending");
			return false;
		}
		
		if (atomsLog.hasLevel(Logging.DEBUG))
			atomsLog.debug(this.context.getName()+": Atom "+atom.getHash()+" is submitted");
		
		this.atomProcessor.signal();
		return true;
	}

	Collection<Atom> submit(final Collection<Atom> atoms, final boolean force)
	{
		Objects.requireNonNull(atoms, "Atoms is null");
		
		if (this.context.getNode().isSynced() == false)
			throw new SyncStatusException("Node is not in sync with network");
		
		// Ensure all the atom hashes are computed BEFORE we lock the queue for insertion.
		// Otherwise any missing hashes which havent been computed will be done so inside the lambda and keep the queue locked!
		atoms.forEach(a -> a.getHash());
		
		// TODO need a deterministic way to do this
		if (force == false && this.pendingAtoms.size() > Constants.ATOM_DISCARD_AT_PENDING_LIMIT)
			return Collections.emptyList();
		
		final Collection<Atom> submitted = this.atomsToProvisionQueue.putAll(atoms, a -> {
			if (atomsLog.hasLevel(Logging.DEBUG))
				atomsLog.debug(this.context.getName()+": Atom "+a.getHash()+" is submitted");
			
			return a.getHash();
		});
		
		if (submitted.isEmpty() == false)
			this.atomProcessor.signal();

		return submitted;
	}

	// SYNC BLOCK LISTENER //
	private SynchronousEventListener syncBlockListener = new SynchronousEventListener()
	{
		@Subscribe
		public void on(final BlockCommittedEvent blockCommittedEvent)
		{
			blockCommittedEvent.getPendingBlock().forInventory(InventoryType.ACCEPTED, pendingAtom -> AtomHandler.this.context.getEvents().post(new AtomAcceptedEvent(blockCommittedEvent.getPendingBlock().getHeader(), pendingAtom)));
			blockCommittedEvent.getPendingBlock().forInventory(InventoryType.COMMITTED, pendingAtom -> AtomHandler.this.context.getEvents().post(new AtomCommitEvent(blockCommittedEvent.getPendingBlock().getHeader(), pendingAtom)));
			blockCommittedEvent.getPendingBlock().forInventory(InventoryType.UNEXECUTED, pendingAtom -> AtomHandler.this.context.getEvents().post(new AtomExecutionTimeoutEvent(blockCommittedEvent.getPendingBlock().getHeader(), pendingAtom)));
			blockCommittedEvent.getPendingBlock().forInventory(InventoryType.UNCOMMITTED, pendingAtom -> AtomHandler.this.context.getEvents().post(new AtomCommitTimeoutEvent(blockCommittedEvent.getPendingBlock().getHeader(), pendingAtom)));
			blockCommittedEvent.getPendingBlock().forInventory(InventoryType.UNACCEPTED, pendingAtom -> AtomHandler.this.context.getEvents().post(new AtomAcceptedTimeoutEvent(blockCommittedEvent.getPendingBlock().getHeader(), pendingAtom)));
			blockCommittedEvent.getPendingBlock().forInventory(InventoryType.EXECUTABLE, pendingAtom -> AtomHandler.this.context.getEvents().post(new AtomExecutableEvent(blockCommittedEvent.getPendingBlock().getHeader(), pendingAtom)));
			blockCommittedEvent.getPendingBlock().forInventory(InventoryType.LATENT, pendingAtom -> AtomHandler.this.context.getEvents().post(new AtomExecuteLatentEvent(blockCommittedEvent.getPendingBlock().getHeader(), pendingAtom)));
		}
	};

	// SYNC ATOM LISTENER //
	private SynchronousEventListener syncAtomListener = new SynchronousEventListener()
	{
		@Subscribe
		public void on(final AtomCommitEvent event) 
		{
			remove(event.getPendingAtom());
			
			if (atomsLog.hasLevel(Logging.DEBUG))
				atomsLog.debug(AtomHandler.this.context.getName()+": Pending atom "+event.getPendingAtom().getHash()+" committed on block "+event.getProposalHeader().getHeight()+":"+event.getProposalHeader().getHash());
		}

		@Subscribe
		public void on(final AtomPrepareTimeoutEvent event) 
		{
			remove(event.getPendingAtom());
			
			if (atomsLog.hasLevel(Logging.INFO))
				atomsLog.info(AtomHandler.this.context.getName()+": Atom "+event.getPendingAtom().getHash()+" prepare timedout at "+System.currentTimeMillis());
		}

		@Subscribe
		public void on(final AtomAcceptedTimeoutEvent event)
		{
			if (event.getPendingAtom().getStatus().after(AtomStatus.State.PREPARED))
				atomsLog.warn(AtomHandler.this.context.getName()+": Unaccepted atom "+event.getPendingAtom().getHash()+" has status of "+event.getPendingAtom().getStatus());

			remove(event.getPendingAtom());
			
			if (atomsLog.hasLevel(Logging.INFO))
				atomsLog.info(AtomHandler.this.context.getName()+": Atom "+event.getPendingAtom().getHash()+" accept timedout at block "+event.getProposalHeader().getHeight()+":"+event.getProposalHeader().getHash());
		}
		
		@Subscribe
		public void on(final AtomExecutableEvent event)
		{
			if (Constants.SKIP_EXECUTION_SIGNALS)
				return;

			event.getPendingAtom().setExecuteSignalledAtBlock(event.getProposalHeader());

			AtomHandler.this.executable.remove(event.getPendingAtom().getHash());
			AtomHandler.this.latent.remove(event.getPendingAtom().getHash());
			
			if (atomsLog.hasLevel(Logging.DEBUG))
				atomsLog.debug(AtomHandler.this.context.getName()+": Atom "+event.getPendingAtom().getHash()+" executable at block "+event.getProposalHeader().getHeight()+":"+event.getProposalHeader().getHash());
		}
		
		@Subscribe
		public void on(final AtomExecutedEvent event)
		{
			AtomHandler.this.unexecuted.remove(event.getPendingAtom().getHash());
		}

		@Subscribe
		public void on(final AtomExecuteLatentEvent event)
		{
			event.getPendingAtom().setExecuteLatentSignalledBlock(event.getProposalHeader());
			AtomHandler.this.latent.remove(event.getPendingAtom().getHash());
			
			if (atomsLog.hasLevel(Logging.DEBUG))
				atomsLog.debug(AtomHandler.this.context.getName()+": Atom "+event.getPendingAtom().getHash()+" execute latent at block "+event.getProposalHeader().getHeight()+":"+event.getProposalHeader().getHash());
		}
		
		@Subscribe
		public void on(final AtomExecutionTimeoutEvent event)
		{
			remove(event.getPendingAtom());

			if (atomsLog.hasLevel(Logging.INFO))
			{
				atomsLog.info(AtomHandler.this.context.getName()+": Atom "+event.getPendingAtom().getHash()+" execution timeout at block "+event.getProposalHeader().getHeight()+":"+event.getProposalHeader().getHash());
				atomsLog.info(AtomHandler.this.context.getName()+":   "+event.getPendingAtom().numStateAddresses(null)+" substates "+event.getPendingAtom().getStateAddresses(null));
				atomsLog.info(AtomHandler.this.context.getName()+":   "+event.getPendingAtom().getOutputs(StateOutput.class));
			}
		}

		@Subscribe
		public void on(final AtomCommitTimeoutEvent event)
		{
			remove(event.getPendingAtom());

			if (atomsLog.hasLevel(Logging.INFO))
			{
				atomsLog.info(AtomHandler.this.context.getName()+": Atom "+event.getPendingAtom().getHash()+" commit timeout at block "+event.getProposalHeader().getHeight()+":"+event.getProposalHeader().getHash());
				atomsLog.info(AtomHandler.this.context.getName()+":   "+event.getPendingAtom().numStateAddresses(null)+" substates "+event.getPendingAtom().getStateAddresses(null));
				atomsLog.info(AtomHandler.this.context.getName()+":   "+event.getPendingAtom().getOutputs(StateOutput.class));
			}
		}
		
		@Subscribe
		public void on(final AtomTimeoutEvent event)
		{
			if (event.getTimeout() instanceof PrepareTimeout)
				AtomHandler.this.context.getEvents().post(new AtomPrepareTimeoutEvent(event.getPendingAtom()));
			
			if (event.getTimeout() instanceof AcceptTimeout)
				AtomHandler.this.unaccepted.putIfAbsent(event.getPendingAtom().getHash(), AtomHandler.this.context.getLedger().getHead().getHeight());
			
			if (event.getTimeout() instanceof ExecutionLatentTimeout)
				AtomHandler.this.latent.putIfAbsent(event.getPendingAtom().getHash(), AtomHandler.this.context.getLedger().getHead().getHeight());
			
			if (event.getTimeout() instanceof ExecutionTimeout)
				AtomHandler.this.unexecuted.putIfAbsent(event.getTimeout().getHash(), event.getPendingAtom());
			
			if (event.getTimeout() instanceof CommitTimeout)
				AtomHandler.this.uncommitted.putIfAbsent(event.getTimeout().getHash(), event.getPendingAtom());
		}
		
		@Subscribe
		public void on(final AtomPreparedEvent event)
		{
			event.getPendingAtom().setAcceptable();
			AtomHandler.this.context.getEvents().post(new AtomAcceptableEvent(event.getPendingAtom()));
		}

		@Subscribe
		public void on(final AtomAcceptableEvent event) 
		{
			if (event.getPendingAtom().isAcceptable() == false)
			{
				atomsLog.error(AtomHandler.this.context.getName()+": Pending atom "+event.getPendingAtom().getHash()+" is not acceptable");
				return;
			}
			
			if (atomsLog.hasLevel(Logging.DEBUG))
				atomsLog.info(AtomHandler.this.context.getName()+": Pending atom "+event.getPendingAtom().getHash()+" is acceptable");

			if (event.getPendingAtom().isForceAcceptTimeout() == true)
				return;
			
			AtomHandler.this.acceptable.put(event.getPendingAtom().getHash(), event.getPendingAtom());
		}

		@Subscribe
		public void on(final AtomAcceptedEvent event) throws IOException 
		{
			event.getPendingAtom().accepted(event.getProposalHeader());
			
			if (AtomHandler.this.acceptable.remove(event.getPendingAtom().getHash(), event.getPendingAtom()) == false)
				atomsLog.warn(AtomHandler.this.context.getName()+": Accepted pending atom "+event.getPendingAtom().getHash()+" not found in acceptable registry for block "+event.getProposalHeader().getHeight()+":"+event.getProposalHeader().getHash());

			// Need to update timeouts in the case that an ACCEPT timeout was produced which will need to be discarded.
			// Timeouts are weakly-subjective.  Local replica may have considered an atom timeout valid but remote replicas may not.
			// TODO is there a better way to manage this?
			if (AtomHandler.this.unaccepted.remove(event.getPendingAtom().getHash(), event.getPendingAtom()) == true && atomsLog.hasLevel(Logging.DEBUG))
				atomsLog.debug(AtomHandler.this.context.getName()+": Removed pending atom "+event.getPendingAtom().getHash()+" from UNACCEPTED that is now ACCEPTED");
		}

		@Subscribe
		public void on(final AtomExceptionEvent event)
		{
			atomsLog.error(AtomHandler.this.context.getName()+": Atom "+event.getPendingAtom().getAtom().getHash()+" threw exception", event.getException());
			for(String instruction : event.getPendingAtom().getAtom().getManifest())
				atomsLog.error(AtomHandler.this.context.getName()+":      "+instruction);
			for(Identity signer : event.getPendingAtom().getAtom().getAuthorities())
				atomsLog.error(AtomHandler.this.context.getName()+":      "+signer);
			atomsLog.error(AtomHandler.this.context.getName()+": ", event.getException());
			event.getPendingAtom().getStatus().thrown(event.getException());
			
			if (event.getPendingAtom().getStatus().current(State.FINALIZING))
				AtomHandler.this.remove(event.getPendingAtom());
		}
	};
	
	// ASYNC ATOM LISTENER //
	private EventListener asyncAtomListener = new EventListener()
	{
		@Subscribe
		public void on(final AtomProvisionedEvent event) 
		{
			if (atomsLog.hasLevel(Logging.DEBUG))
				atomsLog.info(AtomHandler.this.context.getName()+": Atom "+event.getPendingAtom().getHash()+" is provisioned");

			if (event.getPendingAtom().isForceExecutionTimeout())
				return;
			
			if (Constants.SKIP_EXECUTION_SIGNALS)
			{
				event.getPendingAtom().setExecuteSignalledAtBlock(event.getPendingAtom().getBlockHeader());
				AtomHandler.this.context.getEvents().post(new AtomExecutableEvent(event.getPendingAtom().getBlockHeader(), event.getPendingAtom()));
			}
			else if (AtomHandler.this.executable.putIfAbsent(event.getPendingAtom().getHash(), event.getPendingAtom()) != null)
				atomsLog.error(AtomHandler.this.context.getName()+" Atom "+event.getPendingAtom().getHash()+" is already scheduled for execution signal");
		}

		@Subscribe
		public void on(final AtomCertificateEvent event) throws IOException 
		{
			PendingAtom pendingAtom = get(event.getCertificate().getAtom());
			if (pendingAtom == null)
			{
				atomsLog.error(AtomHandler.this.context.getName()+": Pending atom "+event.getCertificate().getAtom()+" for certificate "+event.getCertificate().getHash()+" not found");
				return;
			}
			
			AtomHandler.this.certificates.put(event.getCertificate().getHash(), pendingAtom);
		}
	};
	
	// SYNC CHANGE LISTENER //
	private SynchronousEventListener syncChangeListener = new SynchronousEventListener()
	{
		@Subscribe
		public void on(final SyncAcquiredEvent event)
		{
			atomsLog.log(AtomHandler.this.context.getName()+": Sync status acquired, preparing atom handler");
			AtomHandler.this.atomsToPrepareQueue.clear();
			AtomHandler.this.atomsToProvisionQueue.clear();
			AtomHandler.this.pendingAtoms.clear();
			AtomHandler.this.acceptable.clear();
			AtomHandler.this.unaccepted.clear();
			AtomHandler.this.latent.clear();
			AtomHandler.this.executable.clear();
			AtomHandler.this.unexecuted.clear();
			AtomHandler.this.uncommitted.clear();
			AtomHandler.this.certificates.clear();

			for (PendingAtom pendingAtom : event.getAtoms())
				AtomHandler.this.push(pendingAtom, event.getHead());
		}
		
		@Subscribe
		public void on(final SyncLostEvent event) 
		{
			atomsLog.log(AtomHandler.this.context.getName()+": Sync status lost, flushing atom handler");
			AtomHandler.this.atomsToPrepareQueue.clear();
			AtomHandler.this.atomsToProvisionQueue.clear();
			AtomHandler.this.pendingAtoms.clear();
			AtomHandler.this.acceptable.clear();
			AtomHandler.this.unaccepted.clear();
			AtomHandler.this.latent.clear();
			AtomHandler.this.executable.clear();
			AtomHandler.this.unexecuted.clear();
			AtomHandler.this.uncommitted.clear();
			AtomHandler.this.certificates.clear();
		}
	};
	
	// SEARCH
	@Override
	public Future<PrimitiveSearchResponse> get(final PrimitiveSearchQuery query) 
	{
		if (query.getType().equals(Atom.class) == false || 
			query.getIsolation().equals(Isolation.PENDING) == false)
			return null;
		
		final ShardGroupID localShardGroupID = ShardMapper.toShardGroup(this.context.getNode().getIdentity(), this.context.getLedger().numShardGroups());
		
		try
		{
			final PendingAtom pendingAtom = this.pendingAtoms.get(query.getQuery());
			if (pendingAtom == null)
				return null;
			
			if (pendingAtom.getStatus().before(AtomStatus.State.PREPARED))
				return null;

			// Does this pending atom reference or set a state address that is local?
			final List<StateAddress> stateAddresses = pendingAtom.getStateAddresses(StateLockMode.WRITE); 
			for (int i = 0 ; i < stateAddresses.size() ; i++)
			{
				final StateAddress stateAddress = stateAddresses.get(i);
				final ShardGroupID stateAddressShardGroupID = ShardMapper.toShardGroup(stateAddress, this.context.getLedger().numShardGroups());
				if (stateAddressShardGroupID.equals(localShardGroupID) == false)
					continue;

				// TODO enable Atom/Particle certain container types for Isolation.PENDING
				return CompletableFuture.completedFuture(new PrimitiveSearchResponse(query, pendingAtom.getAtom()));
			}
			
			return null;
		}
		catch (Throwable t)
		{
			atomsLog.error(AtomHandler.this.context.getName()+": Failed to perform state search on AtomHandler for "+query, t);
			return null;
		}			
	}

	@Override
	public Future<SubstateSearchResponse> get(final SubstateSearchQuery query) 
	{
		try
		{
			final ShardGroupID localShardGroupID = ShardMapper.toShardGroup(this.context.getNode().getIdentity(), this.context.getLedger().numShardGroups());
			final ShardGroupID searchShardGroupID = ShardMapper.toShardGroup(query.getAddress(), this.context.getLedger().numShardGroups());
			if (localShardGroupID.equals(searchShardGroupID))
			{
				for (final PendingAtom pendingAtom : this.pendingAtoms.values())
				{
					if (pendingAtom.getStatus().before(AtomStatus.State.PREPARED))
						continue;
					
					if (pendingAtom.getCertificate() != null && pendingAtom.getCertificate().getDecision().equals(CommitDecision.ACCEPT) == false)
						continue;
					
					for (final PendingState pendingState : pendingAtom.getStates(StateLockMode.WRITE))
					{
						if (pendingState.getAddress().equals(query.getAddress()) == false)
							continue;

						// TODO should be only writes?
						if (pendingState.getSubstateLog().isTouched() == false)
							continue;
						
						// TODO want to support Isolation.PENDING substate?  How would that even work??
						// TODO enable Atom/Particle certain container types for Isolation.PENDING
//						if (query.getReturnType().equals(ReturnType.PRIMITIVE) == true)
//							return CompletableFuture.completedFuture(new SearchResult(pendingAtom.getAtom(), Atom.class));
					}
				}
			}
			
			return null;
		}
		catch (Throwable t)
		{
			atomsLog.error(AtomHandler.this.context.getName()+": Failed to perform state search on AtomHandler for "+query, t);
			return null;
		}			
	}

	@Override
	public Future<AssociationSearchResponse> get(final AssociationSearchQuery query) 
	{
		throw new UnsupportedOperationException("Association search not yet supported in AtomPool");
	}
}
