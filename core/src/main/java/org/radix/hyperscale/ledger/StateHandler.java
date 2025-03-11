package org.radix.hyperscale.ledger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.eclipse.collections.api.factory.Sets;
import org.radix.hyperscale.Configuration;
import org.radix.hyperscale.Constants;
import org.radix.hyperscale.Context;
import org.radix.hyperscale.Service;
import org.radix.hyperscale.collections.LRUCacheMap;
import org.radix.hyperscale.collections.MappedBlockingQueue;
import org.radix.hyperscale.common.Primitive;
import org.radix.hyperscale.crypto.CryptoException;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.crypto.bls12381.BLS12381;
import org.radix.hyperscale.crypto.bls12381.BLSPublicKey;
import org.radix.hyperscale.events.EventListener;
import org.radix.hyperscale.events.SyncLostEvent;
import org.radix.hyperscale.events.SynchronousEventListener;
import org.radix.hyperscale.exceptions.StartupException;
import org.radix.hyperscale.exceptions.TerminationException;
import org.radix.hyperscale.exceptions.ValidationException;
import org.radix.hyperscale.executors.LatchedProcessor;
import org.radix.hyperscale.executors.PollingProcessor;
import org.radix.hyperscale.ledger.BlockHeader.InventoryType;
import org.radix.hyperscale.ledger.SubstateRequestHandler.SubstateRequest;
import org.radix.hyperscale.ledger.events.AtomAcceptedEvent;
import org.radix.hyperscale.ledger.events.AtomCertificateEvent;
import org.radix.hyperscale.ledger.events.AtomCommitEvent;
import org.radix.hyperscale.ledger.events.AtomCommitTimeoutEvent;
import org.radix.hyperscale.ledger.events.AtomExceptionEvent;
import org.radix.hyperscale.ledger.events.AtomExecutableEvent;
import org.radix.hyperscale.ledger.events.AtomExecutedEvent;
import org.radix.hyperscale.ledger.events.AtomExecutionTimeoutEvent;
import org.radix.hyperscale.ledger.events.AtomPreparedEvent;
import org.radix.hyperscale.ledger.events.AtomProvisionedEvent;
import org.radix.hyperscale.ledger.events.BlockCommittedEvent;
import org.radix.hyperscale.ledger.events.ProgressPhaseEvent;
import org.radix.hyperscale.ledger.events.StateCertificateConstructedEvent;
import org.radix.hyperscale.ledger.events.SyncAcquiredEvent;
import org.radix.hyperscale.ledger.messages.SyncAcquiredMessage;
import org.radix.hyperscale.ledger.primitives.AtomCertificate;
import org.radix.hyperscale.ledger.primitives.StateCertificate;
import org.radix.hyperscale.ledger.primitives.StateInput;
import org.radix.hyperscale.ledger.primitives.StateOutput;
import org.radix.hyperscale.ledger.sme.exceptions.StateMachinePreparationException;
import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;
import org.radix.hyperscale.network.AbstractConnection;
import org.radix.hyperscale.network.GossipFetcher;
import org.radix.hyperscale.network.GossipFilter;
import org.radix.hyperscale.network.GossipInventory;
import org.radix.hyperscale.network.GossipReceiver;
import org.radix.hyperscale.network.MessageProcessor;
import org.radix.hyperscale.network.messages.InventoryMessage;
import org.radix.hyperscale.utils.Base58;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.eventbus.Subscribe;
import com.sleepycat.je.OperationStatus;

public final class StateHandler implements Service
{
	private final static int MAX_ATOMS_TO_PROVISION = 256;
	private final static int MAX_STATE_CERTIFICATES_TO_PROCESS = 256;

	private final LRUCacheMap<Hash, StateVerificationRecord> verificationCache;
	
	private static final Logger syncLog = Logging.getLogger("sync");
	private static final Logger stateLog = Logging.getLogger("state");
	
	private final Context context;
	private final SubstateRequestHandler substateRequestHandler;

	private final ArrayBlockingQueue<PendingAtom> executionQueue;
	private final MappedBlockingQueue<Hash, PendingAtom> provisioningQueue;
	private final MappedBlockingQueue<Hash, StateInput> stateInputsToProcessQueue;
	private final MappedBlockingQueue<Hash, StateCertificate> stateCertificatesToProcessQueue;
	private final Multimap<SubstateRequest, PendingAtom> pendingSubstateRequests;
	
	private LatchedProcessor stateProcessor = new LatchedProcessor(1, TimeUnit.SECONDS)
	{
		@Override
		public void process()
		{
			if (StateHandler.this.context.getNode().isSynced() == false)
				return;
						
			// Cache for this iteration as behind a lock
			final Epoch epoch = StateHandler.this.context.getLedger().getEpoch();
			final int numShardGroups = StateHandler.this.context.getLedger().numShardGroups(epoch);

			_checkSubstateReadRequests();
				
			_doSubstateProvisioning(epoch, numShardGroups);
			
			_processStateInputs(epoch, numShardGroups);

			_processStateCertificates(epoch, numShardGroups);
		}

		@Override
		public void onError(Throwable thrown) 
		{
			stateLog.fatal(StateHandler.this.context.getName()+": Error processing provisioning queue", thrown);
		}
		
		@Override
		public void onTerminated()
		{
			stateLog.log(StateHandler.this.context.getName()+": State processor is terminated");
		}
	};
	
    private final ExecutorService executorService;
	private PollingProcessor executionProcessor = new PollingProcessor()
	{
		@Override
		public void process() throws InterruptedException
		{
			final PendingAtom pendingAtomToExecute = StateHandler.this.executionQueue.poll(1, TimeUnit.SECONDS);
			if (pendingAtomToExecute == null)
				return;

			if (StateHandler.this.context.getNode().isSynced() == false)
				return;

			try
			{
				if (stateLog.hasLevel(Logging.DEBUG))
					stateLog.debug(StateHandler.this.context.getName()+": Executing atom "+pendingAtomToExecute);

            	StateHandler.this.executorService.submit(() -> StateHandler.this.execute(pendingAtomToExecute));
			}
			catch (Exception ex)
			{
				stateLog.error(StateHandler.this.context.getName()+": Error executing "+pendingAtomToExecute, ex);
					
				// TODO review if allowing atoms that throw any exception on provisioning should be allowed to simply commit-timeout
				// 		or is it better to have a more explicit means of dealing with exceptions that might not be relevant to the 
				//		execution of state.
				// StateHandler.this.context.getEvents().post(new AtomExceptionEvent(pendingAtom, ex));
			}
		}
		
		@Override
		public void onError(Throwable thrown) 
		{
			stateLog.fatal(StateHandler.this.context.getName()+": Error processing execution queue", thrown);
		}
		
		@Override
		public void onTerminated()
		{
			stateLog.log(StateHandler.this.context.getName()+": Execution processor is terminated");
		}
	};

	StateHandler(final Context context)
	{
		this.context = Objects.requireNonNull(context, "Context is null");
		
		this.substateRequestHandler = new SubstateRequestHandler(context);

		this.executionQueue = new ArrayBlockingQueue<PendingAtom>(this.context.getConfiguration().get("ledger.atom.queue", 1<<12));
        this.executorService = Executors.newFixedThreadPool(this.context.getConfiguration().get("ledger.execution.threads", 1), runnable -> new Thread(runnable, context.getName() + " Execution Thread"));
        
		this.provisioningQueue = new MappedBlockingQueue<Hash, PendingAtom>(this.context.getConfiguration().get("ledger.state.queue", 1<<14));
		this.pendingSubstateRequests = Multimaps.synchronizedSetMultimap(HashMultimap.create());
		this.stateInputsToProcessQueue = new MappedBlockingQueue<Hash, StateInput>(this.context.getConfiguration().get("ledger.state.queue", 1<<14));
		this.stateCertificatesToProcessQueue = new MappedBlockingQueue<Hash, StateCertificate>(this.context.getConfiguration().get("ledger.state.queue", 1<<14));
        
		this.verificationCache = new LRUCacheMap<Hash, StateVerificationRecord>(Configuration.getDefault().get("ledger.state.verification.cache", 1<<12));
	}

	@Override
	public void start() throws StartupException
	{
		this.substateRequestHandler.start();
		
		// STATE CERTIFICATE GOSSIP //
		this.context.getNetwork().getGossipHandler().register(StateCertificate.class, new GossipFilter<StateCertificate>(this.context) 
		{
			@Override
			public Set<ShardGroupID> filter(StateCertificate stateCertificate)
			{
				return Collections.singleton(ShardMapper.toShardGroup(StateHandler.this.context.getNode().getIdentity(), StateHandler.this.context.getLedger().numShardGroups()));
			}
		});

		this.context.getNetwork().getGossipHandler().register(StateCertificate.class, new GossipInventory() 
		{
			@Override
			public Collection<Hash> required(final Class<? extends Primitive> type, final Collection<Hash> items, final AbstractConnection connection) throws IOException
			{
				if (type.equals(StateCertificate.class) == false)
				{
					stateLog.error(StateHandler.this.context.getName()+": State certificate type expected but got "+type);
					return Collections.emptyList();
				}
				
				if (StateHandler.this.context.getNode().isSynced() == false)
					return Collections.emptyList();
				
				final List<Hash> required = new ArrayList<Hash>(items);
				required.removeAll(StateHandler.this.stateCertificatesToProcessQueue.contains(required));
				required.removeAll(StateHandler.this.context.getLedger().getLedgerStore().has(required, type));

				return required;
			}
		});

		this.context.getNetwork().getGossipHandler().register(StateCertificate.class, new GossipReceiver<StateCertificate>() 
		{
			@Override
			public void receive(Collection<StateCertificate> stateCertificates, AbstractConnection connection) throws IOException, CryptoException
			{
				if (StateHandler.this.context.getNode().isSynced() == false)
					return;
				
				for (StateCertificate stateCertificate : stateCertificates)
				{
					if (ShardMapper.equal(StateHandler.this.context.getLedger().numShardGroups(), stateCertificate.getAddress(), StateHandler.this.context.getNode().getIdentity()) == true)
					{
						stateLog.warn(StateHandler.this.context.getName()+": Received state certificate "+stateCertificate+" for local shard (has sync just happened?)");
						// 	Disconnected and ban
						continue;
					}
					
					if (stateLog.hasLevel(Logging.DEBUG))
						stateLog.debug(StateHandler.this.context.getName()+": Received state certificate "+stateCertificate);
					
					if (StateHandler.this.context.getLedger().getLedgerStore().store(stateCertificate).equals(OperationStatus.SUCCESS))
						StateHandler.this.stateCertificatesToProcessQueue.put(stateCertificate.getHash(), stateCertificate);
					else
						stateLog.warn(StateHandler.this.context.getName()+": Failed to store state certificate "+stateCertificate);
				}
				
				StateHandler.this.stateProcessor.signal();
			}
		});
					
		this.context.getNetwork().getGossipHandler().register(StateCertificate.class, new GossipFetcher<StateCertificate>() 
		{
			@Override
			public Collection<StateCertificate> fetch(Collection<Hash> items, AbstractConnection connection) throws IOException
			{
				final Set<Hash> toFetch = Sets.mutable.ofAll(items);
				final List<StateCertificate> fetched = new ArrayList<StateCertificate>(items.size());

				StateHandler.this.stateCertificatesToProcessQueue.getAll(toFetch, (h, p) -> { fetched.add(p); toFetch.remove(h); });
				StateHandler.this.context.getLedger().getLedgerStore().get(toFetch, StateCertificate.class, (h, p) -> { fetched.add(p); toFetch.remove(h); });
				
				if (toFetch.isEmpty() == false)
					toFetch.forEach(h -> stateLog.error(StateHandler.this.context.getName()+": Requested state certificate "+h+" not found"));
				
				return fetched;
			}
		});
		
		// STATE INPUT GOSSIP //
		this.context.getNetwork().getGossipHandler().register(StateInput.class, new GossipFilter<StateInput>(this.context) 
		{
			@Override
			public Set<ShardGroupID> filter(StateInput stateInput)
			{
				return Collections.singleton(ShardMapper.toShardGroup(StateHandler.this.context.getNode().getIdentity(), StateHandler.this.context.getLedger().numShardGroups()));
			}
		});

		this.context.getNetwork().getGossipHandler().register(StateInput.class, new GossipInventory() 
		{
			@Override
			public Collection<Hash> required(final Class<? extends Primitive> type, final Collection<Hash> items, final AbstractConnection connection) throws IOException
			{
				if (type.equals(StateInput.class) == false)
				{
					stateLog.error(StateHandler.this.context.getName()+": State input type expected but got "+type);
					return Collections.emptyList();
				}
					
				if (StateHandler.this.context.getNode().isSynced() == false)
					return Collections.emptyList();
				
				final List<Hash> required = new ArrayList<Hash>(items);
				required.removeAll(StateHandler.this.stateInputsToProcessQueue.contains(required));
				required.removeAll(StateHandler.this.context.getLedger().getLedgerStore().has(required, type));
				return required;
			}
		});

		this.context.getNetwork().getGossipHandler().register(StateInput.class, new GossipReceiver<StateInput>() 
		{
			@Override
			public void receive(Collection<StateInput> stateInputs, AbstractConnection connection) throws IOException, CryptoException
			{
				if (StateHandler.this.context.getNode().isSynced() == false)
					return;

				for (final StateInput stateInput : stateInputs)
				{
					if (ShardMapper.equal(StateHandler.this.context.getLedger().numShardGroups(), stateInput.getSubstate().getAddress(), StateHandler.this.context.getNode().getIdentity()) == true)
					{
						if (stateLog.hasLevel(Logging.DEBUG))
							stateLog.debug(StateHandler.this.context.getName()+": Received state input "+stateInput+" for local shard (possible consequence of gossip");
						// 	Disconnected and ban
						continue;
					}
	
					if (stateLog.hasLevel(Logging.DEBUG))
						stateLog.debug(StateHandler.this.context.getName()+": Received state input "+stateInput);
					
					if (StateHandler.this.context.getLedger().getLedgerStore().store(stateInput).equals(OperationStatus.SUCCESS))
						StateHandler.this.stateInputsToProcessQueue.put(stateInput.getHash(), stateInput);
					else
						stateLog.warn(StateHandler.this.context.getName()+": Failed to store state input "+stateInput);
				}

				StateHandler.this.stateProcessor.signal();
			}
		});
					
		this.context.getNetwork().getGossipHandler().register(StateInput.class, new GossipFetcher<StateInput>() 
		{
			@Override
			public Collection<StateInput> fetch(final Collection<Hash> items, final AbstractConnection connection) throws IOException
			{
				Set<Hash> toFetch = Sets.mutable.ofAll(items);
				List<StateInput> fetched = new ArrayList<StateInput>(items.size());
				StateHandler.this.stateInputsToProcessQueue.getAll(toFetch, (h, p) -> { fetched.add(p); toFetch.remove(h); });
				StateHandler.this.context.getLedger().getLedgerStore().get(toFetch, StateInput.class, (h, p) -> { fetched.add(p); toFetch.remove(h); });
				
				if (toFetch.isEmpty() == false)
					toFetch.forEach(h -> stateLog.error(StateHandler.this.context.getName()+": Requested substate input "+h+" not found"));
				
				return fetched;
			}
		});
		
		// SYNC //
		this.context.getNetwork().getMessaging().register(SyncAcquiredMessage.class, this.getClass(), new MessageProcessor<SyncAcquiredMessage>()
		{
			@Override
			public void process(final SyncAcquiredMessage syncAcquiredMessage, final AbstractConnection connection)
			{
				if (ShardMapper.equal(StateHandler.this.context.getLedger().numShardGroups(), connection.getNode().getIdentity(), StateHandler.this.context.getNode().getIdentity()) == false)
				{
					stateLog.error(StateHandler.this.context.getName()+": Received SyncAcquiredMessage from "+connection+" in shard group "+ShardMapper.toShardGroup(connection.getNode().getIdentity(), StateHandler.this.context.getLedger().numShardGroups())+" but local is "+ShardMapper.toShardGroup(StateHandler.this.context.getNode().getIdentity(), StateHandler.this.context.getLedger().numShardGroups()));
					// Disconnect and ban?
					return;
				}

				try
				{
					if (stateLog.hasLevel(Logging.DEBUG))
						stateLog.debug(StateHandler.this.context.getName()+": State handler inventory request from "+connection);
					
					final Set<Hash> stateCertificateInventory = new HashSet<Hash>();
					final Set<Hash> stateInputInventory = new HashSet<Hash>();
					
					final Epoch epoch = Epoch.from(syncAcquiredMessage.getHead());
					final int numShardGroups = StateHandler.this.context.getLedger().numShardGroups(epoch);
					ShardGroupID remoteShardGroupID = ShardMapper.toShardGroup(connection.getNode().getIdentity(), numShardGroups);
					
					StateHandler.this.context.getLedger().getAtomHandler().getAll().forEach(pa -> {
						for (final StateCertificate sc : pa.getOutputs(StateCertificate.class))
						{
							final ShardGroupID stateShardGroupID = ShardMapper.toShardGroup(sc.getAddress(), numShardGroups);
							if (stateShardGroupID.equals(remoteShardGroupID))
								continue;
							
							stateCertificateInventory.add(sc.getHash());
						}
						
						for (final StateInput si : pa.getInputs())
						{
							final ShardGroupID stateShardGroupID = ShardMapper.toShardGroup(si.getSubstate().getAddress(), numShardGroups);
							if (stateShardGroupID.equals(remoteShardGroupID))
								continue;
							
							stateInputInventory.add(si.getHash());
						}
					});

					long syncInventoryHeight = Math.max(1, syncAcquiredMessage.getHead().getHeight() - Constants.SYNC_INVENTORY_HEAD_OFFSET);
					while (syncInventoryHeight <= StateHandler.this.context.getLedger().getHead().getHeight())
					{
						StateHandler.this.context.getLedger().getLedgerStore().getSyncInventory(syncInventoryHeight, StateCertificate.class).forEach(ir -> {
							if (ir.getClockEnd() <= syncAcquiredMessage.getHead().getHeight())
								return;
							
							final ShardGroupID stateShardGroupID = ShardMapper.toShardGroup(ir.<StateCertificate>get().getAddress(), numShardGroups);
							if (stateShardGroupID.equals(remoteShardGroupID))
								return;

							stateCertificateInventory.add(ir.getHash());
						});
						
						StateHandler.this.context.getLedger().getLedgerStore().getSyncInventory(syncInventoryHeight, StateInput.class).forEach(ir -> { 
							if (ir.getClockEnd() <= syncAcquiredMessage.getHead().getHeight())
								return;

							final ShardGroupID stateShardGroupID = ShardMapper.toShardGroup(ir.<StateInput>get().getAddress(), numShardGroups);
							if (stateShardGroupID.equals(remoteShardGroupID))
								return;

							stateInputInventory.add(ir.getHash()); 
						});
						
						syncInventoryHeight++;
					}

					if (syncLog.hasLevel(Logging.DEBUG))
						syncLog.debug(StateHandler.this.context.getName()+": Broadcasting state certificates "+stateCertificateInventory.size()+" / "+stateCertificateInventory+" to "+connection);
					else
						syncLog.log(StateHandler.this.context.getName()+": Broadcasting "+stateCertificateInventory.size()+" state certificates to "+connection);

					while(stateCertificateInventory.isEmpty() == false)
					{
						InventoryMessage stateCertificateInventoryMessage = new InventoryMessage(stateCertificateInventory, 0, Math.min(Constants.MAX_BROADCAST_INVENTORY_ITEMS, stateCertificateInventory.size()), StateCertificate.class);
						StateHandler.this.context.getNetwork().getMessaging().send(stateCertificateInventoryMessage, connection);
						stateCertificateInventory.removeAll(stateCertificateInventoryMessage.asInventory().stream().map(ii -> ii.getHash()).collect(Collectors.toList()));
					}
					
					if (syncLog.hasLevel(Logging.DEBUG))
						syncLog.debug(StateHandler.this.context.getName()+": Broadcasting state inputs "+stateInputInventory.size()+" / "+stateInputInventory+" to "+connection);
					else
						syncLog.log(StateHandler.this.context.getName()+": Broadcasting "+stateInputInventory.size()+" state inputs to "+connection);

					while(stateInputInventory.isEmpty() == false)
					{
						InventoryMessage stateInputInventoryMessage = new InventoryMessage(stateInputInventory, 0, Math.min(Constants.MAX_BROADCAST_INVENTORY_ITEMS, stateInputInventory.size()), StateInput.class);
						StateHandler.this.context.getNetwork().getMessaging().send(stateInputInventoryMessage, connection);
						stateInputInventory.removeAll(stateInputInventoryMessage.asInventory().stream().map(ii -> ii.getHash()).collect(Collectors.toList()));
					}
				}
				catch (Exception ex)
				{
					stateLog.error(StateHandler.this.context.getName()+": ledger.messages.sync.acquired " + connection, ex);
				}
			}
		});
		
		this.context.getEvents().register(this.syncChangeListener);
		this.context.getEvents().register(this.syncBlockListener);
		this.context.getEvents().register(this.syncAtomListener);
		this.context.getEvents().register(this.asyncAtomListener);
		this.context.getEvents().register(this.certificateListener);
		this.context.getEvents().register(this.asyncProgressListener);
		
		Thread stateProcessorThread = new Thread(this.stateProcessor);
		stateProcessorThread.setDaemon(true);
		stateProcessorThread.setName(this.context.getName()+" State Processor");
		stateProcessorThread.start();

		Thread executionProcessorThread = new Thread(this.executionProcessor);
		executionProcessorThread.setDaemon(true);
		executionProcessorThread.setName(this.context.getName()+" Execution Processor");
		executionProcessorThread.start();
	}

	@Override
	public void stop() throws TerminationException
	{
		this.executionProcessor.terminate(true);
		this.stateProcessor.terminate(true);
		
    	this.executorService.shutdown();
        try 
        {
            if (this.executorService.awaitTermination(60, TimeUnit.SECONDS) == false)
            	this.executorService.shutdownNow();
            
            stateLog.info(context.getName() + ": Execution service stopped");
        } 
        catch (InterruptedException e) 
        {
        	this.executorService.shutdownNow();
            Thread.currentThread().interrupt();
        
            stateLog.error(context.getName() + ": Execution service interrupted during shutdown", e);
            throw new TerminationException("Execution service interrupted during shutdown", e);
        }

		
		this.context.getEvents().unregister(this.asyncProgressListener);
		this.context.getEvents().unregister(this.certificateListener);
		this.context.getEvents().unregister(this.asyncAtomListener);
		this.context.getEvents().unregister(this.syncAtomListener);
		this.context.getEvents().unregister(this.syncBlockListener);
		this.context.getEvents().unregister(this.syncChangeListener);
		
		this.context.getNetwork().getMessaging().deregisterAll(this.getClass());
		
		this.substateRequestHandler.stop();
	}
	
	// PROCESSING PIPELINE //
	private void _checkSubstateReadRequests()
	{
		if (this.pendingSubstateRequests.isEmpty() == false) 
		{
			boolean hasQueuedStateInput = false;
			final Iterator<Entry<SubstateRequest, PendingAtom>> pendingSubstateRequestsIterator = this.pendingSubstateRequests.entries().iterator();
			while(pendingSubstateRequestsIterator.hasNext())
			{
				final Entry<SubstateRequest, PendingAtom> entry = pendingSubstateRequestsIterator.next();
				if (entry.getKey().isDone())
				{
					try
					{
						final Substate substate = entry.getKey().get();
       					final PendingAtom pendingAtom = entry.getValue();

       					// Provisioning completed via a different route, or failure occurred 
       					if (pendingAtom.getStatus().after(AtomStatus.State.PROVISIONING))
       						continue;
       						
                 		// Create a state input for this substate request
     					final StateInput stateInput = new StateInput(pendingAtom.getHash(), substate);
     					
     					if (stateLog.hasLevel(Logging.INFO))
     						stateLog.info(this.context.getName()+": Read remote state "+stateInput+" is completed for atom "+pendingAtom.getHash());

      					if (this.context.getLedger().getLedgerStore().store(stateInput).equals(OperationStatus.SUCCESS))
    					{
    						this.stateInputsToProcessQueue.put(stateInput.getHash(), stateInput);
    						hasQueuedStateInput = true;
    					}
    					else
    						stateLog.warn(this.context.getName()+": Failed to store state input "+stateInput);
       				}
					catch (Exception ex)
					{
						stateLog.error(this.context.getName()+": Error provisioning state input "+entry.getKey().getAddress()+" for pending atom "+entry.getValue().getHash(), ex);
					}
					finally
					{
   						pendingSubstateRequestsIterator.remove();
					}
				}
			}

			if (hasQueuedStateInput)
				this.stateProcessor.signal();
		}
	}
	
	private void _doSubstateProvisioning(final Epoch epoch, final int numShardGroups)
	{
		if (this.provisioningQueue.isEmpty() == false)
		{
			final ShardGroupID localShardGroupID = ShardMapper.toShardGroup(this.context.getNode().getIdentity(), numShardGroups);
			final List<PendingAtom> pendingAtomsToProvision = new ArrayList<PendingAtom>(Math.min(MAX_ATOMS_TO_PROVISION, provisioningQueue.size()));
			this.provisioningQueue.drainTo(pendingAtomsToProvision, MAX_ATOMS_TO_PROVISION);
			
			for (final PendingAtom pendingAtom : pendingAtomsToProvision)
			{
				if (this.context.getNode().isSynced() == false)
					break;

				try
				{
					pendingAtom.provision();

    				if (pendingAtom.thrown() != null)
					{
						if (stateLog.hasLevel(Logging.DEBUG))
							stateLog.debug(this.context.getName()+": Aborting local state provisioning for atom "+pendingAtom.getHash()+" as exception thrown");
						
						continue;
					}

    				// Provisioning of pending states
					for (final PendingState pendingState : pendingAtom.getStates())
					{
						final StateInput stateInput;
						
	    				// Determine which local or remote read state inputs are needed
						final ShardGroupID provisionShardGroupID = ShardMapper.toShardGroup(pendingState.getAddress(), numShardGroups);
	                	if (provisionShardGroupID.equals(localShardGroupID))
	                	{
	                		if (stateLog.hasLevel(Logging.INFO))
	                			stateLog.info(this.context.getName()+": Provisioning local state "+pendingState+" for atom "+pendingAtom.getHash());

	                		final Substate substate = this.context.getLedger().getLedgerStore().get(pendingState.getAddress());
        					if (stateLog.hasLevel(Logging.DEBUG))
        						stateLog.debug(this.context.getName()+": State "+pendingState.getAddress()+" in atom "+pendingAtom.getHash()+" was provisioned locally");

        					stateInput = new StateInput(pendingAtom.getHash(), substate);
        					if (this.context.getLedger().getLedgerStore().store(stateInput).equals(OperationStatus.SUCCESS))
        					{
	        					this.process(pendingAtom, stateInput);
        						if (stateLog.hasLevel(Logging.DEBUG))
        							stateLog.debug(this.context.getName()+": Processed locally provisioned state input "+stateInput.getHash()+" with for atom "+stateInput.getAtom());
	        		
        						if (pendingAtom.isPrepared() && pendingAtom.getState(stateInput.getAddress()).getStateLockMode().equals(StateLockMode.WRITE))
        							this.broadcast(pendingAtom, stateInput, epoch);
        					}
        					else
        						stateLog.warn(this.context.getName()+": Failed to store locally provisioned state input "+stateInput);
						}
	                	else if (pendingState.getStateLockMode().equals(StateLockMode.READ))
	                	{
	                		if (stateLog.hasLevel(Logging.INFO))
	                			stateLog.info(this.context.getName()+": Provisioning read remote state "+pendingState+" for atom "+pendingAtom.getHash());
	                		
	                		if (this.context.getNode().isProgressing() == false)
	                			continue;
	                		
	        				if (this.stateInputsToProcessQueue.contains(pendingState.getHash()) ||
	        					this.context.getLedger().getLedgerStore().has(pendingState.getHash(), StateInput.class))
	        				{
		                		if (stateLog.hasLevel(Logging.INFO))
		                			stateLog.info(this.context.getName()+": Read remote state "+pendingState+" is known for atom "+pendingAtom.getHash());
		                		continue;
	        				}
	        				
	        				final SubstateRequest substateRequest = this.substateRequestHandler.request(pendingState.getAddress()); //, pendingAtom.getHash());
	        				if (substateRequest.isDone() == true && substateRequest.isCancelled() == false && substateRequest.isCompletedExceptionally() == false)
	        				{
	        					final Substate substate = substateRequest.resultNow();
		     					
	        					if (stateLog.hasLevel(Logging.INFO))
		     						stateLog.info(this.context.getName()+": Read remote state "+pendingState+" is available for atom "+pendingAtom.getHash());

	                			// Create a state input for this substate request
	        					stateInput = new StateInput(pendingAtom.getHash(), substate);
	        					if (this.context.getLedger().getLedgerStore().store(stateInput).equals(OperationStatus.SUCCESS))
	        						this.stateInputsToProcessQueue.put(stateInput.getHash(), stateInput);
	        					else
	        						stateLog.warn(this.context.getName()+": Failed to store state input "+stateInput);
	        				}
	        				else
	        					this.pendingSubstateRequests.put(substateRequest, pendingAtom);
	                	}
					}
				}
				catch (Exception ex)
				{
					stateLog.error(this.context.getName()+": Error provisioning states for pending atom "+pendingAtom.getHash(), ex);
					
					// TODO review if allowing atoms that throw any exception on provisioning should be allowed to simply commit-timeout
					// 		or is it better to have a more explicit means of dealing with exceptions that might not be relevant to the 
					//		execution of state.
//								StateHandler.this.context.getEvents().post(new AtomExceptionEvent(pendingAtom, ex));
				}
			}
			if (this.provisioningQueue.isEmpty() == false)
				this.stateProcessor.signal();
		}
	}
	
	private void _processStateInputs(final Epoch epoch, final int numShardGroups)
	{
		if (this.stateInputsToProcessQueue.isEmpty() == false) 
		{
			Entry<Hash, StateInput> stateInputToProcess;
			while((stateInputToProcess = this.stateInputsToProcessQueue.peek()) != null)
			{
				if (this.context.getNode().isSynced() == false)
					break;
				
				try
				{
					final PendingAtom pendingAtom = this.context.getLedger().getAtomHandler().get(stateInputToProcess.getValue().getAtom());
					if (pendingAtom == null)
					{
						stateLog.warn(this.context.getName()+": Pending atom "+stateInputToProcess.getValue().getAtom()+" not found for state input "+stateInputToProcess.getKey());
						continue;
					}
	
					// TODO might be able to optimise this state input storage and subsequent gossip
					//		really should only be broadcasting local state inputs to remote replica sets not everything
					if (stateLog.hasLevel(Logging.INFO))
						stateLog.info(this.context.getName()+": Processing state input "+stateInputToProcess.getValue());
					
					process(pendingAtom, stateInputToProcess.getValue());
					if (stateLog.hasLevel(Logging.DEBUG))
						stateLog.debug(this.context.getName()+": Processed state input "+stateInputToProcess.getKey()+" with for atom "+pendingAtom.getHash());
						
					if (pendingAtom.isPrepared() && pendingAtom.getState(stateInputToProcess.getValue().getAddress()).getStateLockMode().equals(StateLockMode.WRITE))
						this.broadcast(pendingAtom, stateInputToProcess.getValue(), epoch);
				}
				catch (Exception ex)
				{
					stateLog.error(this.context.getName()+": Error processing state input "+stateInputToProcess.getValue(), ex);
				}
				finally
				{
					if (this.stateInputsToProcessQueue.remove(stateInputToProcess.getKey(), stateInputToProcess.getValue()) == false && this.context.getNode().isSynced())
						// FIXME sync state can initially flip/flop between ... annoying, so just throw these as warns for now (theres are in all queue handlers!)
						stateLog.warn(StateHandler.this.context.getName()+": State provisioning queue peek/remove failed for "+stateInputToProcess.getValue());
	//								throw new IllegalStateException("State input process peek/remove failed for "+stateInput.getValue());
				}
			}
		
			if (this.stateInputsToProcessQueue.isEmpty() == false)
				this.stateProcessor.signal();
		}
	}
	
	private void _processStateCertificates(final Epoch epoch, final int numShardGroups)
	{
		if (this.stateCertificatesToProcessQueue.isEmpty() == false) 
		{
			// Expensive to process and may be queued in "bursts" so would also like to break that up a little with batches
			final List<StateCertificate> stateCertificatesToProcess = this.stateCertificatesToProcessQueue.getMany(MAX_STATE_CERTIFICATES_TO_PROCESS, (v1, v2) -> (int) (v1.getHeight() - v2.getHeight())); // Oldest first
			for(final StateCertificate stateCertificateToProcess : stateCertificatesToProcess)
			{
				if (this.context.getNode().isSynced() == false)
					break;
				
				try
				{
					final StateVerificationRecord vcr = this.verificationCache.get(stateCertificateToProcess.getVoteMerkle());
					if (vcr == null)
					{
						// TODO doesnt test the audit for the merkle.
						final BLSPublicKey aggregatedPublicKey;
						if (BLS12381.SKIP_VERIFICATION == false)
						{
							final List<BLSPublicKey> signers = this.context.getLedger().getValidatorHandler().getKeys(stateCertificateToProcess.getSigners());
							aggregatedPublicKey = BLS12381.aggregatePublicKey(signers);
							
							if (stateLog.hasLevel(Logging.TRACE))
								stateLog.trace(this.context.getName()+": Verification info for state certificate "+stateCertificateToProcess.getAddress()+" "+stateCertificateToProcess.getVoteMerkle()+" "+aggregatedPublicKey.toString()+":"+Base58.toBase58(stateCertificateToProcess.getSignature().toByteArray()));
						}
						else
							aggregatedPublicKey = null;
						
						if (BLS12381.SKIP_VERIFICATION == false)
						{
							this.context.getMetaData().increment("ledger.pool.state.certificate.verifications");

							if (aggregatedPublicKey.equals(stateCertificateToProcess.getKey()) == false ||  
								aggregatedPublicKey.verify(stateCertificateToProcess.getVoteMerkle(), stateCertificateToProcess.getSignature()) == false)
							{
								// TODO Aggregated state certificate signature has failed.
								//      Is there any route forward here other than just discarding the state certificate and 
								//		hoping that another validator in the group has detected whichever the bad signature
								//		was and produced a state certificate with it excluded?
								stateLog.error(this.context.getName()+": Aggregated signature verification failed for state certificate "+stateCertificateToProcess);
								continue;
							}
							else
							{
								if (stateLog.hasLevel(Logging.TRACE))
									stateLog.trace(this.context.getName()+": Aggregated signature verification SUCCESS for state certificate "+stateCertificateToProcess);

								final StateVerificationRecord stateVerificationRecord = new StateVerificationRecord(stateCertificateToProcess.getVoteMerkle(), stateCertificateToProcess.getSigners(), stateCertificateToProcess.getKey(), stateCertificateToProcess.getSignature());
								this.verificationCache.put(stateCertificateToProcess.getVoteMerkle(), stateVerificationRecord);
							}
						}
					}
					else
						context.getMetaData().increment("ledger.pool.state.certificate.verifications.cached");

					final PendingAtom pendingAtom = this.context.getLedger().getAtomHandler().get(stateCertificateToProcess.getAtom());
					if (pendingAtom == null)
					{
						stateLog.warn(this.context.getName()+": Pending atom not found for state certificate "+stateCertificateToProcess.toString());
						continue;
					}
						
					this.process(pendingAtom, stateCertificateToProcess);
					if (stateLog.hasLevel(Logging.DEBUG))
						stateLog.debug(this.context.getName()+": Processed state certificate "+stateCertificateToProcess.getAddress()+" with for atom "+stateCertificateToProcess.getAtom());
						
					if (pendingAtom.isPrepared() && pendingAtom.getState(stateCertificateToProcess.getAddress()).getStateLockMode().equals(StateLockMode.WRITE))
						broadcast(pendingAtom, stateCertificateToProcess, epoch);
				}
				catch (Exception ex)
				{
					stateLog.error(this.context.getName()+": Error processing state certificate "+stateCertificateToProcess, ex);
				}
				finally
				{
					if (this.stateCertificatesToProcessQueue.remove(stateCertificateToProcess.getHash(), stateCertificateToProcess) == false && this.context.getNode().isSynced())
						stateLog.warn(this.context.getName()+": State certificate peek/remove failed for "+stateCertificateToProcess);
				}
			}
		}
		
		if (this.stateCertificatesToProcessQueue.isEmpty() == false)
			this.stateProcessor.signal();
	}

	// METHODS //
	private void broadcast(final PendingAtom pendingAtom, final StateInput stateInput, final Epoch epoch)
	{
		if (pendingAtom.getStatus().before(AtomStatus.State.PREPARED))
			throw new IllegalStateException("Pending atom "+pendingAtom.getHash()+" is "+pendingAtom.getStatus()+" for broadcast of state input "+stateInput.toString());
		
		final int numShardGroups = this.context.getLedger().numShardGroups(epoch);
		final ShardGroupID localShardGroupID = ShardMapper.toShardGroup(this.context.getNode().getIdentity(), numShardGroups);
		final ShardGroupID stateShardGroupID = ShardMapper.toShardGroup(stateInput.getAddress(), numShardGroups);
		
		if (localShardGroupID.equals(stateShardGroupID))
		{
			final Set<ShardGroupID> shardGroupIDs = ShardMapper.toShardGroups(pendingAtom, StateLockMode.WRITE, stateShardGroupID, numShardGroups);
			if (shardGroupIDs.isEmpty() == false)
				this.context.getNetwork().getGossipHandler().broadcast(stateInput, shardGroupIDs);
		}
		else
			this.context.getNetwork().getGossipHandler().broadcast(stateInput, localShardGroupID);
	}

	private void broadcast(final PendingAtom pendingAtom, final StateCertificate stateCertificate, final Epoch epoch)
	{
		if (pendingAtom.getStatus().before(AtomStatus.State.PREPARED))
			throw new IllegalStateException("Pending atom "+pendingAtom.getHash()+" is "+pendingAtom.getStatus()+" for broadcast of state certificate "+stateCertificate.toString());

		final int numShardGroups = this.context.getLedger().numShardGroups(epoch);
		final ShardGroupID localShardGroupID = ShardMapper.toShardGroup(this.context.getNode().getIdentity(), numShardGroups);
		final ShardGroupID stateShardGroupID = ShardMapper.toShardGroup(stateCertificate.getAddress(), numShardGroups);
		
		if (localShardGroupID.equals(stateShardGroupID))
		{
			final Set<ShardGroupID> shardGroupIDs = ShardMapper.toShardGroups(pendingAtom, StateLockMode.WRITE, stateShardGroupID, numShardGroups);
			if (shardGroupIDs.isEmpty() == false)
				this.context.getNetwork().getGossipHandler().broadcast(stateCertificate, shardGroupIDs);
		}
		else
			this.context.getNetwork().getGossipHandler().broadcast(stateCertificate, localShardGroupID);
	}

	private void process(final PendingAtom pendingAtom, final StateInput stateInput) throws StateMachinePreparationException
	{
		boolean postProvisionedEvent = pendingAtom.isProvisioned() == false;
		
		pendingAtom.provision(stateInput);
		this.context.getMetaData().increment("ledger.stateinput.processed");
		this.context.getMetaData().increment("ledger.stateinput.latency", stateInput.getAge(TimeUnit.MILLISECONDS));
		this.context.getMetaData().increment("ledger.pool.state.inputs");

		if (pendingAtom.isProvisioned() && postProvisionedEvent)
			this.context.getEvents().post(new AtomProvisionedEvent(pendingAtom));
	}

	private void process(final PendingAtom pendingAtom, final StateCertificate certificate) throws IOException, CryptoException, ValidationException
	{
		Objects.requireNonNull(pendingAtom, "Pending atom is null");
		Objects.requireNonNull(certificate, "State certificate is null");

		// Dont provision local state certificates as will be generated locally
		if (ShardMapper.equal(this.context.getLedger().numShardGroups(Epoch.from(certificate.getBlock())), certificate.getAddress(), this.context.getNode().getIdentity()) == false)
			pendingAtom.provision(certificate);
		
		this.context.getMetaData().increment("ledger.pool.state.certificates");
		this.context.getMetaData().increment("ledger.statecertificate.processed");
		this.context.getMetaData().increment("ledger.statecertificate.latency", certificate.getAge(TimeUnit.MILLISECONDS));

		tryFinalize(pendingAtom);
	}
	
	void provision(final PendingAtom pendingAtom)
	{
		Objects.requireNonNull(pendingAtom, "Pending atom for provisioning is null");
		
		if (stateLog.hasLevel(Logging.DEBUG))
		{
			final List<StateAddress> stateAddresses = pendingAtom.getStateAddresses(null);
			stateLog.debug(this.context.getName()+": Queueing state provisioning "+stateAddresses.size()+":"+stateAddresses+" for "+pendingAtom+" in block "+pendingAtom.getBlockHeader().getHash());
		}
		
		if (this.provisioningQueue.putIfAbsent(pendingAtom.getHash(), pendingAtom) != null)
			stateLog.warn(this.context.getName()+": State provisioning should be absent for "+pendingAtom.getHash());
		else
			this.stateProcessor.signal();
	}

	void execute(final PendingAtom pendingAtom)
	{
		try
		{
			pendingAtom.execute();
		} 
		finally
		{
			if (pendingAtom.getStatus().current(AtomStatus.State.FINALIZING))
				this.context.getEvents().post(new AtomExecutedEvent(pendingAtom));
		}
	}

	private boolean tryFinalize(final PendingAtom pendingAtom) throws IOException, CryptoException, ValidationException
	{
		// Don't build atom certificate from state certificates until executed
		if (pendingAtom.getStatus().current(AtomStatus.State.FINALIZING) == false)
			return false;

		if (pendingAtom.getCertificate() != null)
			return false;
		
		final AtomCertificate certificate  = pendingAtom.tryFinalize();
		if (certificate != null)
		{
			this.context.getEvents().post(new AtomCertificateEvent(certificate, pendingAtom));
			return true;
		}

		return false;
	}
	
	private void remove(final PendingAtom pendingAtom)
	{
		Objects.requireNonNull(pendingAtom, "Pending atom is null");
		
		if (pendingAtom.getStatus().before(AtomStatus.State.ACCEPTED))
			throw new IllegalStateException("Pending atom "+pendingAtom+" is not accepted");
		
		if (stateLog.hasLevel(Logging.DEBUG))
			stateLog.debug(this.context.getName()+": Removing states for "+pendingAtom+" in block "+pendingAtom.getBlockHeader().getHash());
		
		pendingAtom.forStateAddresses(null, stateAddress -> {
			if(stateLog.hasLevel(Logging.DEBUG))
				stateLog.debug(this.context.getName()+": Removed pending state "+stateAddress+" for "+pendingAtom.getHash()+" in block "+pendingAtom.getBlockHeader().getHash());
			
			// TODO these dont increment here because pending atom status is not set yet!
			if (pendingAtom.getCertificate() != null && pendingAtom.getCertificate().getDecision().equals(CommitDecision.ACCEPT))
				this.context.getMetaData().increment("ledger.pool.state.committed");
			else if (pendingAtom.getCertificate() != null && pendingAtom.getCertificate().getDecision().equals(CommitDecision.REJECT))
				this.context.getMetaData().increment("ledger.pool.state.rejected");
			this.context.getMetaData().increment("ledger.pool.state.removed");
		});

		this.provisioningQueue.remove(pendingAtom.getHash(), pendingAtom);
	}

	// PARTICLE CERTIFICATE LISTENER //
	private SynchronousEventListener certificateListener = new SynchronousEventListener()
	{
		@Subscribe
		public void on(final StateCertificateConstructedEvent stateCertificateEvent)
		{
			if (stateLog.hasLevel(Logging.INFO))
				stateLog.info(StateHandler.this.context.getName()+": Constructed local state certificate "+stateCertificateEvent.getCertificate().getHash()+" for "+stateCertificateEvent.getCertificate().getAddress()+" in atom "+stateCertificateEvent.getCertificate().getAtom());
			
			// Local replica can trust state certificates it has created.  
			// Use the deterministic properties to speed up verification of received state certificates that share signatures and merkle votes
			if (BLS12381.SKIP_VERIFICATION == false)
			{
				StateHandler.this.verificationCache.computeIfAbsent(stateCertificateEvent.getCertificate().getVoteMerkle(), s -> {
					return new StateVerificationRecord(stateCertificateEvent.getCertificate().getVoteMerkle(), stateCertificateEvent.getCertificate().getSigners(), stateCertificateEvent.getCertificate().getKey(), stateCertificateEvent.getCertificate().getSignature());
				});
			}
			
			try
			{
				if (StateHandler.this.context.getLedger().getLedgerStore().store(stateCertificateEvent.getCertificate()).equals(OperationStatus.SUCCESS))
				{
					if (stateLog.hasLevel(Logging.DEBUG))
						stateLog.debug(StateHandler.this.context.getName()+": Stored locally produced state certificate "+stateCertificateEvent.getCertificate().getHash()+" for atom "+stateCertificateEvent.getCertificate().getAtom());

					StateHandler.this.stateCertificatesToProcessQueue.put(stateCertificateEvent.getCertificate().getHash(), stateCertificateEvent.getCertificate());
				}
				else
					stateLog.warn(StateHandler.this.context.getName()+": Failed to store locally produced state certificate "+stateCertificateEvent.getCertificate().getHash()+" for atom "+stateCertificateEvent.getCertificate().getAtom());
			}
			catch (Exception ex)
			{
				stateLog.error(StateHandler.this.context.getName()+": Error processing locally produced state certificate "+stateCertificateEvent.getCertificate(), ex);
			}

			StateHandler.this.stateProcessor.signal();
		}
	};
		
	// SYNC ATOM LISTENER //
	private SynchronousEventListener syncAtomListener = new SynchronousEventListener()
	{
		@Subscribe
		public void on(final AtomExceptionEvent event) throws IOException 
		{
//			remove(event.getPendingAtom());
		}
	};

	// ASYNC ATOM LISTENER //
	private EventListener asyncAtomListener = new EventListener()
	{
		@Subscribe
		public void on(final AtomPreparedEvent event)
		{
			final Epoch epoch = StateHandler.this.context.getLedger().getEpoch();
			
			// Broadcast any StateCertificate / StateInputs received early
			event.getPendingAtom().forStates(StateLockMode.WRITE, ps -> {
				final StateInput stateInput = ps.getStateInput();
				if (stateInput != null)
					StateHandler.this.broadcast(event.getPendingAtom(), stateInput, epoch);
				
				final StateOutput stateOutput = ps.getStateOutput();
				if (stateOutput instanceof StateCertificate stateCertificate)
					StateHandler.this.broadcast(event.getPendingAtom(), stateCertificate, epoch);
			});
		}
		
		@Subscribe
		public void on(final AtomAcceptedEvent event)
		{
			// Provision accepted atom
			PendingAtom pendingAtom = event.getPendingAtom();
			if (pendingAtom.getStatus().before(AtomStatus.State.ACCEPTED))
				throw new IllegalStateException(StateHandler.this.context.getName()+": Pending atom "+pendingAtom.getHash()+" accepted in block "+pendingAtom.getBlockHeader().getHash()+" is before ACCEPTED state");
				
			if (stateLog.hasLevel(Logging.DEBUG))
				stateLog.debug(StateHandler.this.context.getName()+": Queuing pending atom "+pendingAtom.getHash()+" for provisioning");
           		
       		provision(pendingAtom);
		}
		
		@Subscribe
		public void on(final AtomCommitEvent event)
		{
			remove(event.getPendingAtom());
		}

		@Subscribe
		public void on(final AtomExecutionTimeoutEvent event) 
		{
			remove(event.getPendingAtom());
		}

		@Subscribe
		public void on(final AtomCommitTimeoutEvent event)
		{
			remove(event.getPendingAtom());
		}
		
		@Subscribe
		public void on(final AtomProvisionedEvent event) 
		{
			// Atoms may be signalled executable BEFORE they are provisioned in certain edge cases such as the desync / sync sequence
			if (event.getPendingAtom().isExecuteSignalled() && event.getPendingAtom().isExecuted() == false)
				queueForExecution(event.getPendingAtom());
		}

		@Subscribe
		public void on(final AtomExecutableEvent event)
		{
			if (event.getPendingAtom().isProvisioned() && event.getPendingAtom().isExecuted() == false)
				queueForExecution(event.getPendingAtom());
		}
		
		private void queueForExecution(PendingAtom pendingAtom)
		{
			if (StateHandler.this.executionQueue.offer(pendingAtom))
			{
				if (stateLog.hasLevel(Logging.DEBUG))
					stateLog.debug(StateHandler.this.context.getName()+": Queuing pending atom "+pendingAtom.getHash()+" for execution");
			}
			else
				stateLog.debug(StateHandler.this.context.getName()+": Failed to queue pending atom "+pendingAtom.getHash()+" for execution, will timeout");

		}
	};

	// SYNC BLOCK LISTENER //
	private SynchronousEventListener syncBlockListener = new SynchronousEventListener()
	{
		@Subscribe
		public void on(final BlockCommittedEvent blockCommittedEvent) 
		{
			// Clean up queues and delayed //
			// TODO This isn't the ideal place for this operation.  it was originally in remove(PendingAtom) but was very expensive.
			//		Performing here prior to actually processing the block and the various atom events being fired is also a little nasty
			//		as it relies on the fact that AtomHandler, which actually does the removal of the PendingAtoms is called LAST 
			//		in the BlockCommittedEvent sequence.  If that were to change, this would likely break, or at the least produce
			//		a lot of warnings.
			InventoryType[] checkTypes = {InventoryType.UNACCEPTED, InventoryType.UNEXECUTED, InventoryType.UNCOMMITTED, InventoryType.COMMITTED};

			final List<Hash> certificateRemovals = new ArrayList<Hash>();
			StateHandler.this.stateCertificatesToProcessQueue.forEach((h,sc) -> {
				if (blockCommittedEvent.getPendingBlock().contains(sc.getAtom(), checkTypes))
					certificateRemovals.add(h);
			});
			StateHandler.this.stateCertificatesToProcessQueue.removeAll(certificateRemovals);
			
			final List<Hash> inputRemovals = new ArrayList<Hash>();
			StateHandler.this.stateInputsToProcessQueue.forEach((h,si) -> {
				if (blockCommittedEvent.getPendingBlock().contains(si.getAtom(), checkTypes))
					inputRemovals.add(h);
			});
			StateHandler.this.stateInputsToProcessQueue.removeAll(inputRemovals);
		}
	};

	private EventListener asyncProgressListener = new EventListener() 
	{
		@Subscribe
		public void on(final ProgressPhaseEvent event)
		{
			StateHandler.this.stateProcessor.signal();
		}
	};

	// SYNC CHANGE LISTENER //
	private SynchronousEventListener syncChangeListener = new SynchronousEventListener()
	{
		@Subscribe
		public void on(final SyncAcquiredEvent event)
		{
			stateLog.log(StateHandler.this.context.getName()+": Sync status acquired, preparing state handler");
			StateHandler.this.executionQueue.clear();
			StateHandler.this.provisioningQueue.clear();
			StateHandler.this.stateInputsToProcessQueue.clear();
			StateHandler.this.stateCertificatesToProcessQueue.clear();
			StateHandler.this.verificationCache.clear();
			StateHandler.this.pendingSubstateRequests.clear();
			
			final Map<Hash, PendingAtom> atomRecoveryMap = event.getAtoms().stream().collect(Collectors.toMap(pa -> pa.getHash(), pa -> pa));

			// Recover any state inputs known prior to OOS (need to see back further due to optimistic processing) 
			for (PendingAtom pendingAtom : atomRecoveryMap.values())
			{
				// Pending atoms should ONLY be in a PROVISIONING state here
				if (pendingAtom.getStatus().current(AtomStatus.State.ACCEPTED) == false)
					throw new IllegalStateException("Pending atom "+pendingAtom.getHash()+" is "+pendingAtom.getStatus().current()+" but must be ACCEPTED");
				
				provision(pendingAtom);
			}
		}
		
		@Subscribe
		public void on(final SyncLostEvent event) 
		{
			stateLog.log(StateHandler.this.context.getName()+": Sync status lost, flushing state handler");
			StateHandler.this.executionQueue.clear();
			StateHandler.this.provisioningQueue.clear();
			StateHandler.this.stateInputsToProcessQueue.clear();
			StateHandler.this.stateCertificatesToProcessQueue.clear();
			StateHandler.this.verificationCache.clear();
			StateHandler.this.pendingSubstateRequests.clear();
		}
	};
}	
