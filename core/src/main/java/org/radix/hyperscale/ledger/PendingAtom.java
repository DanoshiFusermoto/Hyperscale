package org.radix.hyperscale.ledger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.eclipse.collections.api.factory.Maps;
import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.map.MutableMap;
import org.radix.hyperscale.Constants;
import org.radix.hyperscale.Context;
import org.radix.hyperscale.Universe;
import org.radix.hyperscale.crypto.CryptoException;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.crypto.Hashable;
import org.radix.hyperscale.exceptions.ValidationException;
import org.radix.hyperscale.ledger.AtomStatus.State;
import org.radix.hyperscale.ledger.primitives.Atom;
import org.radix.hyperscale.ledger.primitives.AtomCertificate;
import org.radix.hyperscale.ledger.primitives.StateCertificate;
import org.radix.hyperscale.ledger.primitives.StateInput;
import org.radix.hyperscale.ledger.primitives.StateOutput;
import org.radix.hyperscale.ledger.sme.ManifestException;
import org.radix.hyperscale.ledger.sme.Package;
import org.radix.hyperscale.ledger.sme.PolyglotPackage;
import org.radix.hyperscale.ledger.sme.StateMachine;
import org.radix.hyperscale.ledger.sme.exceptions.StateMachineExecutionException;
import org.radix.hyperscale.ledger.sme.exceptions.StateMachinePreparationException;
import org.radix.hyperscale.ledger.timeouts.AcceptTimeout;
import org.radix.hyperscale.ledger.timeouts.AtomTimeout;
import org.radix.hyperscale.ledger.timeouts.CommitTimeout;
import org.radix.hyperscale.ledger.timeouts.ExecutionLatentTimeout;
import org.radix.hyperscale.ledger.timeouts.ExecutionTimeout;
import org.radix.hyperscale.ledger.timeouts.PrepareTimeout;
import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;
import org.radix.hyperscale.time.Time;

/** 
 * Represents an Atom that is currently being processed.
 * <br><br>
 * Also acts as a StateMachine wrapper.
 */
public final class PendingAtom implements Hashable, StateAddressable
{
	private static final Logger atomsLog = Logging.getLogger("atoms");
	private static final Logger atomStatusLog = Logging.getLogger("atomstatus");
	private static final Logger stateLog = Logging.getLogger("state");

	private final Context 	context;

	private final Hash			hash;
	private final AtomStatus	status;
	private final StateAddress 	address;

	private volatile Atom 	atom;
	
	/** The wall clock timestamp when the Atom primitive was witnessed for this pending atom **/
	private	volatile long 	witnessedAt;

	/** The wall clock timeout if not prepared WARN subjective **/
	private	volatile long 	prepareTimeoutAt;

	/** The wall clock timestamp when the pending atom was agreed by all shard group validators to be included in a block **/
	private	volatile long 	acceptableAt;
	
	/** The wall clock timestamp when the pending atom was accepted into a proposal **/
	private	volatile long 	acceptedAt;

	/** The wall clock timeout if not accepted into a proposal WARN subjective **/
	private	volatile long 	acceptTimeoutAt;
	
	/** The proposal of an execution signal **/
	private	volatile Hash 	executeSignalledBlock;
	
	/** The proposal of a latent execution signal **/
	private	volatile Hash 	executeLatentSignalledBlock;
	
	/** The wall clock timeout at which an execution latent signal happens **/
	private	volatile long 	executeLatentAt; 

	/** The wall clock timeout at which an execution timeout happens **/
	private	volatile long 	executeTimeoutAt; 

	/** The wall clock timeout at which a commit timeout happens **/
	private	volatile long 	commitTimeoutAt;
	
	private volatile BlockHeader  block;
	private volatile StateMachine stateMachine;
	
	// TODO NEED UNPREPARED TIME OUT
	
	private final MutableMap<Hash, StateVoteBlock> stateVoteBlocks;
	private final MutableMap<StateAddress, PendingState> pendingStates;
	
	private volatile AtomTimeout timeout;
	private volatile AtomCertificate certificate;

	// Faulty
	private boolean forcePrepareTimeout = false;
	private boolean forceAcceptTimeout = false;
	private boolean forceCommitTimeout = false;
	private boolean forceLatentExecution = false;
	private boolean forceExecutionTimeout = false;
	
	PendingAtom(final Context context, final Hash hash) throws ManifestException
	{
		this(context, hash, null, -1);
	}

	// TODO visible for testing
	public PendingAtom(final Context context, final Atom atom) throws ManifestException
	{
		this(context, atom, Time.getSystemTime());
	}

	PendingAtom(final Context context, final Atom atom, final long witnessedAt) throws ManifestException
	{
		this(context, atom.getHash(), atom, witnessedAt);
	}

	private PendingAtom(final Context context, final Hash hash, final Atom atom, final long witnessedAt) throws ManifestException
	{
		this.context = Objects.requireNonNull(context, "Context is null");
		
		this.hash = Objects.requireNonNull(hash, "Hash is null");
		Hash.notZero(hash, "Hash is zero");
		
		this.address = StateAddress.from(Atom.class, hash);
		
		if (atom == null && witnessedAt != -1)
			throw new IllegalArgumentException("Can not specify a witnessed timestamp if Atom is null");
		
		this.witnessedAt = witnessedAt;
		this.prepareTimeoutAt = (this.witnessedAt == -1 ? Time.getSystemTime() : this.witnessedAt) + TimeUnit.SECONDS.toMillis(Constants.ATOM_PREPARE_TIMEOUT_SECONDS);

		this.acceptedAt = -1;
		this.acceptableAt = -1;
		
		this.executeSignalledBlock = Hash.ZERO;
		this.executeLatentSignalledBlock = Hash.ZERO;
		this.executeLatentAt = -1;
		this.executeTimeoutAt = -1;
		this.commitTimeoutAt= -1;
		this.pendingStates = Maps.mutable.<StateAddress, PendingState>ofInitialCapacity(4).asSynchronized();
		this.stateVoteBlocks = Maps.mutable.<Hash, StateVoteBlock>ofInitialCapacity(4).asSynchronized();
		
		this.status = new AtomStatus(context, hash);

		if (this.context.getConfiguration().get("ledger.faults.force.atom.timeout.commit.interval", 0l) > 0 && 
			hash.asLong() % this.context.getConfiguration().get("ledger.faults.force.atom.timeout.commit.interval", 0l) == 0)
		{
			this.forceCommitTimeout = true;
			atomsLog.warn(this.context.getName()+": Atom "+getHash()+" is forcing commit timeout");
		}
		else if (this.context.getConfiguration().get("ledger.faults.force.atom.timeout.execution.interval", 0l) > 0 && 
				 hash.asLong() % this.context.getConfiguration().get("ledger.faults.force.atom.timeout.execution.interval", 0l) == 0)
		{
			this.forceExecutionTimeout = true;
			atomsLog.warn(this.context.getName()+": Atom "+getHash()+" is forcing execution timeout");
		}
		else if (this.context.getConfiguration().get("ledger.faults.force.atom.latency.execution.interval", 0l) > 0 && 
				 hash.asLong() % this.context.getConfiguration().get("ledger.faults.force.atom.latency.execution.interval", 0l) == 0)
		{
			this.forceLatentExecution = true;
			atomsLog.warn(this.context.getName()+": Atom "+getHash()+" is forcing latent execution");
		}
		else if (this.context.getConfiguration().get("ledger.faults.force.atom.timeout.accept.interval", 0l) > 0 && 
				 hash.asLong() % this.context.getConfiguration().get("ledger.faults.force.atom.timeout.accept.interval", 0l) == 0)
		{
			this.forceAcceptTimeout = true;
			atomsLog.warn(this.context.getName()+": Atom "+getHash()+" is forcing accept timeout");
		}
		else if (this.context.getConfiguration().get("ledger.faults.force.atom.timeout.prepare.interval", 0l) > 0 && 
				 hash.asLong() % this.context.getConfiguration().get("ledger.faults.force.atom.timeout.prepare.interval", 0l) == 0)
		{
			this.forcePrepareTimeout = true;
			atomsLog.warn(this.context.getName()+": Atom "+getHash()+" is forcing prepare timeout");
		}

		if (atom != null) 
			setAtom(atom);
	}

	@Override
	public Hash getHash()
	{
		return this.hash;
	}
	
	@Override
	public StateAddress getAddress()
	{
		return this.address;
	}
	
	long getExecuteLatentAt()
	{
		return this.executeLatentAt;
	}

	long getExecuteTimeoutAt()
	{
		return this.executeTimeoutAt;
	}

	long getCommitTimeoutAt()
	{
		return this.commitTimeoutAt;
	}

	public long getWitnessedAt()
	{
		return this.witnessedAt;
	}

	long getPrepareTimeoutAt()
	{
		return this.prepareTimeoutAt;
	}

	public long getAcceptableAt()
	{
		return this.acceptableAt;
	}

	long getAcceptTimeoutAt()
	{
		return this.acceptTimeoutAt;
	}

	public long getAcceptedAt()
	{
		return this.acceptedAt;
	}

	public Atom getAtom()
	{
		return this.atom;
	}
			
	void setAtom(final Atom atom) throws ManifestException
	{
		Objects.requireNonNull(atom, "Atom is null");
		if (this.hash.equals(atom.getHash()) == false)
			throw new IllegalArgumentException("Atom hash "+atom.getHash()+" does not match expected hash "+this.hash);
		
		synchronized(this)
		{
			if (this.atom != null)
				throw new IllegalStateException("Atom primitive is already set for "+this.hash);
		
			this.atom = atom;
			this.witnessedAt = Time.getSystemTime();
			
			// Accept timeout is a baseline constant scaled log(quantity of instructions).
			// More instructions generally means more shards are touched, therefore more latency for 
			// all required shard groups to be ready to accept if there is some state contention.
			double additionalTimeout = Math.log(atom.getManifest().size()) * Constants.ATOM_ACCEPT_TIMEOUT_SECONDS;
			this.acceptTimeoutAt = this.witnessedAt + TimeUnit.SECONDS.toMillis((long) (Constants.ATOM_ACCEPT_TIMEOUT_SECONDS + additionalTimeout));
			if (this.context.getConfiguration().get("ledger.atompool", Boolean.TRUE) == Boolean.FALSE)
				this.acceptableAt = this.witnessedAt;

		}
	}
	
	public boolean isGlobal()
	{
		if (this.status.before(AtomStatus.State.PREPARED))
			throw new IllegalStateException("Pending atom "+getHash()+" is not PREPARED but "+this.status);
		
		return this.stateMachine.isGlobal();
	}
	
	public void forShards(final StateLockMode lockMode, final BiConsumer<StateAddress, ShardID> consumer)
	{
		if (this.status.before(AtomStatus.State.PREPARED))
			throw new IllegalStateException("Pending atom "+getHash()+" is not PREPARED but "+this.status);
		
		synchronized(this.pendingStates)
		{
			if (this.pendingStates.isEmpty())
				throw new IllegalStateException("Pending states for atom "+getHash()+" is empty"); 
			
			this.pendingStates.forEachWith((ps, p) -> {
				if (p != null && ps.getStateLockMode().equals(p) == false)
					return;
						
				consumer.accept(ps.getAddress(), ps.getAddress().getShardID());
			}, lockMode);
		}
	}
	
	public Set<ShardID> getShards(final StateLockMode lockMode)
	{
		if (this.status.before(AtomStatus.State.PREPARED))
			throw new IllegalStateException("Pending atom "+getHash()+" is not PREPARED but "+this.status);

		synchronized(this.pendingStates)
		{
			final Set<ShardID> shardIDs = Sets.mutable.<ShardID>ofInitialCapacity(this.pendingStates.size());
			this.pendingStates.collectIf(ps -> {
				if (lockMode != null && ps.getStateLockMode().equals(lockMode) == false)
					return false;
				
				return true;
			}, ps -> ps.getAddress().getShardID(), shardIDs);

			return Collections.unmodifiableSet(shardIDs);
		}
	}

	public Set<PendingState> getStates()
	{
		return getStates(null);
	}
	
	public Set<PendingState> getStates(final StateLockMode lockMode)
	{
		if (this.status.before(AtomStatus.State.PREPARED))
			throw new IllegalStateException("Pending atom "+getHash()+" is not PREPARED but "+this.status);

		synchronized(this.pendingStates)
		{
			if (this.pendingStates.isEmpty())
				throw new IllegalStateException("Pending states for "+getHash()+" is empty");
				
			final Set<PendingState> pendingStates = Sets.mutable.<PendingState>ofInitialCapacity(this.pendingStates.size());
			this.pendingStates.selectWith((ps, p) -> {
				if (p!= null && ps.getStateLockMode().equals(p) == false)
					return false;
				
				return true;
			}, lockMode, pendingStates);

			return Collections.unmodifiableSet(pendingStates);
		}
	}
	
	public void forStates(final StateLockMode lockMode, final Consumer<PendingState> consumer)
	{
		if (this.status.before(AtomStatus.State.PREPARED))
			throw new IllegalStateException("Pending atom "+getHash()+" is not PREPARED but "+this.status);
	
		synchronized(this.pendingStates)
		{
			if (this.pendingStates.isEmpty())
				throw new IllegalStateException("Pending states for "+getHash()+" is empty"); 
				
			this.pendingStates.forEachWith((ps, p) -> {
				if (p != null && ps.getStateLockMode().equals(p) == false)
					return;
					
				consumer.accept(ps);
			}, lockMode);
		}
	}

	public PendingState getState(final StateAddress stateAddress)
	{
		if (this.status.before(AtomStatus.State.PREPARED))
			throw new IllegalStateException("Pending atom "+getHash()+" is not PREPARED but "+this.status);
			
		return this.pendingStates.get(stateAddress);
	}
	
	public void forStateAddresses(final StateLockMode lockMode, final Consumer<StateAddress> consumer)
	{
		if (this.status.before(AtomStatus.State.PREPARED))
			throw new IllegalStateException("Pending atom "+getHash()+" is not PREPARED but "+this.status);
	
		synchronized(this.pendingStates)
		{
			if (this.pendingStates.isEmpty())
				throw new IllegalStateException("Pending states for "+getHash()+" is empty"); 
				
			this.pendingStates.forEach(ps -> {
				if (lockMode != null && ps.getStateLockMode().equals(lockMode) == false)
					return;
				
				consumer.accept(ps.getAddress());
			});
		}
	}

	public List<StateAddress> getStateAddresses(final StateLockMode lockMode)
	{
		if (this.status.before(AtomStatus.State.PREPARED))
			throw new IllegalStateException("Pending atom "+getHash()+" is not PREPARED but "+this.status);

		synchronized(this.pendingStates)
		{
			if (this.pendingStates.isEmpty())
				throw new IllegalStateException("Pending states for "+getHash()+" is empty"); 
			
			final List<StateAddress> stateAddresses = new ArrayList<StateAddress>(this.pendingStates.size());
			this.pendingStates.collectIf(ps -> {
				if (lockMode != null && ps.getStateLockMode().equals(lockMode) == false)
					return false;
				
				return true;
			}, ps -> ps.getAddress(), stateAddresses);

			return Collections.unmodifiableList(stateAddresses);
		}
	}
	
	public int numStateAddresses(final StateLockMode lockMode)
	{
		if (this.status.before(AtomStatus.State.PREPARED))
			throw new IllegalStateException("Pending atom "+getHash()+" is not PREPARED but "+this.status);

		synchronized(this.pendingStates)
		{
			if (this.pendingStates.isEmpty())
				throw new IllegalStateException("Pending states for "+getHash()+" is empty");
			
			if (lockMode == null)
				return this.pendingStates.size();
			
			return this.pendingStates.countWith((ps,p) -> ps.getStateLockMode().equals(p), lockMode);
		}
	}

	public List<Object> getInstructions()
	{
		return this.stateMachine.getInstructions();
	}

	public <T> List<T> getInstructions(final Class<T> type)
	{
		return this.stateMachine.getInstructions(type);
	}

	public boolean isForcePrepareTimeout()
	{
		return this.forcePrepareTimeout;
	}
	
	void prepare() throws IOException, StateMachinePreparationException, ManifestException
	{
		if (this.forcePrepareTimeout)
			return;
		
		synchronized(this)
		{
			if (this.atom == null)
				throw new IllegalStateException("Atom is null");
			
			if (this.status.after(State.NONE))
				throw new IllegalStateException("Atom is already PREPARED");

			this.stateMachine = new StateMachine(this.context, this, this.pendingStates);
			this.stateMachine.prepare();
			
			this.prepareTimeoutAt = -1;
			this.status.set(AtomStatus.State.PREPARED);
			
			if (atomsLog.hasLevel(Logging.INFO))
			{
				final Set<ShardGroupID> shardGroupIDs;
				if (this.stateMachine.isGlobal())
					shardGroupIDs = ShardMapper.toAllShardGroups(this.context.getLedger().numShardGroups(this.context.getLedger().getEpoch()));
				else
					shardGroupIDs = ShardMapper.toShardGroups(getShards(StateLockMode.WRITE), this.context.getLedger().numShardGroups(this.context.getLedger().getEpoch()));

				atomsLog.info(this.context.getName()+": Pending atom "+getHash()+" is prepared for shard group IDs "+shardGroupIDs.toString());
				if (atomsLog.hasLevel(Logging.DEBUG))
				{
					for (String manifestItem : this.atom.getManifest())
					{
						if (manifestItem.length() > 256)
							atomsLog.debug(this.context.getName()+":    "+manifestItem.substring(0, 256)+" ...");
						else
							atomsLog.debug(this.context.getName()+":    "+manifestItem);
					}
				}
			}
		} 
	}

	void accepted(final BlockHeader header) throws IOException
	{
		Objects.requireNonNull(header, "Block header is null");
		
		synchronized(this)
		{
			this.block = header;
			this.stateMachine.accepted(this.block);
			this.status.set(AtomStatus.State.ACCEPTED);

			this.acceptedAt = header.getTimestamp();
			this.acceptTimeoutAt = -1;
			this.executeLatentAt = this.acceptedAt + TimeUnit.SECONDS.toMillis(Constants.ATOM_EXECUTE_LATENT_SECONDS);
			this.executeTimeoutAt = this.acceptedAt + TimeUnit.SECONDS.toMillis(Constants.ATOM_EXECUTE_TIMEOUT_SECONDS);
			
			clearTimeout();
		}
	}
	
	void provisioned()
	{
		synchronized(this)
		{
			this.status.set(AtomStatus.State.PROVISIONED);

			if (atomsLog.hasLevel(Logging.INFO))
				atomsLog.info(this.context.getName()+": Pending atom "+this.getHash()+" is provisioned");
		}
	}

	void completed()
	{
		synchronized(this)
		{
			boolean canComplete = this.status.thrown() != null || this.certificate != null || this.timeout != null;
			
			if (canComplete == false)
				throw new IllegalStateException("Pending atom "+this.hash+" state can not be set as COMPLETED without an AtomCertificate, AtomTimeout or thrown exception");

			this.prepareTimeoutAt = -1;
			this.acceptTimeoutAt = -1;
			this.executeLatentAt = -1;
			this.executeTimeoutAt = -1;
			this.commitTimeoutAt = -1;
			this.status.set(AtomStatus.State.COMPLETED);
			
			if (atomsLog.hasLevel(Logging.INFO))
			{
				final String completedVia;
				if (this.certificate != null)
					completedVia = "via certificate "+this.certificate.getHash();
				else if (this.timeout != null)
					completedVia = "via timeout "+this.timeout.getClass().getSimpleName()+":"+this.timeout.getHash();
				else if (this.status.thrown() != null)
					completedVia = "via exception "+this.status.thrown().getClass().getName()+":"+this.status.thrown().getMessage();
				else
					completedVia = ": UNKNOWN COMPLETION CONDITIONAL";
				
				atomsLog.info(this.context.getName()+": Pending atom "+this.getHash()+" is completed "+completedVia);
			}
			
			clearTimeout();
		}
	}

	boolean isCompleted()
	{
		return this.status.current(AtomStatus.State.COMPLETED);
	}

	boolean isAccepted()
	{
		return this.status.after(AtomStatus.State.PREPARED);
	}

	boolean isPrepared()
	{
		return this.status.after(AtomStatus.State.NONE);
	}

	void load(final Package pakage) 
	{
		Objects.requireNonNull(pakage, "Package is null");

		synchronized(this)
		{
			if (isProvisioned())
				throw new IllegalStateException("Atom "+getHash()+" is already PROVISIONED when loading package "+pakage);
			
			this.stateMachine.load(pakage);
			if (this.stateMachine.isProvisioned() && isProvisioned() == false)
				provisioned();
		}
	}

	Collection<PolyglotPackage> load(final Collection<PolyglotPackage> pakages, boolean throwIfNotRequired) 
	{
		Objects.requireNonNull(pakages, "Packages is null");

		synchronized(this)
		{
			if (isProvisioned())
				throw new IllegalStateException("Atom "+getHash()+" is already PROVISIONED when loading packages "+pakages.stream().map(p -> p.getAddress()).collect(Collectors.toList()));
			
			final Collection<PolyglotPackage> loadedPackages = this.stateMachine.load(pakages, throwIfNotRequired);
			if (loadedPackages.isEmpty() == false && this.stateMachine.isProvisioned() && isProvisioned() == false)
				provisioned();
			
			return loadedPackages;
		}
	}

	boolean isProvisioned()
	{
		return this.status.after(AtomStatus.State.PROVISIONING);
	}
	
	boolean isProvisioned(StateAddress stateAddress)
	{
		Objects.requireNonNull(stateAddress, "State address is null");

		synchronized(this)
		{
			return this.stateMachine.isProvisioned(stateAddress);
		}
	}

	void provision() throws StateMachinePreparationException
	{
		synchronized(this)
		{
			// Call the state machine to provision any state inputs received early
			synchronized(this.pendingStates)
			{
				for (final PendingState pendingState : this.pendingStates.values())
				{
					final StateInput stateInput = pendingState.getStateInput();
					if (stateInput == null)
						continue;
	
					this.stateMachine.provision(pendingState.getStateInput());
				}
			}
			
			this.status.set(AtomStatus.State.PROVISIONING);
			
			if (atomsLog.hasLevel(Logging.INFO))
				atomsLog.info(this.context.getName()+": Pending atom "+getHash()+" is provisioning");

			// Check provisioned in the case we had all the state inputs available (global execution)
			if (this.stateMachine.isProvisioned() && isProvisioned() == false)
				provisioned();
		}
	}
	
	void provision(final StateInput stateInput) throws StateMachinePreparationException
	{
		Objects.requireNonNull(stateInput, "State input is null");
		if (stateInput.getAtom().equals(this.hash) == false)
			throw new IllegalArgumentException("State input "+stateInput+" does not reference atom "+this.hash);
		
		synchronized(this)
		{
			if (this.status.after(AtomStatus.State.PROVISIONING))
				throw new IllegalStateException("Pending atom "+getHash()+" is already PROVISIONED "+this.status);
			
			final PendingState pendingState = this.pendingStates.computeIfAbsent(stateInput.getAddress(), sa -> new PendingState(this.context, sa, this));
			pendingState.setStateInput(stateInput);
			
			if (this.status.current().equals(AtomStatus.State.PROVISIONING))
			{
				this.stateMachine.provision(stateInput);
				if (this.stateMachine.isProvisioned() && isProvisioned() == false)
					provisioned();
			}
		}
	}
	
	boolean hasInput(final StateInput input)
	{
		return this.pendingStates.containsKey(input.getSubstate().getAddress());
	}
	
	boolean hasInput(final StateAddress stateAddress)
	{
		return this.pendingStates.containsKey(stateAddress);
	}

	StateInput getInput(final StateAddress stateAddress)
	{
		return this.pendingStates.get(stateAddress).getStateInput();
	}

	void execute()
	{
		synchronized(this)
		{
			try
			{
				if (this.status.thrown() != null)
					throw new IllegalStateException("Detected thrown exception for pending atom "+getHash()+" when executing");
	
				if (this.status.current(AtomStatus.State.PROVISIONED) == false) 
					throw new IllegalStateException("Pending atom "+this.getHash()+" is not PROVISIONED but "+this.status);
		
				this.status.set(AtomStatus.State.EXECUTING);
				this.stateMachine.execute();
				this.status.set(AtomStatus.State.FINALIZING);
				
				this.executeLatentAt = -1;
				this.executeTimeoutAt = -1;
				clearTimeout();
				
				if (atomsLog.hasLevel(Logging.INFO))
					atomsLog.info(this.context.getName()+": Atom "+getHash()+" is executed");

				// Finalize the state references of read only substates
				synchronized(this.pendingStates)
				{
					for (final PendingState pendingState : this.pendingStates.values())
					{
						if (pendingState.getStateLockMode().equals(StateLockMode.READ))
						{
							if (pendingState.isFinalized() == false)
								pendingState.tryFinalize();
						}
					}
				}
				
				// If global then some participants will be observers and wont be creating state certificates
				// which would trigger finalization when processing them in the regular pipeline.
				//
				// Therefore we must call it here too as its possible that the local instance received all the 
				// state certificates from remote validators prior to execution.
				if (isGlobal())
					tryFinalize();
			}
			catch(StateMachineExecutionException smex)
			{
				this.status.thrown(smex);
	
				if (atomsLog.hasLevel(Logging.DEBUG))
					atomsLog.debug(this.context.getName()+": Atom "+getHash()+" executed with exception");
			}
			catch(Exception ex)
			{
				this.status.thrown(ex);
	
				atomsLog.warn(this.context.getName()+": Atom "+getHash()+" executed with exception which should be wrapped in a StateMachineException", ex);
			}
		}
	}
	
	List<StateInput> getInputs()
	{
		synchronized(this.pendingStates)
		{
			final List<StateInput> inventory = new ArrayList<StateInput>(this.pendingStates.size());
			for (PendingState pendingState : this.pendingStates.values())
			{
				final StateInput stateInput = pendingState.getStateInput();
				if (stateInput == null)
					continue;

				inventory.add(stateInput);
			}
			
			return inventory;
		}
	}

	public Hash getExecutionDigest()
	{
		synchronized(this)
		{
			return this.stateMachine.getExecutionDigest();
		}
	}

	public Exception thrown()
	{
		return this.status.thrown();
	}	
	
	CommitOperation getCommitOperation()
	{
		synchronized(this)
		{
			final CommitOperation commitOperation = this.stateMachine.getCommitOperation();
			if (commitOperation == null)
				throw new IllegalStateException("Commit operation not yet created for pending atom "+getHash());
			
			if (this.certificate == null && Universe.getDefault().getGenesis().contains(getHash()) == false)
				throw new IllegalStateException("Commit operation not accessible without atom certificate for pending atom "+getHash());
			
			if (this.certificate != null && this.certificate.getDecision().equals(commitOperation.getDecision()) == false)
				throw new IllegalStateException("Commit operation decision "+commitOperation.getDecision()+" for pending atom "+getHash()+" does not match atom certificate decision "+this.certificate.getDecision());
			
			return commitOperation;
		}
	}

	public AtomStatus getStatus()
	{
		return this.status;
	}
			
	public BlockHeader getBlockHeader()
	{
		return this.block;
	}
			
	@Override
	public int hashCode()
	{
		return this.hash.hashCode();
	}

	@Override
	public boolean equals(Object object)
	{
		if (object == null)
			return false;

		if (object == this)
			return true;

		if (object instanceof PendingAtom pendingAtom)
		{
			if (pendingAtom.getHash().equals(this.hash))
				return true;
		}
		
		return false;
	}

	@Override
	public String toString()
	{
		return this.hash+":"+this.status+" "+(this.block == null ? "" : "("+this.block+")")+" @ "+this.witnessedAt;
	}
	
	boolean isAcceptable()
	{
		return this.acceptableAt > -1;
	}
	
	void setAcceptable()
	{
		synchronized(this)
		{
			if (this.acceptableAt > -1)
				throw new IllegalStateException("Pending atom "+getHash()+" is already acceptable");
			
			if (getStatus().before(AtomStatus.State.PREPARED))
				throw new IllegalStateException("Pending atom "+getHash()+" is not PREPARED");
			
			if (getStatus().after(AtomStatus.State.PREPARED))
				throw new IllegalStateException("Pending atom "+getHash()+" is already ACCEPTED with state "+getStatus().current());

			this.acceptableAt = Time.getSystemTime();
			
			// NOTE remove this to cause less rare disjoint timeouts across validator sets 
			this.acceptTimeoutAt = this.acceptableAt + TimeUnit.SECONDS.toMillis(Constants.ATOM_ACCEPT_TIMEOUT_SECONDS);
		}
	}

	PendingState vote(final StateVote stateVote, long weight) throws ValidationException
	{
		synchronized(this)
		{
			if (this.status.before(AtomStatus.State.FINALIZING))
				throw new IllegalStateException("Pending atom "+getHash()+" is not FINALIZING but "+this.status);
			
			final PendingState pendingState = getState(stateVote.getAddress());
			if (pendingState == null)
				throw new ValidationException(this, "Pending state not found for state vote "+stateVote.toString());

			if (this.status.current(AtomStatus.State.FINALIZING) && pendingState.isFinalized() == false)
				pendingState.vote(stateVote, weight);
			
			return pendingState;
		}
	}

	
	public boolean isForceAcceptTimeout()
	{
		return this.forceAcceptTimeout;
	}

	public boolean isExecuted() 
	{
		return this.status.after(AtomStatus.State.EXECUTING); 
	}

	public boolean isExecuteSignalled()
	{
		return this.executeSignalledBlock != Hash.ZERO;
	}

	Hash getExecuteSignalledBlock()
	{
		return this.executeSignalledBlock;
	}

	void setExecuteSignalledAtBlock(final BlockHeader header)
	{
		Objects.requireNonNull(header, "Header is null");

		synchronized(this)
		{
			if(this.executeSignalledBlock != Hash.ZERO)
				throw new IllegalStateException("Execution signal has already been made for "+this);
			
			this.executeSignalledBlock = header.getHash();
			this.commitTimeoutAt = header.getTimestamp() + TimeUnit.SECONDS.toMillis(Constants.ATOM_COMMIT_TIMEOUT_SECONDS);
			
			if (atomsLog.hasLevel(Logging.INFO))
			{
				if (isProvisioned())
					atomsLog.info(this.context.getName()+": Atom "+getHash()+" is signalled executable at "+this.executeSignalledBlock);
				else 
					atomsLog.info(this.context.getName()+": Atom "+getHash()+" is signalled executable at "+this.executeSignalledBlock+" but not provisioned");
			}
		}
	}

	public boolean isForceLatentExecution()
	{
		return this.forceLatentExecution;
	}

	public boolean isExecuteLatent(final BlockHeader header)
	{
		synchronized(this)
		{
			if (this.executeLatentSignalledBlock != Hash.ZERO)
				return true;
			
			if (this.status.after(AtomStatus.State.PREPARED) && this.status.before(AtomStatus.State.EXECUTING) && header.getTimestamp() >= this.executeLatentAt)
				return true;
			
			return false;
		}
	}
	
	public boolean isExecuteLatentSignalled()
	{
		return this.executeLatentSignalledBlock != Hash.ZERO;
	}

	Hash getExecuteLatentSignalledBlock()
	{
		synchronized(this)
		{
			return this.executeLatentSignalledBlock;
		}
	}

	void setExecuteLatentSignalledBlock(final BlockHeader header)
	{
		Objects.requireNonNull(header, "Header is null");

		synchronized(this)
		{
			if(this.executeLatentSignalledBlock != Hash.ZERO)
				throw new IllegalStateException("Execution latency signal has already been made for "+this);

			this.executeLatentSignalledBlock = header.getHash();
			
			if (atomsLog.hasLevel(Logging.INFO))
				atomsLog.info(this.context.getName()+": Atom "+getHash()+" is signalled execute latent at "+this.executeLatentSignalledBlock);
			
			clearTimeout();
		}
	}
	
	public boolean isForceExecutionTimeout()
	{
		return this.forceExecutionTimeout;
	}

	public boolean isForceCommitTimeout()
	{
		return this.forceCommitTimeout;
	}

	public AtomCertificate getCertificate()
	{
		return this.certificate;
	}
	
	public Collection<StateVoteBlock> getStateVoteBlocks()
	{
		synchronized(this.stateVoteBlocks)
		{
			return Collections.unmodifiableList(new ArrayList<>(this.stateVoteBlocks.values()));
		}
	}

	boolean addStateVoteBlock(final StateVoteBlock stateVoteBlock)
	{
		synchronized(this)
		{
			if (this.stateVoteBlocks.putIfAbsent(stateVoteBlock.getHash(), stateVoteBlock) == null)
				return true;
			else
				stateLog.warn(PendingAtom.this.context.getName()+": State vote block "+stateVoteBlock.getHash()+" is already known for pending atom "+this);
			
			return false;
		}
	}

	void setCertificate(final AtomCertificate certificate)
	{
		synchronized(this)
		{
			if (this.certificate != null)
				throw new IllegalStateException("Atom certificate is already set "+this.certificate);

			if (Objects.requireNonNull(certificate).getAtom().equals(getHash()) == false)
				throw new IllegalArgumentException("Atom certificate "+certificate.getHash()+" does not reference "+getHash());
			
			if (atomsLog.hasLevel(Logging.INFO))
				atomsLog.info(this.context.getName()+": Atom certificate "+certificate.getHash()+" assigned to pending atom "+getHash());
			
			this.certificate = certificate;
			this.status.set(AtomStatus.State.FINALIZED);
		}
	}
	
	// TODO harden this used by sync to skip execution
	void complete(final AtomCertificate certificate)
	{
		Objects.requireNonNull(certificate, "Atom certificate is null");
		
		if (certificate.getAtom().equals(getHash()) == false)
			throw new IllegalArgumentException("Atom certificate "+certificate.getHash()+" does not reference "+getHash());
		
		if (this.status.current(AtomStatus.State.PROVISIONED) == false)
			throw new IllegalStateException("Pending atom "+getHash()+" is not PROVISIONED but "+this.status);
		
		getStatus().set(State.EXECUTING);
		List<StateCertificate> stateCertificates = certificate.getInventory(StateCertificate.class);
		this.stateMachine.complete(stateCertificates.getFirst().getExecution());
		getStatus().set(State.FINALIZING);
		setCertificate(certificate);
		
		if (atomsLog.hasLevel(Logging.INFO))
			atomsLog.info(this.context.getName()+": Pending atom "+getHash()+" has bypassed execution and been completed with certificate "+certificate.getHash());
	}

	public List<StateOutput> getOutputs()
	{
		synchronized(this.pendingStates)
		{
			final List<StateOutput> inventory = new ArrayList<StateOutput>(this.pendingStates.size());
			for (final PendingState pendingState : this.pendingStates.values())
			{
				final StateOutput stateOutput = pendingState.getStateOutput();
				if (stateOutput == null)
					continue;
	
				inventory.add(stateOutput);
			}
				
			return inventory;
		}
	}

	public <T extends StateOutput> List<T> getOutputs(final Class<T> type)
	{
		Objects.requireNonNull(type, "Output type is null");
		
		synchronized(this.pendingStates)
		{
			final List<T> inventory = new ArrayList<T>(this.pendingStates.size());
			for (final PendingState pendingState : this.pendingStates.values())
			{
				final StateOutput stateOutput = pendingState.getStateOutput();
				if (stateOutput == null)
					continue;
					
				if (type.isInstance(stateOutput) == false)
					continue;
					
				inventory.add(type.cast(stateOutput));
			}
				
			return inventory;
		}
	}

	void provision(final StateOutput output)
	{
		synchronized(this)
		{
			if (stateLog.hasLevel(Logging.INFO))
				stateLog.info(this.context.getName()+": Adding state output "+output+" to "+getHash());
				
			// Not in a block yet but has at least one state certificate so effectively cancel the inclusion timeout
			if (this.block == null)
			{
				// TODO this actually should never happen if the local replica is healthy as StateCertificates
				// 		can not be created unless the atom is accepted in all relevant shard groups
				this.acceptTimeoutAt = Long.MAX_VALUE;
				
				if (atomsLog.hasLevel(Logging.DEBUG))
					atomsLog.debug(this.context.getName()+": Atom "+this.getHash()+" accept timeout cancelled due to state certificate prior to ACCEPTED");
			}
			
			final PendingState pendingState = this.pendingStates.computeIfAbsent(output.getAddress(), sa -> new PendingState(this.context, sa, this));
			pendingState.setStateOutput(output);
		}
	}
	
	boolean hasOutput(final StateOutput output)
	{
		return this.pendingStates.containsKey(output.getAddress());
	}
	
	boolean hasOutput(final StateAddress stateAddress)
	{
		return this.pendingStates.containsKey(stateAddress);
	}

	@SuppressWarnings("unchecked")
	<T extends StateOutput> T getOutput(final StateAddress stateAddress)
	{
		return (T) this.pendingStates.get(stateAddress).getStateOutput();
	}
	
	/** Return the wall clock timestamp for the next eligible timeout
	 * 
	 */
	long getNextTimeoutAt()
	{
		// Commit timeout
		if (this.status.after(AtomStatus.State.EXECUTING) && this.status.before(AtomStatus.State.COMPLETED))
			return getCommitTimeoutAt();
		// Execution timeout
		else if (this.status.after(AtomStatus.State.PREPARED) && this.status.before(AtomStatus.State.EXECUTING) && isExecuteLatentSignalled() == true)
			return getExecuteTimeoutAt();
		// Execution latent 
		else if (this.status.after(AtomStatus.State.PREPARED) && this.status.before(AtomStatus.State.EXECUTING) && isExecuteLatentSignalled() == false)
			return getExecuteLatentAt();
		// Accept timeout
		else if (this.status.current(AtomStatus.State.PREPARED))
			return getAcceptTimeoutAt();
		// Prepare timeout
		else if (this.status.before(AtomStatus.State.PREPARED))
			return getPrepareTimeoutAt();
		
		return -1;
	}
	
	/** Creates any timeouts due for this PendingAtom.
	 * 
	 * @return
	 */
	AtomTimeout tryTimeout(long timestamp)
	{
		synchronized(this)
		{
			if (this.status.current(AtomStatus.State.COMPLETED))
				throw new IllegalStateException("Atom "+getHash()+" is completed");
			
			AtomTimeout timeout = null;
			// Commit timeout
			if (this.status.after(AtomStatus.State.EXECUTING) && this.status.before(AtomStatus.State.COMPLETED) && timestamp >= getCommitTimeoutAt())
				timeout = new CommitTimeout(getHash(), getInputs());
			// Execution timeout
			else if (this.status.after(AtomStatus.State.PREPARED) && this.status.before(AtomStatus.State.EXECUTING) && isExecuteLatentSignalled() == true && timestamp >= getExecuteTimeoutAt())
				timeout = new ExecutionTimeout(getHash());
			// Execution latent 
			else if (this.status.after(AtomStatus.State.PREPARED) && this.status.before(AtomStatus.State.EXECUTING) && isExecuteLatentSignalled() == false && timestamp >= getExecuteLatentAt())
				timeout = new ExecutionLatentTimeout(getHash());
			// Accept timeout
			else if (this.status.current(AtomStatus.State.PREPARED) && timestamp >= getAcceptTimeoutAt())
				timeout = new AcceptTimeout(getHash());
			// Prepare timeout
			else if (this.status.before(AtomStatus.State.PREPARED) && timestamp >= getPrepareTimeoutAt())
				timeout = new PrepareTimeout(getHash());
			
			if (timeout != null)
			{
				// Debug conditionals to check timeouts should actually be triggered
				boolean falsePositive = false;
				if (timeout instanceof CommitTimeout)
				{
					falsePositive = true;
					for (final PendingState pendingState : getStates())
					{
						if (pendingState.getStateOutput() == null)
						{
							falsePositive = false;
							break;
						}
					}

					atomStatusLog.error(this.context.getName()+": "+timeout.getClass().getSimpleName()+" for "+getHash()+" but has all state outputs");
					
					if (this.certificate == null)
						falsePositive = false;
					else
						atomStatusLog.error(this.context.getName()+": "+timeout.getClass().getSimpleName()+" for "+getHash()+" but has atom certificate");

				}
				else if (timeout instanceof ExecutionLatentTimeout)
				{
					falsePositive = true;
					for (final PendingState pendingState : getStates())
					{
						if (pendingState.getStateInput() == null)
						{
							falsePositive = false;
							break;
						}
					}

					atomStatusLog.error(this.context.getName()+": "+timeout.getClass().getSimpleName()+" for "+getHash()+" but has all state inputs");
				}
	
				if (falsePositive)
					atomStatusLog.error(this.context.getName()+": "+timeout.getClass().getSimpleName()+" for "+getHash()+" but is false positive");
			
				setTimeout(timeout);
			}
			
			return timeout;
		}
	}

	@SuppressWarnings("unchecked")
	public <T extends AtomTimeout> T getTimeout()
	{
		return (T) this.timeout;
	}
	
	<T extends AtomTimeout> T clearTimeout()
	{
		synchronized(this)
		{
			if (this.timeout == null)
				return null;
			
			if (atomsLog.hasLevel(Logging.INFO))
				atomsLog.info(this.context.getName()+": Cleared "+this.timeout.getClass().getSimpleName()+" on "+this.status.current()+" for Atom "+getHash());
			
			T timeout = (T) this.timeout;
			this.timeout = null;
			return timeout;
		}
	}

	void setTimeout(final AtomTimeout timeout)
	{
		synchronized(this)
		{
			if (this.timeout != null)
				throw new IllegalStateException("Atom timeout is already set "+this.timeout);
			
			if (Objects.requireNonNull(timeout).getAtom().equals(getHash()) == false)
				throw new IllegalArgumentException("Atom timeout "+timeout.getHash()+" does not reference "+getHash());
			
			if (atomStatusLog.hasLevel(Logging.INFO))
				atomStatusLog.info(this.context.getName()+": Atom "+getHash()+" is signalling timeout "+timeout);
			
			this.timeout = timeout;
		}
	}
	
	AtomCertificate tryFinalize() throws CryptoException, IOException, ValidationException
	{
		synchronized(this)
		{
			if (getCertificate() != null)
			{
				stateLog.warn(this.context.getName()+": Atom certificate for "+getHash()+" already created");
				return this.certificate;
			}
			
			if (this.status.before(AtomStatus.State.EXECUTING))
			{
				stateLog.warn(this.context.getName()+": Attempted to create atom certificate for "+getHash()+" when status "+this.status.current());
				return null;
			}

			// TODO where does the validation of received certificates from other shard groups go? 
			//	    and what does it do?
			
			if (this.pendingStates.allSatisfy(PendingState::isFinalized) == false)
				return null;

			// Investigate possibility of creating an atom certificate.  All state references / certificates must have been created.
			final List<StateOutput> stateOutputs;
			CommitDecision decision = CommitDecision.ACCEPT;
			synchronized(this.pendingStates)
			{
				stateOutputs = new ArrayList<StateOutput>(this.pendingStates.size());
				for (final PendingState pendingState : this.pendingStates.values())
				{
					// Check existence of and decisions on the write substates
					if (pendingState.getStateLockMode().equals(StateLockMode.WRITE))
					{
						if (pendingState.isFinalized() == false)
							return null;
						
						if (pendingState.getStateOutput() instanceof StateCertificate stateCertificate)
						{
							if (stateCertificate.getDecision().equals(CommitDecision.ACCEPT) == false)
								decision = CommitDecision.REJECT;
						}
						else
							throw new IllegalArgumentException("Write locked pending state output "+pendingState.getAddress()+" is not a certificate");
					}
					// Check read locked pending states are already finalized post execute
					else if (pendingState.getStateLockMode().equals(StateLockMode.READ))
					{
						if (pendingState.isFinalized() == false)
							throw new IllegalArgumentException("Read locked pending state output "+pendingState.getAddress()+" is not finalized as expected");
					}
					
					stateOutputs.add(pendingState.getStateOutput());
				}
			}
			
			final AtomCertificate atomCertificate = new AtomCertificate(getHash(), decision, stateOutputs);
			setCertificate(atomCertificate);
			
			this.context.getMetaData().increment("ledger.pool.atom.certificates");
			if (stateLog.hasLevel(Logging.INFO))
				stateLog.info(this.context.getName()+": Created atom certificate "+atomCertificate.getHash()+" for atom "+atomCertificate.getAtom()+" with decision "+atomCertificate.getDecision());

			return atomCertificate;
		}
	}
}
