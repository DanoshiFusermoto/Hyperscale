package org.radix.hyperscale.ledger.sme;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.Collectors;

import org.eclipse.collections.api.factory.Maps;
import org.eclipse.collections.api.map.MutableMap;
import org.radix.hyperscale.Context;
import org.radix.hyperscale.crypto.ComputeKey;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.crypto.Identity;
import org.radix.hyperscale.crypto.MerkleTree;
import org.radix.hyperscale.ledger.BlockHeader;
import org.radix.hyperscale.ledger.CommitDecision;
import org.radix.hyperscale.ledger.CommitOperation;
import org.radix.hyperscale.ledger.PendingAtom;
import org.radix.hyperscale.ledger.PendingState;
import org.radix.hyperscale.ledger.ShardGroupID;
import org.radix.hyperscale.ledger.ShardMapper;
import org.radix.hyperscale.ledger.StateAddress;
import org.radix.hyperscale.ledger.StateContext;
import org.radix.hyperscale.ledger.StateLockMode;
import org.radix.hyperscale.ledger.SubstateOpLog;
import org.radix.hyperscale.ledger.SubstateOutput;
import org.radix.hyperscale.ledger.Substate.NativeField;
import org.radix.hyperscale.ledger.events.PackageLoadedEvent;
import org.radix.hyperscale.ledger.primitives.Atom;
import org.radix.hyperscale.ledger.primitives.Blob;
import org.radix.hyperscale.ledger.primitives.StateInput;
import org.radix.hyperscale.ledger.sme.StateMachineStatus.State;
import org.radix.hyperscale.ledger.sme.arguments.AccountArgument;
import org.radix.hyperscale.ledger.sme.arguments.BadgeArgument;
import org.radix.hyperscale.ledger.sme.arguments.IdentityArgument;
import org.radix.hyperscale.ledger.sme.exceptions.StateMachineExecutionException;
import org.radix.hyperscale.ledger.sme.exceptions.StateMachinePreparationException;
import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;
import org.radix.hyperscale.serialization.SerializerId2;

import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;

public final class StateMachine
{
	private static final Logger stateMachineLog = Logging.getLogger("statemachine");

	private final 	Context 			context;
	private final 	PendingAtom 		pendingAtom;
	private final 	StateMachineStatus 	status;
	
	private final 	List<Object> instructions;
	private final 	MutableMap<Object, Object> primitives;
	private final 	Multimap<StateAddress, Hash> associations;
	private final 	Map<StateAddress, PendingState> pendingStates;

	private final 	MutableMap<StateAddress, Package> 	packages;
	private final 	MutableMap<StateAddress, Component> components;

	private volatile BlockHeader block;
	private volatile int readLockCount;
	private volatile int writeLockCount;
	private volatile Hash executionDigest;
	private volatile int currentInstruction;
	private volatile CommitOperation commitOperation;
	
	private volatile boolean global = false;
	
	public StateMachine(final Context context, final PendingAtom pendingAtom, final Map<StateAddress, PendingState> pendingStates) throws ManifestException
	{
		this.context = Objects.requireNonNull(context);
		this.pendingAtom = Objects.requireNonNull(pendingAtom);
		this.status = new StateMachineStatus(this);
		this.pendingStates = pendingStates;
		this.associations = LinkedHashMultimap.create();
		this.primitives = Maps.mutable.ofInitialCapacity(4);

		this.currentInstruction = -1;
		this.instructions = ManifestParser.parse(this.pendingAtom.getAtom().getManifest());
		this.packages = Maps.mutable.ofInitialCapacity(4);
		this.components = Maps.mutable.ofInitialCapacity(4);
	}
	
	Context getContext()
	{
		return this.context;
	}
	
	PendingAtom getPendingAtom()
	{
		return this.pendingAtom;
	}
	
	public boolean isGlobal()
	{
		if (this.status.before(State.PREPARED))
			throw new IllegalStateException("State machine for "+this.pendingAtom.getHash()+" is not PREPARED but "+this.status);

		return this.global;
	}
	
	public StateMachineStatus.State getStatus()
	{
		return this.status.current();
	}
	
	final boolean exists(final StateAddress stateAddress)
	{
		final SubstateLog substateLog = this.pendingStates.get(stateAddress).getSubstateLog();
		if (substateLog.hasWrites() == false && substateLog.isVoid())
			return false;
		
		final Identity authority = substateLog.getAuthority();
		if (authority == null)
			return false;
		
		return true;
	}

	final void assertExists(final StateAddress stateAddress)
	{
		final SubstateLog substateLog = this.pendingStates.get(stateAddress).getSubstateLog();
		
		if (substateLog.hasWrites() == false && substateLog.isVoid())
			throw new IllegalStateException("Substate "+stateAddress+" does not exist");
		
		final Identity authority = substateLog.getAuthority();
		if (authority == null)
			throw new IllegalStateException("Substate "+stateAddress+" does not exist");
	}
	
	final void assertNotExists(final StateAddress stateAddress)
	{
		final SubstateLog substateLog = this.pendingStates.get(stateAddress).getSubstateLog();
		if (substateLog.hasWrites() == false && substateLog.isVoid())
			return;
		
		final Identity authority = substateLog.getAuthority();
		if (authority == null)
			return;
		
		throw new IllegalStateException("Substate "+stateAddress+" exists");
	}

	void assertCreate(final StateAddress stateAddress, final Identity authority)
	{
		final SubstateLog substateLog = this.pendingStates.get(stateAddress).getSubstateLog();
		if (substateLog.hasWrites() == false && substateLog.isVoid())
		{
			substateLog.setAuthority(authority);
			return;
		}
		
		throw new IllegalStateException("Could not create substate "+stateAddress+", exists");
	}

	Bucket bucket(final String label)
	{
		synchronized(this)
		{
			Bucket bucket = (Bucket) this.primitives.get(label);
			if (bucket == null)
			{
				bucket = new Bucket(label);
				this.primitives.put(label, bucket);
			}
			
			return bucket;
		}
	}
	
	void lock(final StateAddress stateAddress, final StateLockMode lockMode)
	{
		synchronized(this)
		{
			final PendingState pendingState = this.pendingStates.computeIfAbsent(stateAddress, sa -> new PendingState(this.context, sa, this.pendingAtom));
			pendingState.lock(lockMode, this);

			if (lockMode.equals(StateLockMode.READ))
				this.readLockCount++;
			else if (lockMode.equals(StateLockMode.WRITE))
				this.writeLockCount++;
		}
	}
	
	Vault vault(final Identity identity)
	{
		return vault(StateAddress.from(Vault.class, identity), identity);
	}

	Vault vault(final StateAddress stateAddress, final Identity authority)
	{
		if (stateAddress.context().equalsIgnoreCase(Vault.class.getAnnotation(StateContext.class).value()) == false)
			throw new IllegalArgumentException("State address context is not a vault "+stateAddress);
		
		synchronized(this)
		{
			final PendingState pendingState = this.pendingStates.get(stateAddress);
			if (this.block.getHeight() > 0)
			{
				if (this.status.before(State.EXECUTING))
					throw new IllegalStateException("Attempt to modify substate "+stateAddress+" before EXECUTION of "+this.pendingAtom.getHash());

				if (pendingState == null)
					throw new IllegalStateException("Attempt to construct a vault with an unloaded substate "+stateAddress+" in "+this.pendingAtom.getHash());

				if (pendingState.getSubstateLog() == null)
					throw new IllegalStateException("Attempt to modify unloaded substate "+stateAddress+" in "+this.pendingAtom.getHash());
			}
			
			Vault vault = (Vault) this.primitives.computeIfAbsent(stateAddress, (sa) -> {
				SubstateLog vaultSubstateLog = pendingState.getSubstateLog();
				// Set the authority if the substate is void
				if (vaultSubstateLog.isVoid())
					vaultSubstateLog.setAuthority(authority);

				boolean debitable = true;
				if (vaultSubstateLog.getAuthority() != null && vaultSubstateLog.getAuthority().equals(authority) == false && 
					this.pendingAtom.getAtom().hasAuthority(vaultSubstateLog.getAuthority()) == false)
					debitable = false;
				
				return new Vault(this, vaultSubstateLog, debitable);
			});
			return vault;
		}
	}

	<T> T get(final StateAddress address, final NativeField field)
	{
		return get(address, field, null);
	}

	<T> T get(final StateAddress stateAddress, final String field)
	{
		return get(stateAddress, field, null);
	}
	
	<T> T get(final StateAddress address, final NativeField field, final T def)
	{
		return get(address, field.lower(), def);
	}

	<T> T get(final StateAddress stateAddress, final String field, final T def)
	{
		synchronized(this)
		{
			final PendingState pendingState = this.pendingStates.get(stateAddress);
			if (this.block.getHeight() > 0)
			{
				if (this.status.before(State.EXECUTING))
					throw new IllegalStateException("Attempt to read substate "+stateAddress+" before EXECUTION of "+this.pendingAtom.getHash());

				if (pendingState == null)
					throw new IllegalStateException("Attempt to read a substate that doesn't exist "+stateAddress+" in "+this.pendingAtom.getHash());

				if (pendingState.getSubstateLog() == null)
					throw new IllegalStateException("Attempt to read unloaded substate "+stateAddress+" in "+this.pendingAtom.getHash());
			}

			final SubstateLog substateLog = pendingState.getSubstateLog();
			if (substateLog.getAddress().context().equalsIgnoreCase(Vault.class.getAnnotation(StateContext.class).value()))
				throw new IllegalArgumentException("Substate "+stateAddress+" is a vault");
			
			T value = substateLog.get(field);
			if (value == null)
				value = def;
			return value;
		}
	}

	<T> void set(final StateAddress stateAddress, final NativeField field, final T value, final Identity caller)
	{
		set_internal(stateAddress, field.lower(), value, caller);
	}

	<T> void set(final StateAddress stateAddress, final String field, final T value, final Identity caller)
	{
		if(NativeField.isNativeField(field))
			throw new IllegalArgumentException("Field "+field+" is a protected NativeField");
		
		set_internal(stateAddress, field, value, caller);
	}
	
	private <T> void set_internal(final StateAddress stateAddress, final String field, final T value, final Identity caller)
	{
		synchronized(this)
		{
			final PendingState pendingState = this.pendingStates.get(stateAddress);
			if (this.block.getHeight() > 0) // Universe.getDefault().getGenesis().contains(this.pendingAtom.getHash()) == false)
			{
				if (this.status.before(State.EXECUTING))
					throw new IllegalStateException("Attempt to read substate "+stateAddress+" before EXECUTION of "+this.pendingAtom.getHash());

				if (pendingState == null)
					throw new IllegalStateException("Attempt to modify a substate that doesn't exist "+stateAddress+" in "+this.pendingAtom.getHash());

				if (pendingState.getSubstateLog() == null)
					throw new IllegalStateException("Attempt to modify unloaded substate "+stateAddress+" in "+this.pendingAtom.getHash());
			}
			
			final SubstateLog substateLog = pendingState.getSubstateLog();
			if (substateLog.getAddress().context().equalsIgnoreCase(Vault.class.getAnnotation(StateContext.class).value()))
				throw new IllegalArgumentException("Substate "+stateAddress+" is a vault");

			// TODO Components can modify their own substate
			if (1==0)
			{
/*				final Component component = this.components.get(this.currentOperation.getInstruction());
				if (component == null || component.getIdentity().equals(caller) == false)
				{
					if (substateLog.getAuthority().equals(ComputeKey.NULL.getIdentity()) == false && substateLog.getAuthority() != null && this.pendingAtom.getAtom().hasAuthority(substateLog.getAuthority()) == false)
						throw new IllegalStateException("No authority found to modify substate "+stateAddress+" in "+this.pendingAtom.getHash());
				}*/
			}
			
			substateLog.set(field, value);
		}
	}

	boolean associate(final StateAddress address, final Identity identity)
	{
		return associate(address, identity.getHash());
	}
	
	boolean associate(final StateAddress address, final Hash association)
	{
		synchronized(this)
		{
			if (this.associations.containsEntry(address, association))
				return false;

			this.associations.put(address, association);
			return true;
		}
	}			
	
	public boolean isProvisioned()
	{
		synchronized(this)
		{
			if (this.status.before(State.PREPARED))
				return false;

			if (this.status.after(State.PREPARED))
				return true;
			
			synchronized(this.pendingStates)
			{
				if (this.pendingStates.isEmpty())
					return false;
				
				// States are provisioned
				// Special case for UnifiedMaps for efficiency
				if (this.pendingStates instanceof MutableMap<StateAddress, PendingState> mutableMap)
				{
					if (mutableMap.allSatisfy(ps -> ps.isProvisioned()) == false)
						return false;
				}
				else
				{
					for (final PendingState pendingState : this.pendingStates.values())
					{
						if (pendingState.isProvisioned() == false)
							return false;
					}
				}
			}
			
			// Packages are provisioned
			for (int i = 0 ; i < this.instructions.size() ; i++)
			{
				final Object instruction = this.instructions.get(i);
				if (instruction instanceof Instantiate instantiate)
				{
					final StateAddress packageAddress = instantiate.getPackageAddress();
					if (this.packages.containsKey(packageAddress) == false)
						return false;
				}
				else if (instruction instanceof Method method)
				{
					if (method.isNative() == false)
					{
						final PendingState componentState = this.pendingStates.get(((Method)instruction).getComponentAddress());
						final StateAddress packageAddress = StateAddress.from(Package.class, componentState.getStateInput().getSubstate().<String>get(NativeField.BLUEPRINT));

						if (this.packages.containsKey(packageAddress) == false)
							return false;
					}
				}
			}
			
			return true;
		}
	}

	public boolean isProvisioned(final StateAddress stateAddress)
	{
		synchronized(this)
		{
			if (Package.class.getAnnotation(StateContext.class).value().equals(stateAddress.context()))
			{
				if (this.packages.isEmpty())
					return false;
				
				if (this.packages.containsKey(stateAddress) == false)
					return false;
			}
			else
			{
				synchronized(this.pendingStates)
				{
					if (this.pendingStates.isEmpty())
						return false;
					
					if (this.pendingStates.get(stateAddress).isProvisioned() == false)
						return false;
				}
			}
			
			return true;
		}
	}
	
	void provisioned()
	{
		this.status.set(State.PROVISIONED);

		if (stateMachineLog.hasLevel(Logging.INFO))
			stateMachineLog.info(this.context.getName()+": State machine for pending atom "+this.getPendingAtom().getHash()+" is provisioned");
	}
	
	public int numInstructions()
	{
		if (this.pendingAtom.getAtom() == null)
			throw new IllegalStateException("Atom is null");
			
		return this.instructions.size();
	}

	public List<Object> getInstructions()
	{
		if (this.pendingAtom.getAtom() == null)
			throw new IllegalStateException("Atom is null");
			
		return this.instructions;
	}

	public <T> List<T> getInstructions(final Class<T> type)
	{
		if (this.pendingAtom.getAtom() == null)
			throw new IllegalStateException("Atom is null");
			
		final List<T> instructions = new ArrayList<T>(this.instructions.size());
		for (int i = 0 ; i < this.instructions.size() ; i++)
		{
			final Object instruction = this.instructions.get(i);
			if (type.isInstance(instruction))
				instructions.add(type.cast(instruction));
		}
			
		return Collections.unmodifiableList(instructions);
	}

	private boolean isPackageEmbedded(final StateAddress packageAddress)
	{
		for (int i = 0 ; i < this.instructions.size() ; i++)
		{
			final Object instruction = this.instructions.get(i);
			if (instruction instanceof Instruction)
			{
				if (instruction instanceof Deploy deploy)
				{
					if (deploy.getBlueprintAddress().scope().equals(packageAddress.scope()))
						return true;
				}
				// TODO distinct package objects in atom?
			}
		}
			
		return false;
	}

	public List<Entry<StateAddress, Hash>> getAssociations()
	{
		synchronized(this)
		{
			if (this.associations.isEmpty())
				return Collections.emptyList();
			
			return Collections.unmodifiableList(new ArrayList<Entry<StateAddress, Hash>>(this.associations.entries()));
		}
	}
	
	public Hash getExecutionDigest()
	{
		synchronized(this)
		{
			return this.executionDigest;
		}
	}
	
	public CommitOperation getCommitOperation()
	{
		synchronized(this)
		{
			return this.commitOperation;
		}
	}
	
	private boolean isRequired(final StateAddress packageAddress)
	{
		if (this.status.before(State.PREPARED))
			throw new IllegalStateException("State machine for atom "+this.pendingAtom.getHash()+" is not PREPARED");
		
		for (int i = 0 ; i < this.instructions.size() ; i++)
		{
			final Object instruction = this.instructions.get(i);
			if (instruction instanceof Instantiate instantiate)
			{
				if (packageAddress.equals(instantiate.getPackageAddress()))
					return true;
			}
			else if (instruction instanceof Method method)
			{
				if (method.isNative() == false)
				{
					final PendingState componentState = this.pendingStates.get(((Method)instruction).getComponentAddress());
					if (componentState.isProvisioned())
					{
						if (packageAddress.equals(StateAddress.from(Package.class, componentState.getStateInput().getSubstate().<String>get(NativeField.BLUEPRINT))))
							return true;
					}
				}
			}
		}
		
		return false;
	}
	
	private boolean isLoaded(final StateAddress packageAddress)
	{
		synchronized(this)
		{
			return this.packages.containsKey(packageAddress);
		}
	}

	private void load(final StateAddress packageAddress) throws StateMachinePreparationException
	{
		synchronized(this)
		{
			if (isProvisioned())
				throw new IllegalStateException("State machine for atom "+this.pendingAtom.getHash()+" is already PROVISIONED when loading package "+packageAddress);

			if (isRequired(packageAddress) == false)
				throw new IllegalArgumentException("Package "+packageAddress+" is not required for atom "+this.pendingAtom.getHash());

			if (this.packages.containsKey(packageAddress))
				throw new IllegalStateException("Package "+packageAddress+" is already loaded for atom "+this.pendingAtom.getHash());
			
			// Bit of a hack, but really don't want to expose the PackageHander.load function publicly for the case of a single call from here
			try 
			{
				java.lang.reflect.Method loadMethod;
				loadMethod = org.radix.hyperscale.ledger.PackageHandler.class.getDeclaredMethod("load", StateAddress.class, PendingAtom.class);
				loadMethod.setAccessible(true);
				loadMethod.invoke(this.context.getLedger().getPackageHandler(), packageAddress, this.pendingAtom);
			} 
			catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) 
			{
				throw new StateMachinePreparationException(ex);
			}
		}
	}
	
	public void load(final Package pakage)
	{
		synchronized(this)
		{
			if (isProvisioned())
				throw new IllegalStateException("State machine for atom "+this.pendingAtom.getHash()+" is already PROVISIONED when loading package "+pakage);
			
			if (isRequired(pakage.getAddress()) == false)
				throw new IllegalArgumentException("Package "+pakage.getAddress()+" is not required for atom "+this.pendingAtom.getHash());

			if (this.packages.putIfAbsent(pakage.getAddress(), pakage) != null)
				throw new IllegalStateException("Package "+pakage.getAddress()+" is already loaded for atom "+this.pendingAtom.getHash());
			
			if (isProvisioned())
				provisioned();
		}
	}
	
	public Collection<PolyglotPackage> load(final Collection<PolyglotPackage> pakages, boolean throwIfNotRequired)
	{
		synchronized(this)
		{
			if (isProvisioned())
				throw new IllegalStateException("State machine for atom "+this.pendingAtom.getHash()+" is already PROVISIONED when loading package "+pakages.stream().map(p -> p.getAddress()).collect(Collectors.toList()));

			final Collection<PolyglotPackage> loadedPackages = new ArrayList<PolyglotPackage>(packages.size());
			for (final PolyglotPackage pakage : pakages)
			{
				if (isRequired(pakage.getAddress()) == false)
				{
					if (throwIfNotRequired)
						throw new IllegalArgumentException("Package "+pakage.getAddress()+" is not required for atom "+this.pendingAtom.getHash());
					
					continue;
				}

				if (this.packages.putIfAbsent(pakage.getAddress(), pakage) != null)
					throw new IllegalStateException("Package "+pakage.getAddress()+" is already loaded for atom "+this.pendingAtom.getHash());
				
				loadedPackages.add(pakage);
			}

			if (isProvisioned())
				provisioned();
			
			return loadedPackages;
		}
	}


	void inject(final Package pakage)
	{
		synchronized(this)
		{
			if (this.packages.putIfAbsent(pakage.getAddress(), pakage) != null)
				throw new IllegalStateException("Package "+pakage.getAddress()+" is already loaded for atom "+this.pendingAtom.getHash());
			
			if (isProvisioned())
				provisioned();
		}
	}
	
	public void prepare() throws StateMachinePreparationException
	{
		synchronized(this)
		{
			if (this.status.after(State.NONE))
				throw new IllegalStateException("State machine for atom "+this.pendingAtom.getHash()+" is already PREPARED");
			
			lock(this.pendingAtom.getAddress(), StateLockMode.WRITE);
			
			this.currentInstruction = 0;
			for (int i = 0 ; i < this.instructions.size() ; i++)
			{
				final Object instruction = this.instructions.get(i);
				if (instruction instanceof Instruction)
				{
					if (instruction instanceof Deploy deploy)
						prepare(deploy);
					else if (instruction instanceof Epoch epoch)
						prepare(epoch);
					else if (instruction instanceof Shards shards)
						prepare(shards);
					else if (instruction instanceof Instantiate instantiate)
						prepare(instantiate);
					else if (instruction instanceof Method method)
						prepare(method);
					else if (instruction instanceof Persist persist)
						prepare(persist);
					else
						throw new UnsupportedOperationException("Instruction type "+instruction.getClass()+" is not supported");

					if (instruction instanceof ArgumentedInstruction argumentedInstruction)
					{
						final Object[] arguments = argumentedInstruction.getArguments();
						for (int a = 0 ; a < arguments.length ;  a++)
						{
							final Object argument = arguments[a];
							if (argument instanceof SubstateArgument<?> substateArgument)
							{
								List<Entry<StateAddress, StateLockMode>> addresses = substateArgument.getAddresses();
								for (int sa = 0 ; sa < addresses.size() ; sa++)
								{
									Entry<StateAddress, StateLockMode> address = addresses.get(sa);
									lock(address.getKey(), address.getValue());
								}
							}
						}
						
						for (int a = 0 ; a < arguments.length ;  a++)
						{
							final Object argument = arguments[a];
							if (argument instanceof AccountArgument accountArgument)
								associate(this.pendingAtom.getAddress(), accountArgument.get());
							else if (argument instanceof BadgeArgument badgeArgument)
								associate(this.pendingAtom.getAddress(), badgeArgument.get());
							else if (argument instanceof IdentityArgument identityArgument)
								associate(this.pendingAtom.getAddress(), identityArgument.get());
						}
					}
					
					if (instruction instanceof GlobalInstruction)
						this.global = true;
				
					this.currentInstruction++;
				}
				else
					throw new UnsupportedOperationException("Manifest type "+instruction.getClass()+" is not supported");
			}
			this.currentInstruction = -1;
			
			// Check there is at least a mutation of state and the execution is not a NOP
			// EXCLUDES the Atom state itself
			if (this.writeLockCount <= 1)
				throw new IllegalStateException("Atom "+getPendingAtom().getHash()+" does not modify any state");
				
			this.status.set(State.PREPARED);
		}
	}
	
	private void prepare(final Persist persist) throws StateMachinePreparationException
	{
		try
		{
			if (stateMachineLog.hasLevel(Logging.DEBUG))
				stateMachineLog.debug(this.context.getName()+": Preparing persist of Blob "+persist);

			persist.lock(this);
		}
		// FIX ME this catches everything, some exceptions we want to report as implementation 
		//		  exceptions NOT execution excetions (e.g IllegalStateException due to state snapshot!)
		catch (Exception ex)
		{
			StateMachinePreparationException smex = new StateMachinePreparationException(ex);
			thrown(smex);
			throw smex; 
		}
	}

	private void prepare(final Deploy deploy) throws StateMachinePreparationException
	{
		try
		{
			if (stateMachineLog.hasLevel(Logging.DEBUG))
				stateMachineLog.debug(this.context.getName()+": Preparing deployment of blueprint "+deploy);

			deploy.lock(this);
		}
		// FIX ME this catches everything, some exceptions we want to report as implementation 
		//		  exceptions NOT execution excetions (e.g IllegalStateException due to state snapshot!)
		catch (Exception ex)
		{
			StateMachinePreparationException smex = new StateMachinePreparationException(ex);
			thrown(smex);
			throw smex; 
		}
	}

	private void prepare(final Epoch epoch) throws StateMachinePreparationException
	{
		try
		{
			if (stateMachineLog.hasLevel(Logging.DEBUG))
				stateMachineLog.debug(this.context.getName()+": Preparing epoch "+epoch);

			epoch.lock(this);
		}
		// FIX ME this catches everything, some exceptions we want to report as implementation 
		//		  exceptions NOT execution excetions (e.g IllegalStateException due to state snapshot!)
		catch (Exception ex)
		{
			StateMachinePreparationException smex = new StateMachinePreparationException(ex);
			thrown(smex);
			throw smex; 
		}
	}

	private void prepare(final Shards shards) throws StateMachinePreparationException
	{
		try
		{
			if (stateMachineLog.hasLevel(Logging.DEBUG))
				stateMachineLog.debug(this.context.getName()+": Preparing shards "+shards);

			shards.lock(this);
		}
		// FIX ME this catches everything, some exceptions we want to report as implementation 
		//		  exceptions NOT execution excetions (e.g IllegalStateException due to state snapshot!)
		catch (Exception ex)
		{
			StateMachinePreparationException smex = new StateMachinePreparationException(ex);
			thrown(smex);
			throw smex; 
		}
	}

	private void prepare(final Instantiate instantiate) throws StateMachinePreparationException
	{
		try
		{
			if (stateMachineLog.hasLevel(Logging.DEBUG))
				stateMachineLog.debug(this.context.getName()+": Preparing instantiation of blueprint "+instantiate.getBlueprintName()+" to component "+instantiate.getComponentName());

			instantiate.lock(this);
		}
		// FIX ME this catches everything, some exceptions we want to report as implementation 
		//		  exceptions NOT execution excetions (e.g IllegalStateException due to state snapshot!)
		catch (Exception ex)
		{
			StateMachinePreparationException smex = new StateMachinePreparationException(ex);
			thrown(smex);
			throw smex; 
		}
	}

	private void prepare(final Method method) throws StateMachinePreparationException
	{
		try
		{
			final Class<? extends NativeComponent> nativeClass = Component.getNative(method.getComponentAddress());

			Component component = this.components.get(method.getComponentAddress());
			if (component == null)
			{
				if (nativeClass != null)
				{
					component = Component.load(this, nativeClass);
					if (component == null)
						throw new StateMachineExecutionException("Native component "+method.getComponentAddress()+" not found");
				}
	
				// TODO major improvements of state management for non-native components are possible
				if (nativeClass == null)
				{
					// Component state
					lock(method.getComponentAddress(), StateLockMode.WRITE);
						
					// Component vault
					lock(StateAddress.from(Vault.class, ComputeKey.from(method.getComponentAddress().getHash()).getIdentity()), StateLockMode.WRITE);
				}

				this.components.put(method.getComponentAddress(), component);
			}
			
			if (nativeClass != null)
				component.prepare(method.getMethod(), method.getArguments());
		}
		// FIX ME this catches everything, some exceptions we want to report as implementation 
		//		  exceptions NOT execution excetions (e.g IllegalStateException due to state snapshot!)
		catch (Exception ex)
		{
			StateMachinePreparationException smex = new StateMachinePreparationException(ex);
			thrown(smex);
			throw smex; 
		}
	}

	public void accepted(final BlockHeader header) throws IOException
	{
		Objects.requireNonNull(header, "Proposal header is null");
		
		final org.radix.hyperscale.ledger.Epoch epoch = org.radix.hyperscale.ledger.Epoch.from(header);
		final int numShardGroups = this.context.getLedger().numShardGroups(epoch);
		final ShardGroupID localShardGroupID = ShardMapper.toShardGroup(this.context.getNode().getIdentity(), numShardGroups);
		final long localShardGroupVoteThreshold = this.context.getLedger().getValidatorHandler().getVotePowerThreshold(epoch, localShardGroupID);

		synchronized(this)
		{
			if (this.block != null)
				throw new IllegalStateException("Statemachine for "+this.pendingAtom.getHash()+" is already accepted");
	
			this.block = header;
			
			synchronized(this.pendingStates)
			{
				for (final PendingState pendingState : this.pendingStates.values())
				{
					final ShardGroupID stateShardGroupID = ShardMapper.toShardGroup(pendingState.getAddress(), numShardGroups);
					if (stateShardGroupID.equals(localShardGroupID))
						pendingState.accepted(this.block, localShardGroupVoteThreshold);
					else
						pendingState.accepted(this.block);
				}
			}

			if (isProvisioned())
				provisioned();
		}
	}
	
	public void provision(final StateInput stateInput) throws StateMachinePreparationException
	{
		Objects.requireNonNull(stateInput, "State input is null");
		Objects.requireNonNull(stateInput.getSubstate(), "State input substate is null");

		if (stateInput.getAtom().equals(this.pendingAtom.getHash()) == false)
			throw new IllegalArgumentException("State input "+stateInput.getSubstate().getAddress()+" not relevant for Statemachine "+this.pendingAtom.getHash());

		if (isProvisioned())
			throw new IllegalStateException("Statemachine for atom "+this.pendingAtom.getHash()+" is already provisioned");

		synchronized(this)
		{
			final PendingState pendingState = this.pendingStates.get(stateInput.getSubstate().getAddress());
			if (pendingState.getStateInput() == null)
				pendingState.setStateInput(stateInput);
			pendingState.provision();
			
			// Need to load a package?
			StateAddress packageAddress = null;
			if (stateInput.getSubstate().getAddress().context().equals(Deploy.class.getAnnotation(StateContext.class).value()))
			{
				packageAddress = StateAddress.from(Package.class, stateInput.getSubstate().getAddress().scope());
				if (isPackageEmbedded(packageAddress))
					packageAddress = null;
			}
			else if (stateInput.getSubstate().getAddress().context().equals(Component.class.getAnnotation(StateContext.class).value()))
			{
				String blueprint = stateInput.getSubstate().<String>get(NativeField.BLUEPRINT);
				if (blueprint != null)
					packageAddress = StateAddress.from(Package.class, blueprint);
			}

			if (packageAddress != null)
			{
				if (isLoaded(packageAddress) == false)
					load(packageAddress);
			}
			else if (isProvisioned())
				provisioned();
		}
	}
	
	public void execute() throws StateMachineExecutionException, ReflectiveOperationException
	{
		synchronized(this)
		{
			try
			{
				if (isProvisioned() == false)
					throw new StateMachineExecutionException("Statemachine for atom "+this.pendingAtom.getHash()+" is not provisioned");
				
				this.status.set(State.EXECUTING);
	
				try
				{
					this.currentInstruction = 0;
					for (int i = 0 ; i < this.instructions.size() ; i++)
					{
						final Object instruction = this.instructions.get(i);
						if (instruction instanceof Instruction)
						{
							if (instruction instanceof Deploy deploy)
								execute(deploy);
							else if (instruction instanceof Epoch epoch)
								execute(epoch);
							else if (instruction instanceof Shards shards)
								execute(shards);
							else if (instruction instanceof Method method)
								call(method);
							else if (instruction instanceof Instantiate instantiate)
								execute(instantiate);
							else if (instruction instanceof Persist persist)
								execute(persist);
							else
								throw new UnsupportedOperationException("Instruction type "+instruction.getClass()+" is not supported");
						}
						else
							throw new UnsupportedOperationException("Manifest type "+instruction.getClass()+" is not supported");
	
						this.currentInstruction++;
					}
					this.currentInstruction = -1;
					
					// Check all buckets are empty
					this.primitives.allSatisfy(primitive -> {
						if (primitive instanceof Bucket bucket && bucket.isEmpty() == false)
							throw new IllegalStateException("Bucket "+bucket+" is not empty");
						
						return true;
					});
					
					this.status.set(State.COMPLETED);
				}
				// FIX ME this catches everything, some exceptions we want to report as implementation 
				//		  exceptions NOT execution exceptions (e.g IllegalStateException due to state snapshot!)
				catch (StateMachineExecutionException smex)
				{
					thrown(smex);
					throw smex; 
				}
				catch (Exception ex)
				{
					thrown(ex);
					throw ex; 
				}
			}
			finally
			{
				buildExecutionOutput();

				// Close all contexts
				this.components.each(component -> component.close());
			}
		}
	}
	
	private void execute(final Persist persist) throws StateMachineExecutionException
	{
		try
		{
			if (stateMachineLog.hasLevel(Logging.DEBUG))
				stateMachineLog.debug(this.context.getName()+": Executing persist of Blob "+persist);

			persist.execute(this);
		}
		// FIX ME this catches everything, some exceptions we want to report as implementation 
		//		  exceptions NOT execution excetions (e.g IllegalStateException due to state snapshot!)
		catch (Exception ex)
		{
			StateMachineExecutionException smex = new StateMachineExecutionException(ex);
			thrown(smex);
			throw smex; 
		}
	}

	private void execute(final Deploy deploy) throws StateMachineExecutionException
	{
		try
		{
			if (stateMachineLog.hasLevel(Logging.DEBUG))
				stateMachineLog.debug(this.context.getName()+": Executing deployment of blueprint "+deploy);

			final PolyglotPackage pakage = deploy.execute(this);
			
			// TODO potentially causes issues for syncing
			this.context.getEvents().post(new PackageLoadedEvent(pakage));
		}
		// FIX ME this catches everything, some exceptions we want to report as implementation 
		//		  exceptions NOT execution excetions (e.g IllegalStateException due to state snapshot!)
		catch (Exception ex)
		{
			StateMachineExecutionException smex = new StateMachineExecutionException(ex);
			thrown(smex);
			throw smex; 
		}
	}

	private void execute(final Epoch epoch) throws StateMachineExecutionException
	{
		try
		{
			if (stateMachineLog.hasLevel(Logging.DEBUG))
				stateMachineLog.debug(this.context.getName()+": Executing epoch "+epoch);

			epoch.execute(this);
		}
		// FIX ME this catches everything, some exceptions we want to report as implementation 
		//		  exceptions NOT execution excetions (e.g IllegalStateException due to state snapshot!)
		catch (Exception ex)
		{
			StateMachineExecutionException smex = new StateMachineExecutionException(ex);
			thrown(smex);
			throw smex; 
		}
	}

	private void execute(final Shards shards) throws StateMachineExecutionException
	{
		try
		{
			if (stateMachineLog.hasLevel(Logging.DEBUG))
				stateMachineLog.debug(this.context.getName()+": Executing shards "+shards);

			shards.execute(this);
		}
		// FIX ME this catches everything, some exceptions we want to report as implementation 
		//		  exceptions NOT execution excetions (e.g IllegalStateException due to state snapshot!)
		catch (Exception ex)
		{
			StateMachineExecutionException smex = new StateMachineExecutionException(ex);
			thrown(smex);
			throw smex; 
		}
	}

	private void execute(final Instantiate instantiate) throws StateMachineExecutionException
	{
		try
		{
			if (stateMachineLog.hasLevel(Logging.DEBUG))
				stateMachineLog.debug(this.context.getName()+": Executing instantiation of blueprint "+instantiate);

			instantiate.execute(this);
		}
		// FIX ME this catches everything, some exceptions we want to report as implementation 
		//		  exceptions NOT execution excetions (e.g IllegalStateException due to state snapshot!)
		catch (Exception ex)
		{
			StateMachineExecutionException smex = new StateMachineExecutionException(ex);
			thrown(smex);
			throw smex; 
		}
	}

	private void call(final Method method) throws StateMachineExecutionException, ReflectiveOperationException
	{
		synchronized(this)
		{
			Component component = this.components.get(method.getComponentAddress());
			if (component == null)
			{
				final Class<? extends NativeComponent> nativeRef = Component.getNative(method.getComponentAddress());
				if (nativeRef == null)
				{
					if (stateMachineLog.hasLevel(Logging.DEBUG))
						stateMachineLog.debug(this.context.getName()+": Creating component "+method.getComponentAddress());
						
					PendingState componentSubstate = this.pendingStates.get(method.getComponentAddress());
					if (componentSubstate == null)
						throw new StateMachineExecutionException("Substate for component "+method.getComponentAddress()+" not found");
					
					if (componentSubstate.getSubstateLog().isVoid())
						throw new StateMachineExecutionException("Substate is void for component "+method.getComponentAddress());
		
					StateAddress packageAddress = StateAddress.from(Package.class, componentSubstate.getSubstateLog().<String>get(NativeField.BLUEPRINT));
					Package pakage = this.packages.get(packageAddress); 
					if (pakage == null)
						throw new StateMachineExecutionException("Package "+packageAddress+" not found");
		
					component = Component.load(this, (PolyglotPackage) pakage, componentSubstate.getSubstateLog());
					if (component == null)
						throw new StateMachineExecutionException("Component "+method.getComponentAddress()+" could not be loaded");
						
				}
				else
				{
					component = Component.load(this, nativeRef);
					if (component == null)
						throw new StateMachineExecutionException("Native component "+method.getComponentAddress()+" not found");
				}

				this.components.put(method.getComponentAddress(), component);
			}

			if (stateMachineLog.hasLevel(Logging.DEBUG))
				stateMachineLog.debug(this.context.getName()+": Executing "+method.getMethod()+" on component "+method.getComponentAddress()+" with arguments "+method.getArguments());
			
			component.call(method.getMethod(), method.getArguments());
		}
	}
	
	void call(final StateAddress componentAddress, final String method, final Object[] arguments) throws StateMachineExecutionException, ReflectiveOperationException
	{
		synchronized(this)
		{
			try
			{
				Component component = this.components.get(componentAddress);
				if (component == null)
				{
					final Class<? extends NativeComponent> nativeRef = Component.getNative(componentAddress);
					if (nativeRef == null)
					{
						final PendingState componentSubstate = this.pendingStates.get(componentAddress);
						if (componentSubstate == null)
							throw new StateMachineExecutionException("Substate for component "+componentAddress+" not found");
					
						final StateAddress packageAddress = StateAddress.from(Package.class, componentSubstate.getSubstateLog().<String>get(NativeField.BLUEPRINT));
						final Package pakage = this.packages.get(packageAddress); 
						if (pakage == null)
							throw new StateMachineExecutionException("Package "+packageAddress+" not found");
		
						component = Component.load(this, (PolyglotPackage) pakage, componentSubstate.getSubstateLog());
						if (component == null)
							throw new StateMachineExecutionException("Component "+componentAddress+" could not be loaded");
					}
					else
					{
						component = Component.load(this, nativeRef);
						if (component == null)
							throw new StateMachineExecutionException("Native component "+componentAddress+" not found");
					}
					
					this.components.put(componentAddress, component);
				}
	
				if (stateMachineLog.hasLevel(Logging.DEBUG))
					stateMachineLog.debug(this.context.getName()+": Executing "+method+" on component "+componentAddress+" with arguments "+arguments);
				
				component.call(method, arguments);
			}
			catch (Exception ex)
			{
				if (stateMachineLog.hasLevel(Logging.INFO))
				{
					stateMachineLog.error(StateMachine.this.context.getName()+": Exception executing method "+method+" on component "+componentAddress+" with arguments "+arguments+" in atom "+this.pendingAtom.getHash());
					stateMachineLog.error(StateMachine.this.context.getName()+" -> "+ex.getMessage());
				}
				
				throw ex;
			}			
		}
	}

	int getCurrentInstruction()
	{
		return this.currentInstruction;
	}

	private void buildExecutionOutput()
	{
		if (thrown() == null)
		{
			// IMPORTANT: Execution outputs should be deterministic and ordered.  Validators may
			//			  have provisioned pending states in a different sequence to each other
			//			  due to sharding latencies etc.
			//
			//			  Explicit ordering via a state address hash sort is a simple, effective solution. 
			final MerkleTree executionMerkle;
			final List<PendingState> orderedPendingStates;
			synchronized(this.pendingStates)
			{
				executionMerkle = new MerkleTree(this.pendingStates.size());
				orderedPendingStates = new ArrayList<>(this.pendingStates.values());
			}
			orderedPendingStates.sort((ps1, ps2) -> ps1.getAddress().compareTo(ps2.getAddress()));
			
			for (final PendingState pendingState : orderedPendingStates)
			{
				final SubstateLog substateLog = pendingState.getSubstateLog();
				substateLog.seal();

				// Don't include the executing Atom substate in execution digest!
				// TODO should this exclusion be here of further up the stack at the provisioning layer etc
				if (pendingState.getAddress().context().equals(Atom.class.getAnnotation(StateContext.class).value()))
					continue;
				
				final Hash executionLeaf;
				
				if (substateLog.hasWrites())
				{
					final SubstateTransitions transitions = substateLog.toSubstateTransitions();
					executionLeaf = transitions.getHash();
					
					if (stateMachineLog.hasLevel(Logging.INFO))
					{
						stateMachineLog.info(this.context.getName()+": Execution output transitions "+transitions.getHash()+" for substate "+pendingState.getAddress()+" in atom "+pendingState.getAtom());
						stateMachineLog.info(this.context.getName()+": 							    "+transitions.toSubstate(SubstateOutput.TRANSITIONS).values());
					}
				}
				else 
				{
					executionLeaf = pendingState.getAddress().getHash();
					
					if (stateMachineLog.hasLevel(Logging.INFO))
						stateMachineLog.info(this.context.getName()+": Execution output reference "+pendingState.getAddress()+" in atom "+pendingState.getAtom());
				}
	
				executionMerkle.appendLeaf(executionLeaf);
			}
	
			this.executionDigest = executionMerkle.buildTree();
		}
		else
			this.executionDigest = Hash.ZERO;
		
		buildCommitOperation();
	}
	
	private void buildCommitOperation()
	{
		if (thrown() == null)
		{
			// TODO dont think this filter should be here
			final org.radix.hyperscale.ledger.Epoch epoch = org.radix.hyperscale.ledger.Epoch.from(this.block);
			final int numShardGroups = this.context.getLedger().numShardGroups(epoch);
			final ShardGroupID localShardGroupID = ShardMapper.toShardGroup(this.context.getNode().getIdentity(), numShardGroups);
			
			// Filter substates that are relevant to the local shard group.  
			// NOTE Blob substates are persisted in all shard groups the atom touches as they provide a mapping to the parent atom & proposal.
			final List<SubstateTransitions> filteredSubstateTransitions;
			final Multimap<Integer, SubstateTransitions> snapshotSubstateTransitions;
			synchronized(this.pendingStates)
			{
				filteredSubstateTransitions = new ArrayList<>(this.pendingStates.size());
				for (final PendingState pendingState : this.pendingStates.values())
				{
					final ShardGroupID shardGroupID = ShardMapper.toShardGroup(pendingState.getAddress().getShardID(), numShardGroups);
					if (pendingState.getAddress().context().equalsIgnoreCase(Blob.class.getAnnotation(SerializerId2.class).value()) == false && shardGroupID.equals(localShardGroupID) == false)
						continue;
					
					final SubstateLog substateLog = pendingState.getSubstateLog();
					if (substateLog.hasWrites() == false)
						continue;
	
					filteredSubstateTransitions.add(substateLog.toSubstateTransitions());
				}
				
				// Build the substate operation log
				snapshotSubstateTransitions = LinkedHashMultimap.create();
				for (final PendingState pendingState : this.pendingStates.values())
				{
					final SubstateLog substateLog = pendingState.getSubstateLog();
					if (substateLog.isVoid())
						continue;

					for (int i = 0 ; i < this.instructions.size() ; i++)
					{
						final SubstateTransitions substateTransitions = substateLog.toSubstateTransitions(i);
						if (substateTransitions.isVoid())
							continue;
							
						snapshotSubstateTransitions.put(i, substateTransitions);
					}
				}
			}
				
			final Multimap<StateAddress, Hash> filteredAssociations = LinkedHashMultimap.create();
			for (final Entry<StateAddress, Hash> association : this.associations.entries())
			{
				final ShardGroupID shardGroupID = ShardMapper.toShardGroup(association.getKey().getShardID(), numShardGroups);
				if (shardGroupID.equals(localShardGroupID) == false)
					continue;
				
				if (filteredAssociations.put(association.getKey(), association.getValue()) == false)
					stateMachineLog.warn(this.context.getName()+": Duplicate association for state address "+association.getKey()+" in atom "+this.pendingAtom.getHash());
			}

			this.commitOperation = new CommitOperation(CommitDecision.ACCEPT, this.block, this.pendingAtom.getAtom(), filteredSubstateTransitions, filteredAssociations.entries(), new SubstateOpLog(this.pendingAtom.getHash(), snapshotSubstateTransitions));
		}
		else
			this.commitOperation = new CommitOperation(CommitDecision.REJECT, this.block, this.pendingAtom.getAtom(), Collections.emptyList(), Collections.emptyList(), new SubstateOpLog(this.pendingAtom.getHash()));
	}
	
	// TODO harden this used by sync to skip execution
	public void complete(final Hash executionDigest)
	{
		Objects.requireNonNull(executionDigest, "Execution digest is null");
		Hash.notZero(executionDigest, "Execution digest is ZERO");
		
		if (this.status.current(State.PROVISIONED) == false)
			throw new IllegalStateException("State machine for atom "+this.pendingAtom.getHash()+" is not provisioned but "+this.status.current());
			
		this.status.set(State.EXECUTING);
		this.executionDigest = executionDigest;
		this.status.set(State.COMPLETED);
	}

	Exception thrown()
	{
		return this.status.thrown();
	}
	
	private void thrown(final Exception t)
	{
		Objects.requireNonNull(t, "Exception is null");

		stateMachineLog.error(this.context.getName()+": Failed to execute atom "+this.pendingAtom.getHash(), t);
		
		this.status.thrown(t);
	}
}

