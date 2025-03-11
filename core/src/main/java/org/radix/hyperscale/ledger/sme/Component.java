package org.radix.hyperscale.ledger.sme;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.radix.hyperscale.common.BasicObject;
import org.radix.hyperscale.crypto.ComputeKey;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.crypto.Identity;
import org.radix.hyperscale.ledger.StateAddress;
import org.radix.hyperscale.ledger.StateAddressable;
import org.radix.hyperscale.ledger.StateContext;
import org.radix.hyperscale.ledger.StateLockMode;
import org.radix.hyperscale.ledger.Substate.NativeField;
import org.radix.hyperscale.utils.Strings;
import org.reflections.Reflections;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.reflections.util.FilterBuilder;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

@StateContext("component")
public abstract class Component extends BasicObject implements StateAddressable
{
	private static final BiMap<Object, String> componentContexts;
	private static final Map<StateAddress, Class<? extends NativeComponent>> nativeComponents;

	// TODO probably not the best place for all this, handler would be better
	static
	{
		componentContexts = HashBiMap.create();
		nativeComponents = new HashMap<StateAddress, Class<? extends NativeComponent>>();

		// Hacky means to load all native component classes and register them
		ConfigurationBuilder reflectionsConfig = new ConfigurationBuilder().setUrls(ClasspathHelper.forJavaClassPath())
																		   .filterInputsBy(new FilterBuilder().includePackage("org.radix.hyperscale"));
		Reflections reflections = new Reflections(reflectionsConfig);
		Set<Class<? extends NativeComponent>> nativeComponentClasses = reflections.getSubTypesOf(NativeComponent.class);
		for (Class<? extends NativeComponent> nativeComponentClass : nativeComponentClasses)
		{
			try 
			{
				Class.forName(nativeComponentClass.getName(), true, nativeComponentClass.getClassLoader());
			} 
			catch (ClassNotFoundException e) 
			{
				System.out.println("Failed to load NativeComponent "+nativeComponentClass+" "+e.toString());
			}
		}
	}

	public static Component load(StateMachine stateMachine, Class<? extends NativeComponent> nativeComponentClass) throws ReflectiveOperationException
	{
		return nativeComponentClass.getConstructor(StateMachine.class).newInstance(stateMachine);
	}

	public static Component load(StateMachine stateMachine, PolyglotPackage pakage, SubstateLog substateLog)
	{
		return new PolyglotComponent(stateMachine, pakage.getCode(), substateLog);
	}
	
	public static boolean registerContext(StateAddress address, String context)
	{
		if (componentContexts.putIfAbsent(address, context) != null)
			throw new IllegalStateException("Component context "+context+" is already registered");
		
		return true;
	}

	public static String getContext(Class<? extends NativeComponent> componentClass)
	{
		return componentContexts.get(componentClass);
	}

	public static String getContext(StateAddress stateAddress)
	{
		return componentContexts.get(stateAddress);
	}

	public static List<Class<? extends NativeComponent>> getNative()
	{
		return new ArrayList<Class<? extends NativeComponent>>(nativeComponents.values());
	}

	protected static Class<? extends NativeComponent> getNative(StateAddress stateAddress)
	{
		return nativeComponents.get(stateAddress);
	}
	
	protected static boolean registerNative(Class<? extends NativeComponent> componentClass)
	{
		String context = Strings.toLowerCase(componentClass.getAnnotation(StateContext.class).value());
		if (nativeComponents.putIfAbsent(StateAddress.from(Component.class.getAnnotation(StateContext.class).value(), Hash.valueOf(context)), componentClass) != null)
			throw new IllegalStateException("Native component "+componentClass+" is already registered");

		if (componentContexts.putIfAbsent(componentClass, context) != null)
			throw new IllegalStateException("Component context "+context+" is already registered");
		
		return true;
	}

	private final Identity identity;
	private transient SubstateLog substateLog;
	private transient StateMachine stateMachine;
	
	Component(StateMachine stateMachine, SubstateLog substateLog)
	{
		this.stateMachine = stateMachine;
		this.substateLog = substateLog;
		this.identity = ComputeKey.from(this.substateLog.getAddress().getHash()).getIdentity();
	}
	
	public Identity getIdentity()
	{
		return this.identity;
	}
	
	public String context()
	{
		return this.substateLog.get(NativeField.COMPONENT);
	}
	
	public String context(String extension)
	{
		return this.substateLog.get(NativeField.COMPONENT)+"."+Strings.toLowerCase(extension);
	}

	public Hash getAtom()
	{
		return getStateMachine().getPendingAtom().getHash();
	}
	
	public StateAddress getAtomAddress()
	{
		return getStateMachine().getPendingAtom().getAddress();
	}

	public Hash getBlock()
	{
		return getStateMachine().getPendingAtom().getBlockHeader().getHash();
	}

	@Override
	public StateAddress getAddress()
	{
		return this.substateLog.getAddress();
	}

	protected StateMachine getStateMachine()
	{
		return this.stateMachine;
	}
	
	protected final Bucket bucket(String label)
	{
		return getStateMachine().bucket(label);
	}
	
	protected final Vault vault(Identity identity)
	{
		return getStateMachine().vault(identity);
	}

	protected final Vault vault(Hash identifier, Identity authority)
	{
		return getStateMachine().vault(StateAddress.from(Vault.class.getAnnotation(StateContext.class).value(), identifier), authority);
	}

	protected final Vault vault(StateAddress address, Identity authority)
	{
		return getStateMachine().vault(address, authority);
	}

	protected final void lock(StateAddress stateAddress, StateLockMode lockMode)
	{
		getStateMachine().lock(stateAddress, lockMode);
	}
	
	protected final <T> T get(final StateAddress stateAddress, String field)
	{
		return get(stateAddress, field, null);
	}

	protected final <T> T get(final StateAddress stateAddress, String field, T def)
	{
		T value = getStateMachine().get(stateAddress, field);
		if (value == null)
			value = def;
		return value;
	}

	protected final <T> void set(final StateAddress stateAddress, String field, T value)
	{
		getStateMachine().set(stateAddress, field, value, getIdentity());
	}

	protected final void associate(StateAddress stateAddress, final Identity identity)
	{
		getStateMachine().associate(stateAddress, Hash.valueOf(identity));
	}

	protected final void associate(StateAddress stateAddress, final Hash association)
	{
		getStateMachine().associate(stateAddress, association);
	}
	
	protected final void assertExists(StateAddress stateAddress)
	{
		getStateMachine().assertExists(stateAddress);
	}
	
	protected final void assertNotExists(StateAddress stateAddress)
	{
		getStateMachine().assertNotExists(stateAddress);
	}

	protected final void assertCreate(StateAddress stateAddress, Identity authority)
	{
		getStateMachine().assertCreate(stateAddress, authority);
	}

	protected abstract void prepare(String method, Object[] arguments) throws ReflectiveOperationException;
	
	protected abstract <T> T call(String method, Object[] arguments) throws ReflectiveOperationException;

	protected abstract void close();
}
