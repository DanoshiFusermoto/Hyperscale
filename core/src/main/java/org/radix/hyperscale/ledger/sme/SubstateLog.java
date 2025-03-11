package org.radix.hyperscale.ledger.sme;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.eclipse.collections.api.factory.Maps;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.impl.factory.primitive.IntObjectMaps;
import org.radix.hyperscale.crypto.CryptoException;
import org.radix.hyperscale.crypto.Identity;
import org.radix.hyperscale.crypto.Key;
import org.radix.hyperscale.crypto.PublicKey;
import org.radix.hyperscale.crypto.Signature;
import org.radix.hyperscale.exceptions.ValidationException;
import org.radix.hyperscale.ledger.StateAddress;
import org.radix.hyperscale.ledger.StateAddressable;
import org.radix.hyperscale.ledger.Substate;
import org.radix.hyperscale.ledger.Substate.NativeField;
import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;
import org.radix.hyperscale.utils.Numbers;
import org.radix.hyperscale.utils.Strings;

public class SubstateLog implements StateAddressable
{
	private static final Logger stateMachineLog = Logging.getLogger("statemachine");
	
	private final StateMachine 			stateMachine;
	private final StateAddress 			address;
	private final Map<String, Object> 	inputs;
	private final Map<String, Object> 	outputs;

	private final SubstateTransitions untouched;
	private transient MutableIntObjectMap<SubstateTransitions> transitions;
	
	private volatile boolean hasReads = false;
	private volatile boolean hasWrites = false;
	private volatile boolean sealed = false;
	
	public SubstateLog(final StateMachine stateMachine, final Substate input)
	{
		this(stateMachine, input, false);
	}

	protected SubstateLog(final StateMachine stateMachine, final Substate input, boolean immutable)
	{
		Objects.requireNonNull(input, "Input substate is null");
		
		this.stateMachine = stateMachine;
		this.address = input.getAddress();
		
		if (input.isVoid() == false)
		{
			this.inputs = Maps.mutable.ofInitialCapacity(input.size());
			input.forEach(inputs::put);
		}
		else 
			this.inputs = Collections.emptyMap();
		
		if (immutable == true)
		{
			this.outputs = Collections.emptyMap();
			this.sealed = true;
		}
		else
			this.outputs = Maps.mutable.ofInitialCapacity(8);
		
		this.untouched = new SubstateTransitions(this.address, false);
		this.transitions = IntObjectMaps.mutable.ofInitialCapacity(stateMachine.numInstructions()+1);
	}

	@Override
	public StateAddress getAddress()
	{
		return this.address;
	}
	
	public Identity getAuthority() 
	{
		return get(NativeField.AUTHORITY);
	}
	
	public Identity setAuthority(final Identity authority)
	{
		throwIfSealed();
		
		synchronized(this)
		{
			if (getAuthority() != null)
				throw new IllegalStateException("Authority can not be set without authorization");
		
			return (Identity) set(NativeField.AUTHORITY, authority);
		}
	}
	
	public Identity setAuthority(final Identity authority, final Signature signature) throws ValidationException, CryptoException
	{
		throwIfSealed();

		synchronized(this)
		{
			if (getAuthority() != null)
			{
				Key key = authority.getKey();
				if (key.canVerify() == false)
					throw new CryptoException("Authority key "+authority+" can not verify signatures");
				
				if ((key instanceof PublicKey) == false)
					throw new CryptoException("Authority key "+authority+" can is not a public key");
				
				if(((PublicKey)key).verify(address.getHash(), signature) == false)
					throw new CryptoException("Verification of signature "+signature+" by authority key "+authority+" failed");
				
				throw new ValidationException("Authority can not be set without authorization");
			}
			
			return (Identity) set(NativeField.AUTHORITY, authority);
		}
	}

	public boolean isVoid()
	{
		synchronized(this)
		{
			return this.inputs.isEmpty() && this.outputs.isEmpty();
		}
	}
	
	public boolean exists(String field)
	{
		field = Strings.toLowerCase(field);

		synchronized(this)
		{
			Object value = this.outputs.get(field);
			if (value == null)
				value = this.inputs.get(field);

			if (value == null)
				return false;

			return true;
		}
	}
	
	public <T> T get(final NativeField field)
	{
		return getOrDefault(field.lower(), null);
	}

	public <T> T get(final String field)
	{
		return getOrDefault(field, null);
	}

	@SuppressWarnings("unchecked")
	public <T> T getOrDefault(String field, T def)
	{
		field = Strings.toLowerCase(field);

		synchronized(this)
		{
			Object value = (T) this.outputs.get(field);
			if (value == null)
			{
				value = (T) this.inputs.get(field);
			
				if (value != null)
					read(field, value);
			}
			
			if (value == null)
				value = def;
			else
			{
				// Return shallow copies of collections and maps
				if (value instanceof Collection)
				{
					Class<? extends Collection<Object>> clazz = (Class<? extends Collection<Object>>) value.getClass();
					
					// Quite a few uses of Collections.singletonList() for code readability. 
					// It is a private class, so just convert it to ArrayList.  Do the same for all other private collections.
					// TODO better methods
					if (Modifier.isPrivate(clazz.getModifiers()))
						value = (T) new ArrayList<Object>((Collection<Object>)value);
					else
					{
						try
						{
							Object newInstance = clazz.getDeclaredConstructor().newInstance();
							clazz.cast(newInstance).addAll(clazz.cast(value));
							value = newInstance;
						}
						catch (Exception ex)
						{
							stateMachineLog.error(this.stateMachine.getContext().getName()+": Failed to clone substate Collection of type "+clazz, ex);
							value = (T) new ArrayList<Object>((Collection<Object>)value);
						}
					}
				}
				else if (value instanceof Map)
				{
					Class<? extends Map<Object, Object>> clazz = (Class<? extends Map<Object, Object>>) value.getClass();
					
					// Quite a few uses of Collections.singletonMap() for code readability. 
					// It is a private class, so just convert it to HashMap.  Do the same for all other private collections.
					// TODO better methods
					if (Modifier.isPrivate(clazz.getModifiers()))
						value = (T) new HashMap<Object, Object>((Map<Object, Object>)value);
					else
					{
						try
						{
							Object newInstance = clazz.getDeclaredConstructor().newInstance();
							clazz.cast(newInstance).putAll(clazz.cast(value));
							value = newInstance;
						}
						catch (Exception ex)
						{
							stateMachineLog.error(this.stateMachine.getContext().getName()+": Failed to clone substate Map of type "+clazz, ex);
							value = (T) new HashMap<Object, Object>((Map<Object, Object>)value);
						}
					}
				}
			}
			
			return (T) value;
		}
	}
	
	public <T, R> R set(final NativeField field, final T value)
	{
		Objects.requireNonNull(value, "Value is null");
		Objects.requireNonNull(field, "Field is null");
		
		throwIfSealed();
		
		return set(field.lower(), value);
	}

	@SuppressWarnings("unchecked")
	public <T, R> R set(String field, final T value)
	{
		Objects.requireNonNull(value, "Value is null");
		Objects.requireNonNull(field, "Field is null");
		
		throwIfSealed();
		
		field = Strings.toLowerCase(field);
		
		synchronized(this)
		{
			R replaced = (R) this.outputs.put(field, value);
			write(field, value);
			return replaced;
		}
	}
	
	private void read(final String field, final Object value)
	{
		synchronized(this)
		{
			int instruction = this.stateMachine.getCurrentInstruction();
			if (instruction == -1)
				throw new IllegalStateException("Instruction is invalid");
	
			SubstateTransitions instructionTransitions = this.transitions.get(instruction);
			if (instructionTransitions == null)
			{
				instructionTransitions = new SubstateTransitions(this.address, true);
				this.transitions.put(instruction, instructionTransitions);
			}
			instructionTransitions.read(field, value);
			
			SubstateTransitions executionTransitions = this.transitions.get(Integer.MAX_VALUE);
			if (executionTransitions == null)
			{
				executionTransitions = new SubstateTransitions(this.address, true);
				this.transitions.put(Integer.MAX_VALUE, executionTransitions);
			}
			executionTransitions.read(field, value);

			this.hasReads = true;
		}
	}
	
	private void write(final String field, final Object value)
	{
		synchronized(this)
		{
			int instruction = this.stateMachine.getCurrentInstruction();
			if (instruction == -1)
				throw new IllegalStateException("Instruction is invalid");
				
			SubstateTransitions instructionTransitions = this.transitions.get(instruction);
			if (instructionTransitions == null)
			{
				instructionTransitions = new SubstateTransitions(this.address, true);
				this.transitions.put(instruction, instructionTransitions);
			}
			instructionTransitions.write(field, value);
			
			SubstateTransitions executionTransitions = this.transitions.get(Integer.MAX_VALUE);
			if (executionTransitions == null)
			{
				executionTransitions = new SubstateTransitions(this.address, true);
				this.transitions.put(Integer.MAX_VALUE, executionTransitions);
			}
			executionTransitions.write(field, value);

			this.hasWrites = true;
		}
	}

	public boolean isTouched()
	{
		return this.hasReads || this.hasWrites;
	}
	
	boolean hasReads()
	{
		return this.hasReads;
	}

	boolean hasWrites()
	{
		return this.hasWrites;
	}
	
	public boolean isSealed()
	{
		return this.sealed;
	}
	
	private void throwIfSealed()
	{
		if (this.sealed)
			throw new IllegalStateException("Substate OP log is sealed for "+this.stateMachine.getPendingAtom().getHash()+" "+this.address);
	}
	
	void seal()
	{
		throwIfSealed();

		this.sealed = true;
	}
		
	public SubstateTransitions toSubstateTransitions()
	{
		if (this.sealed == false)
			throw new IllegalStateException("Substate log must be sealed to create substate transitions "+this.address);

		if (isVoid() == true && isTouched() == false)
			return this.untouched;

		final SubstateTransitions transitions = this.transitions.get(Integer.MAX_VALUE);
		if (transitions == null)
			return this.untouched;
		
		return transitions;
	}

	public SubstateTransitions toSubstateTransitions(final int instruction)
	{
		Numbers.isNegative(instruction, "Instruction index is invalid");
		
		if (this.sealed == false)
			throw new IllegalStateException("Substate log must be sealed to create substate transitions for instruction "+instruction+" at "+this.address);

		if (isVoid() == true && isTouched() == false)
			return this.untouched;
		
		final SubstateTransitions transitions = this.transitions.get(instruction);
		if (transitions == null)
			return this.untouched;
			
		return transitions;
	}
}
