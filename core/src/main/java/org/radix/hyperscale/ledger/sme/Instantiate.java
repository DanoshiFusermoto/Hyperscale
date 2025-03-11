package org.radix.hyperscale.ledger.sme;

import java.util.Objects;
import java.util.regex.Pattern;

import org.radix.hyperscale.crypto.ComputeKey;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.crypto.Identity;
import org.radix.hyperscale.ledger.StateAddress;
import org.radix.hyperscale.ledger.StateContext;
import org.radix.hyperscale.ledger.StateLockMode;
import org.radix.hyperscale.ledger.Substate.NativeField;
import org.radix.hyperscale.ledger.sme.exceptions.StateMachineExecutionException;
import org.radix.hyperscale.utils.Strings;

public final class Instantiate extends ArgumentedInstruction
{
	private final String blueprintName;
	private final String componentName;
	private final Identity authority;
	private final StateAddress blueprintAddress;
	private final StateAddress componentAddress;
	private final StateAddress packageAddress;

	public Instantiate(final String blueprintName, final String componentName, final Identity authority)
	{
		this(blueprintName, componentName, authority, ArgumentedInstruction.EMPTY_ARGUMENTS);
	}

	public Instantiate(final String blueprintName, final String componentName, final Identity authority, final Object[] arguments)
	{
		super(arguments);
		
		Objects.requireNonNull(blueprintName, "Blueprint name is null");
		
		if (Pattern.matches("^[a-zA-Z0-9_.-]*$", blueprintName) == false)
			throw new IllegalArgumentException("Blueprint name "+blueprintName+" is invalid");

		if (Pattern.matches("^[a-zA-Z0-9_.-]*$", componentName) == false)
			throw new IllegalArgumentException("Component name "+componentName+" is invalid");
		
		this.blueprintName = Strings.toLowerCase(blueprintName);
		this.componentName = Strings.toLowerCase(componentName);
		this.authority = authority;
		this.blueprintAddress = StateAddress.from(Deploy.class.getAnnotation(StateContext.class).value(), Hash.valueOf(this.blueprintName));
		this.componentAddress = StateAddress.from(Component.class.getAnnotation(StateContext.class).value(), Hash.valueOf(this.componentName));
		this.packageAddress = StateAddress.from(Package.class.getAnnotation(StateContext.class).value(), Hash.valueOf(this.blueprintName));
	}
	
	public String getBlueprintName()
	{
		return this.blueprintName;
	}
	
	public StateAddress getBlueprintAddress()
	{
		return this.blueprintAddress;
	}

	public String getComponentName()
	{
		return this.componentName;
	}

	public StateAddress getComponentAddress()
	{
		return this.componentAddress;
	}
	
	public StateAddress getPackageAddress()
	{
		return this.packageAddress;
	}
	
	@Override
	public int hashCode() 
	{
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((authority == null) ? 0 : authority.hashCode());
		result = prime * result + ((blueprintName == null) ? 0 : blueprintName.hashCode());
		result = prime * result + ((componentName == null) ? 0 : componentName.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) 
	{
		if (this == obj)
			return true;
	
		if (!super.equals(obj))
			return false;
		
		if (getClass() != obj.getClass())
			return false;
		
		Instantiate other = (Instantiate) obj;
		if (authority == null) 
		{
			if (other.authority != null)
				return false;
		} 
		else if (!authority.equals(other.authority))
			return false;
		
		if (blueprintName == null) 
		{
			if (other.blueprintName != null)
				return false;
		} else if (!blueprintName.equals(other.blueprintName))
			return false;
		
		if (componentName == null) 
		{
			if (other.componentName != null)
				return false;
		} else if (!componentName.equals(other.componentName))
			return false;
		
		return true;
	}

	void lock(final StateMachine stateMachine) 
	{
		stateMachine.lock(getBlueprintAddress(), StateLockMode.READ);
		stateMachine.lock(getComponentAddress(), StateLockMode.WRITE);
		stateMachine.lock(StateAddress.from(Vault.class.getAnnotation(StateContext.class).value(), ComputeKey.from(getComponentAddress().getHash()).getIdentity()), StateLockMode.WRITE);
	}
	
	void execute(final StateMachine stateMachine) throws StateMachineExecutionException, ReflectiveOperationException
	{
		stateMachine.assertExists(getBlueprintAddress());
		
		stateMachine.assertCreate(getComponentAddress(), this.authority);
		
		stateMachine.vault(ComputeKey.from(getComponentAddress().getHash()).getIdentity());
		
		stateMachine.set(getComponentAddress(), NativeField.CODE, stateMachine.get(getBlueprintAddress(), NativeField.CODE), authority);
		stateMachine.set(getComponentAddress(), NativeField.BLUEPRINT, Strings.toLowerCase(this.blueprintName), this.authority);
		stateMachine.set(getComponentAddress(), NativeField.COMPONENT, Strings.toLowerCase(this.componentName), this.authority);
		stateMachine.set(getComponentAddress(), NativeField.LANGUAGE, stateMachine.get(getBlueprintAddress(), NativeField.LANGUAGE), authority);

		stateMachine.associate(getComponentAddress(), getBlueprintAddress().getHash());
		stateMachine.associate(getComponentAddress(), this.authority);
		
		stateMachine.call(getComponentAddress(), "constructor", getArguments());
	}
}
