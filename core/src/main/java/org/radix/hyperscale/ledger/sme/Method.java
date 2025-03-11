package org.radix.hyperscale.ledger.sme;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.radix.hyperscale.ledger.StateAddress;
import org.radix.hyperscale.utils.Numbers;
import org.radix.hyperscale.utils.Strings;

public class Method extends ArgumentedInstruction
{
	private static final Pattern methodPattern = Pattern.compile("^[a-zA-Z0-9_.-]*$");
	private static final ThreadLocal<Matcher> methodMatcher = ThreadLocal.withInitial(() -> methodPattern.matcher(""));

	public static final int CONTEXT_NAME_MIN_LENGTH = 3; 
	public static final int CONTEXT_NAME_MAX_LENGTH = 64; 

	public static final int METHOD_NAME_MIN_LENGTH = 1; 
	public static final int METHOD_NAME_MAX_LENGTH = 64; 
	
	private final boolean _native;
	private final String method;
	private final String componentName;
	private final StateAddress componentAddress;

	public Method(final Class<? extends NativeComponent> componentClass, final String method, final Object[] arguments)
	{
		this(Component.getContext(Objects.requireNonNull(componentClass, "Native component class is null")), method, arguments, true);
	}

	public Method(final String componentName, final String method, final Object[] arguments)
	{
		this(StateAddress.from(Component.class, componentName), componentName, method, arguments, false);
	}
	
	private Method(final String componentName, final String method, final Object[] arguments, final boolean _native)
	{
		this(StateAddress.from(Component.class, componentName), componentName, method, arguments, _native);
	}

	Method(final StateAddress componentAddress, final String componentName, final String method, final Object[] arguments, final boolean _native)
	{
		super(arguments);
		
		Objects.requireNonNull(componentName, "Component name is null");
		Numbers.inRange(componentName.length(), CONTEXT_NAME_MIN_LENGTH, CONTEXT_NAME_MAX_LENGTH, "Component name length is invalid");
		Objects.requireNonNull(method, "Component method is null");
		Numbers.inRange(method.length(), METHOD_NAME_MIN_LENGTH, METHOD_NAME_MAX_LENGTH, "Component name length is invalid");
		Objects.requireNonNull(arguments, "Component method arguments is null");
		
		if (methodMatcher.get().reset(componentName).matches() == false)
			throw new IllegalArgumentException("Component name "+componentName+" is invalid");

		if (methodMatcher.get().reset(method).matches() == false)
			throw new IllegalArgumentException("Component method "+method+" is invalid");

		this.componentName = Strings.toLowerCase(componentName);
		this.method = method;
		this._native = _native;
		this.componentAddress = componentAddress;
	}
	
	@Override
	public int hashCode() 
	{
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((componentAddress == null) ? 0 : componentAddress.hashCode());
		result = prime * result + ((componentName == null) ? 0 : componentName.hashCode());
		result = prime * result + ((method == null) ? 0 : method.hashCode());
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
		
		Method other = (Method) obj;
		if (componentAddress == null) 
		{
			if (other.componentAddress != null)
				return false;
		} 
		else if (!componentAddress.equals(other.componentAddress))
			return false;
		
		if (componentName == null) 
		{
			if (other.componentName != null)
				return false;
		} 
		else if (!componentName.equals(other.componentName))
			return false;
		
		if (method == null) 
		{
			if (other.method != null)
				return false;
		} 
		else if (!method.equals(other.method))
			return false;
		
		return true;
	}

	StateAddress getComponentAddress()
	{
		return this.componentAddress;
	}

	public String getComponentName()
	{
		return this.componentName;
	}
	
	public String getMethod()
	{
		return this.method;
	}
	
	public boolean isNative()
	{
		return this._native;
	}
}
