package org.radix.hyperscale.ledger.sme;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.radix.hyperscale.crypto.ComputeKey;
import org.radix.hyperscale.crypto.Identity;
import org.radix.hyperscale.ledger.StateAddress;
import org.radix.hyperscale.ledger.StateContext;
import org.radix.hyperscale.ledger.Substate;
import org.radix.hyperscale.ledger.Substate.NativeField;
import org.radix.hyperscale.utils.Strings;

public abstract class NativeComponent extends Component
{
	private static final Map<Class<? extends NativeComponent>, MethodInfo[]> methods = Collections.synchronizedMap(new HashMap<>());
	
	private static MethodInfo[] computeComponentMethodInfo(Class<? extends NativeComponent> clazz)
	{
		Method[] methods = clazz.getMethods();
		MethodInfo[] methodInfoArray = new MethodInfo[methods.length];
		for (int i = 0 ; i < methods.length ; i++)
			methodInfoArray[i] = new MethodInfo(methods[i], methods[i].getParameterTypes());
		
		return methodInfoArray;
	}
	
	private static class MethodInfo
	{
		private final Method method;
		private final Class<?>[] parameterTypes;
		
		private MethodInfo(final Method method, final Class<?>[]  parameterTypes)
		{
			this.method = method;
			this.parameterTypes = parameterTypes;
		}

		protected Method getMethod() 
		{
			return this.method;
		}

		protected Class<?>[] getParameterTypes() 
		{
			return this.parameterTypes;
		}
	}
	
	private static final Map<Class<? extends NativeComponent>, Substate> nativeComponentSubstates = new HashMap<>();
	
	private static final SubstateLog createNativeComponentSubstate(StateMachine stateMachine, Class<? extends NativeComponent> clazz)
	{
		Substate nativeComponentSubstate;
		synchronized(nativeComponentSubstates)
		{
			nativeComponentSubstate = nativeComponentSubstates.get(clazz);
			if (nativeComponentSubstate == null)
			{
				String component = Strings.toLowerCase(clazz.getAnnotation(StateContext.class).value());
				nativeComponentSubstate = new Substate(StateAddress.from(Component.class, component)).set(NativeField.COMPONENT, component).set(NativeField.AUTHORITY, ComputeKey.NULL.getIdentity());
				nativeComponentSubstates.put(clazz, nativeComponentSubstate);
			}
			
			// TODO validate authority is ComputeKey.NULL
		}
		
		// TODO make immutable!
		return new SubstateLog(stateMachine, nativeComponentSubstate, true);
	}
	
	protected NativeComponent(StateMachine stateMachine, Class<? extends NativeComponent> clazz)
	{
		super(stateMachine, createNativeComponentSubstate(stateMachine, clazz));
	}
	
	@Override
	protected void close() 
	{
	}

	protected final <T> void set(final StateAddress stateAddress, NativeField field, T value)
	{
		getStateMachine().set(stateAddress, field, value, getIdentity());
	}
	
	@Override
	protected final void prepare(String method, Object[] arguments) throws ReflectiveOperationException
	{
		final MethodInfo[] methodInfo = NativeComponent.methods.computeIfAbsent(getClass(), NativeComponent::computeComponentMethodInfo);
		for (int m = 0 ; m < methodInfo.length ; m++) 
		{
	        if (methodInfo[m].getMethod().getName().equals("lock") == false)
	            continue;

	        Class<?>[] parameterTypes = methodInfo[m].getParameterTypes();
	        if (parameterTypes.length != 2)
	        	continue;
	        
	        if (parameterTypes[0].isAssignableFrom(String.class) == false)
	        	continue;

	        if (parameterTypes[1].isAssignableFrom(Object[].class) == false)
	        	continue;
	        
	        Object[] methodArguments = new Object[arguments.length];
			for(int i = 0 ; i < arguments.length ; i++)
	        {
	        	if (arguments[i] instanceof Argument argument)
	        		methodArguments[i] = argument.get();
	        	else
	        		methodArguments[i] = arguments[i];
	        }

			methodInfo[m].getMethod().invoke(this, method, methodArguments);
        	return;
	    }

		// TODO need to throw?
//        throw new NoSuchMethodException(getClass().getName() + "." + "lock(method, args) not found");
	}

	@Override
	public final Identity getIdentity()
	{
		return ComputeKey.NULL.getIdentity();
	}
	
	public void lock(String method, Object[] arguments) 
	{
		// STUB
	}

	@SuppressWarnings("unchecked")
	@Override
	protected final <T> T call(String method, Object[] arguments) throws ReflectiveOperationException
	{
        Object[] methodArguments = new Object[arguments.length];
		for(int i = 0 ; i < arguments.length ; i++)
        {
        	if (arguments[i] instanceof Argument argument)
        		methodArguments[i] = argument.get();
        	else
        		methodArguments[i] = arguments[i];
        }

		final MethodInfo[] methodInfo = NativeComponent.methods.computeIfAbsent(getClass(), NativeComponent::computeComponentMethodInfo);
		for (int m = 0 ; m < methodInfo.length ; m++) 
		{
	        if (methodInfo[m].getMethod().getName().equals(method) == false)
	            continue;

	        Class<?>[] parameterTypes = methodInfo[m].getParameterTypes();
	        if (parameterTypes.length != arguments.length)
	        	continue;
	        
	        boolean matches = true;
			for(int a = 0 ; a < methodArguments.length ; a++)
	        {
 	            if (parameterTypes[a].isAssignableFrom(methodArguments[a].getClass()) == false) 
	            {
	                matches = false;
	                break;
	            }
	        }
	        
	        if (matches)
	        	return (T) methodInfo[m].getMethod().invoke(this, methodArguments);
		}

        throw new NoSuchMethodException(getClass().getName() + "." + method + "("+Arrays.toString(methodArguments)+")");
	}
}
