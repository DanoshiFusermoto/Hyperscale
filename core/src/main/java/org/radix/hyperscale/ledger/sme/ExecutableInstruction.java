package org.radix.hyperscale.ledger.sme;

import java.lang.reflect.Method;
import java.util.Arrays;

abstract class ExecutableInstruction extends ArgumentedInstruction
{ 
	ExecutableInstruction(Object[] arguments)
	{
		super(arguments);
	}
	
	protected final void lock(StateMachine stateMachine, Object[] arguments) throws ReflectiveOperationException
	{
		Object[] extendedArguments = new Object[arguments.length+1];
		Class<?>[] argumentClasses = new Class<?>[arguments.length+1];
		extendedArguments[0] = stateMachine; argumentClasses[0] = StateMachine.class;
		for(int a = 0 ; a < arguments.length ; a++)
		{
			extendedArguments[a+1] = arguments[a];
			argumentClasses[a+1] = arguments[a].getClass();
		}
		
		Method[] methods = this.getClass().getDeclaredMethods();
		for (Method methref : methods) 
		{
	        if (!methref.getName().equals("lock"))
	            continue;

	        Class<?>[] parameterTypes = methref.getParameterTypes();
	        if (parameterTypes.length != extendedArguments.length)
	        	continue;

	        boolean matches = true;
	        for (int i = 0; i < extendedArguments.length; i++) 
	        {
	            if (!parameterTypes[i].isAssignableFrom(extendedArguments[i].getClass())) 
	            {
	                matches = false;
	                break;
	            }
	        }
	        
	        if (matches)
	        {
	        	methref.invoke(this, extendedArguments);
	        	return;
	        }
	    }

        throw new NoSuchMethodException(getClass().getName() + ".lock("+ Arrays.toString(argumentClasses)+")");
	}
	
	@SuppressWarnings("unchecked")
	protected final <T> T execute(StateMachine stateMachine, Object[] arguments) throws ReflectiveOperationException
	{
		Object[] extendedArguments = new Object[arguments.length+1];
		Class<?>[] argumentClasses = new Class<?>[arguments.length+1];
		extendedArguments[0] = stateMachine; argumentClasses[0] = StateMachine.class;
		for(int a = 0 ; a < arguments.length ; a++)
		{
			extendedArguments[a+1] = arguments[a];
			argumentClasses[a+1] = arguments[a].getClass();
		}
		
		Method[] methods = getClass().getDeclaredMethods();
		for (Method methref : methods) 
		{
	        if (!methref.getName().equals("execute"))
	            continue;

	        Class<?>[] parameterTypes = methref.getParameterTypes();
	        if (parameterTypes.length != extendedArguments.length)
	        	continue;
	        
	        boolean matches = true;
	        for (int i = 0; i < extendedArguments.length; i++) 
	        {
	            if (!parameterTypes[i].isAssignableFrom(extendedArguments[i].getClass())) 
	            {
	                matches = false;
	                break;
	            }
	        }
	        
	        if (matches)
	        	return (T) methref.invoke(this, extendedArguments);
	    }

        throw new NoSuchMethodException(getClass().getName() + ".execute("+Arrays.toString(argumentClasses)+")");
	}
}