package org.radix.hyperscale.console;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.lang.System;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Scanner;
import java.util.StringTokenizer;

import org.radix.hyperscale.Context;

public class Console
{
	private final Scanner 		scanner;
	private final PrintStream 	printStream;

	private final Map<String, Function> functions = new HashMap<String, Function>();
	
	public Console(InputStream inputStream, PrintStream printStream, Function ... functions)
	{
		this.scanner = new Scanner(Objects.requireNonNull(inputStream));
		this.printStream = Objects.requireNonNull(printStream);
		
		registerFunctions(functions);

		run();
	}

	public Console(InputStream inputStream, OutputStream outputStream, Function ... functions)
	{
		this.scanner = new Scanner(Objects.requireNonNull(inputStream));
		this.printStream = new PrintStream(Objects.requireNonNull(outputStream));
		
		registerFunctions(functions);
		
		run();
	}

	private void defaultFunctions()
	{
		register(new Echo());
	}

	private void registerFunctions(Function ... functions)
	{
		defaultFunctions();
		
		if (functions != null)
		{
			for (int f = 0 ; f < functions.length ; f++)
			{
				register(functions[f]);
			}
		}
	}
	
	private void run()
	{
		this.printStream.print("> ");
		
		String command = "";
		String arguments = "";
		while(command.equalsIgnoreCase("exit") == false && this.scanner.hasNext())
		{
			String input = this.scanner.nextLine();
			input = input.trim();
			
			command = "";
			arguments = "";
			
			// No command
			if (input.isEmpty())
			{
				this.printStream.println();
				this.printStream.print("> ");
				continue;
			}
			
			final int contextIndex = input.indexOf(":");
			final Context context;
			if (contextIndex == -1)
				context = Context.get();
			else
			{
				String contextName = input.substring(0, contextIndex);
				if (contextName.indexOf(" ") == -1)
				{
					context = Context.get(contextName);
					input = input.substring(contextIndex+1);
				}
				else
					context = Context.get();
			}
			
			final int spaceIndex = input.indexOf(" ");
			if (spaceIndex == -1)
				command = input;
			else
			{
				command = input.substring(0, spaceIndex);
				arguments = input.substring(spaceIndex+1);
			}
			
			final Function function;
			synchronized(this.functions)
			{
				function = this.functions.get(command.toLowerCase());
			}

			if (command.equalsIgnoreCase("exit"))
			{
				System.exit(0);
			}
			else
			{
				try
				{
					if (context == null)
					{
						this.printStream.println("Context not found");
						continue;
					}

					if (function == null)
					{
						this.printStream.println("Function '"+command.toLowerCase()+"' is not found");
						continue;
					}

					final StringTokenizer argumentTokenizer = new StringTokenizer(arguments);
					final String[] tokenizedArguments = new String[argumentTokenizer.countTokens()];
	
					for (int i = 0 ; i < tokenizedArguments.length ; i++)
						tokenizedArguments[i] = argumentTokenizer.nextToken();
					
					try
					{
						function.execute(context, tokenizedArguments, this.printStream);
					}
					catch (Exception e)
					{
						this.printStream.println("Function '"+function.getName()+"' produced error:");
						e.printStackTrace(this.printStream);
					}
				}
				finally
				{
					this.printStream.print("> ");
				}
			}
		}
	}
	
	public void register(Function function)
	{
		Objects.requireNonNull(function);
		
		synchronized(this.functions)
		{
			if (this.functions.containsKey(function.getName().toLowerCase()))
				throw new IllegalStateException("Function "+function.getName()+" is already registered");
			
			this.functions.put(function.getName().toLowerCase(), function);
		}
	}
	
	public boolean deregister(String function)
	{
		Objects.requireNonNull(function);
		
		synchronized(this.functions)
		{
			if (this.functions.remove(function) == null)
				return false;
			
			return true;
		}
	}
}
