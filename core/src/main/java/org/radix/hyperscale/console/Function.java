package org.radix.hyperscale.console;

import java.io.PrintStream;
import java.util.Objects;

import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.radix.hyperscale.Context;

public abstract class Function
{
	public static final CommandLineParser parser = new DefaultParser(); 
	
	private String 	name;
	private Options options;
	
	protected Function(String name, Options options)
	{
		this.name = Objects.requireNonNull(name);
		this.options = Objects.requireNonNull(options);
		
		if (name.contains(" "))
			throw new IllegalArgumentException("Function name '"+name+"' can not contain spaces");
	}
	
	public final String getName()
	{
		return this.name;
	}
	
	public final Options getOptions()
	{
		return this.options;
	}

	public abstract void execute(Context context, String[] arguments, PrintStream printStream) throws Exception;
}
