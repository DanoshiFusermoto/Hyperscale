package org.radix.hyperscale.console;

import java.io.File;
import java.io.PrintStream;
import java.util.Collection;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.radix.hyperscale.Context;
import org.radix.hyperscale.crypto.ed25519.EDKeyPair;
import org.radix.hyperscale.network.AbstractConnection;
import org.radix.hyperscale.network.StandardConnectionFilter;
import org.radix.hyperscale.network.messages.KillMessage;

public class Godix extends Function
{
	private static final Options options = new Options().addOption(Option.builder("killnetwork").desc("Forcibly attempts to kill all nodes in the network").optionalArg(true).numberOfArgs(2).build());
	
	public Godix()
	{
		super("godix", options);
	}

	@Override
	public void execute(final Context context, final String[] arguments, final PrintStream printStream) throws Exception
	{
		CommandLine commandLine = Function.parser.parse(options, arguments);

		if (commandLine.hasOption("killnetwork"))
		{
			final String[] optionValues = commandLine.getOptionValues("killnetwork"); 
			if (optionValues.length > 2)
				throw new IllegalArgumentException("Too many arguments");

			boolean bootstraps = false;
			File godixKeypath = new File("universe.key");
			for(final String arg : optionValues)
			{
				if (arg.equalsIgnoreCase("true") || arg.equalsIgnoreCase("false"))
					bootstraps = Boolean.parseBoolean(arg);
				else
					godixKeypath = new File(arg);
			}
			
			final EDKeyPair godixKeyPair = EDKeyPair.fromFile(godixKeypath, false);
			final KillMessage killMessage = new KillMessage(bootstraps, godixKeyPair);
			
			final Collection<AbstractConnection> allConnections = context.getNetwork().get(StandardConnectionFilter.build(context));
			for (final AbstractConnection connection : allConnections)
				context.getNetwork().getMessaging().send(killMessage, connection);
		}
	}
}
