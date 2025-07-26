package org.radix.hyperscale.console;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.radix.hyperscale.Context;
import org.radix.hyperscale.crypto.ed25519.EDKeyPair;
import org.radix.hyperscale.network.AbstractConnection;
import org.radix.hyperscale.network.ConnectionState;
import org.radix.hyperscale.network.StandardConnectionFilter;
import org.radix.hyperscale.network.messages.BlobMessage;
import org.radix.hyperscale.network.messages.KillMessage;
import org.radix.hyperscale.time.Time;

import io.netty.util.internal.ThreadLocalRandom;

public class Godix extends Function
{
	private static final Options options = new Options().addOption(Option.builder("killnetwork").desc("Forcibly attempts to kill all nodes in the network").optionalArg(true).numberOfArgs(2).build())
														.addOption(Option.builder("flood").desc("Floods the network with junk data").numberOfArgs(3).build());

	
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
		else if (commandLine.hasOption("flood"))
		{
			final String[] optionValues = commandLine.getOptionValues("flood"); 
			if (optionValues.length > 3)
				throw new IllegalArgumentException("Too many arguments");
			
			doFlood(context, optionValues, printStream);
		}
	}
	
	private void doFlood(final Context context, final String[] optionValues, final PrintStream printStream)
	{
		final int iterations = Integer.parseInt(optionValues[0]);
		final int rate = Integer.parseInt(optionValues[1]);
		final int payloadSize = Integer.parseInt(optionValues[2]);
		
		final Runnable floodRunnable = new Runnable()
		{
			int remaining = iterations;
			byte[] buffer = new byte[payloadSize];
			
			@Override
			public void run()
			{
				long lastReport = Time.getSystemTime();
				while(this.remaining > 0)
				{
					long start = Time.getSystemTime();
					
					final List<AbstractConnection> allConnections = context.getNetwork().get(StandardConnectionFilter.build(context).setStates(ConnectionState.SELECT_CONNECTED));
					for (int r = 0 ; r < rate && this.remaining > 0; r++)
					{
						ThreadLocalRandom.current().nextBytes(this.buffer);
						final BlobMessage blobMessage = new BlobMessage(this.buffer);
						final AbstractConnection connection = allConnections.get(ThreadLocalRandom.current().nextInt(allConnections.size()));
						try
						{
							context.getNetwork().getMessaging().send(blobMessage, connection);
						} 
						catch (IOException e)
						{
							e.printStackTrace(printStream);
						}
						
						this.remaining--;
					}
					
					long duration = Time.getSystemTime() - start;
					if (duration < 1000)
					{
						try
						{
							Thread.sleep(1000 - duration);
						} 
						catch (InterruptedException e)
						{
							Thread.interrupted();
						}
					}
					
					if (Time.getSystemTime() - lastReport >= TimeUnit.SECONDS.toMillis(10))
					{
						printStream.printf("Network Flood: iterations=%d/%d\n", (iterations-remaining), iterations);
						lastReport = Time.getSystemTime();
					}
				}
			}
		};
		
		final Thread floodThread = new Thread(floodRunnable, "Network Flooder");
		floodThread.setDaemon(true);
		floodThread.start();
	}
}
