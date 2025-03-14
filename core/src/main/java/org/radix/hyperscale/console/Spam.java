package org.radix.hyperscale.console;

import java.io.PrintStream;
import java.lang.System;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.Range;
import org.radix.hyperscale.Context;
import org.radix.hyperscale.ledger.BlockHeader.InventoryType;
import org.radix.hyperscale.ledger.ShardGroupID;
import org.radix.hyperscale.tools.spam.Spamathon;
import org.radix.hyperscale.tools.spam.Spammer;

public class Spam extends Function
{
	private static final Options options = new Options().addOption("i", "iterations", true, "Quantity of spam iterations").
														 addOption("r", "rate", true, "Rate at which to produce events").
														 addOption("spammer", true, "Which spammer to run").
														 addOption("saturation", true, "Saturation range per atom (SwapPoolSpammer, UniqueValueSpammer)").
														 addOption("shardtarget", true, "The shard group to target for all atoms (UniqueValueSpammer)").
														 addOption("shardfactor", true, "Factor of forced single shard atoms (UniqueValueSpammer, TokenTransferSpammer)").
														 addOption("fundingsource", true, "File path to wallet key to use as funding source (TokenTransferSpammer)").
														 addOption("destinations", true, "List of comma delimited idenitites to include as destinations (TokenTransferSpammer)").
														 addOption("pools", true, "Quantity of pools to generate (SwapPoolSpammer)").
														 addOption("wallets", true, "Quantity of wallets to generate (SwapPoolSpammer)").
														 addOption("profile", false, "Profile the spam and output stats").
														 addOption("cancel", false, "Cancels all currently running spammers");

	public Spam()
	{
		super("spam", options);
	}

	@Override
	public void execute(Context context, String[] arguments, PrintStream printStream) throws Exception
	{
		CommandLine commandLine = Function.parser.parse(options, arguments);
		
		if (commandLine.hasOption("cancel"))
		{
			Spamathon.getInstance().cancel();
			return;
		}
		
		if (Spamathon.getInstance().isSpamming())
			throw new IllegalStateException("Already an instance of spammer running");

		final int rate = Integer.parseInt(commandLine.getOptionValue("rate"));
		final Range<Integer> saturation = parseRange(commandLine.getOptionValue("saturation", "1"));
		
		final String name = commandLine.getOptionValue("spammer", "unique");
		final Spammer spammer = Spamathon.getInstance().spam(name, 
															 Integer.parseInt(commandLine.getOptionValue("iterations")), 
												   			 rate, saturation,
			   			  									 commandLine.hasOption("shardTarget") ? ShardGroupID.from(Integer.parseInt(commandLine.getOptionValue("shardtarget"))) : null,
												   			 Double.parseDouble(commandLine.getOptionValue("shardfactor", "0")));

		printStream.println("Starting spam "+name+" of "+commandLine.getOptionValue("iterations")+" iterations at rate of "+commandLine.getOptionValue("rate")+" ... ");

		if (commandLine.hasOption("profile"))
		{
			long start = System.currentTimeMillis();
			long totalAtomsCommittedCount = 0;
			long lastCommittedBlockHeight = context.getLedger().getHead().getHeight();
			while(spammer.getProcessed().get() != spammer.getIterations())
			{
				Thread.sleep(100);
				
				if (context.getLedger().getHead().getHeight() > lastCommittedBlockHeight)
				{
					long duration = System.currentTimeMillis()-start;
					long committedAtoms = context.getLedger().getHead().getInventory(InventoryType.ACCEPTED).size();
					totalAtomsCommittedCount += committedAtoms;
					printStream.println("Atoms: "+totalAtomsCommittedCount+" TPS: "+(totalAtomsCommittedCount / TimeUnit.MILLISECONDS.toSeconds(duration)));
					lastCommittedBlockHeight = context.getLedger().getHead().getHeight();
				}
			}
			
			printStream.println("Completed local spam, waiting for network...");
			
			while(Spamathon.getInstance().isSpamming())
			{
				Thread.sleep(1000);
			
				if (context.getLedger().getHead().getHeight() > lastCommittedBlockHeight)
				{
					long duration = System.currentTimeMillis()-start;
					long committedAtoms = context.getLedger().getHead().getInventory(InventoryType.ACCEPTED).size();
					totalAtomsCommittedCount += committedAtoms;
					printStream.println("Atoms: "+totalAtomsCommittedCount+" TPS: "+(totalAtomsCommittedCount / TimeUnit.MILLISECONDS.toSeconds(duration)));
					lastCommittedBlockHeight = context.getLedger().getHead().getHeight();
				}
			}

			long duration = System.currentTimeMillis()-start;
			printStream.println("Spam took "+duration+" to process "+totalAtomsCommittedCount+" atoms @ average rate of "+(totalAtomsCommittedCount / TimeUnit.MILLISECONDS.toSeconds(duration))+" TPS");
		}
	}
	
	private Range<Integer> parseRange(final String range)
	{
		if (range.contains("-") == false)
		{
			Integer value = Integer.parseInt(range);
			return Range.between(value, value);
		}
		else
		{
			String patternStr = "(\\d+)-(\\d+)";
			Pattern pattern = Pattern.compile(patternStr);
			Matcher matcher = pattern.matcher(range);
			if (matcher.matches())
			    return Range.between(Integer.parseInt(matcher.group(1)), Integer.parseInt(matcher.group(2)));
			
			throw new NumberFormatException("Integer range format invalid for '"+range+"'");
		}
	}
}

