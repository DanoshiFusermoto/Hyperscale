package org.radix.hyperscale.console;

import java.io.PrintStream;
import java.lang.System;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.Range;
import org.radix.hyperscale.Context;
import org.radix.hyperscale.ledger.BlockHeader.InventoryType;
import org.radix.hyperscale.ledger.ShardGroupID;
import org.radix.hyperscale.tools.spam.SpamConfig;
import org.radix.hyperscale.tools.spam.Spamathon;
import org.radix.hyperscale.tools.spam.Spammer;

public class Spam extends Function
{
	private static final Options options = new Options().addOption("i", "iterations", true, "Quantity of spam iterations").
														 addOption("r", "rate", true, "Rate at which to produce events").
														 addOption("spammer", true, "Which spammer to run").
														 addOption("saturation", true, "Saturation range per atom").
														 addOption("shardtarget", true, "The shard group to target for all atoms").
														 addOption("shardfactor", true, "Factor of forced single shard atoms").
														 addOption("profile", false, "Profile the spam and output stats").
														 addOption("cancel", false, "Cancels all currently running spammers");

	public Spam()
	{
		super("spam", options);
	}

	@Override
	public void execute(Context context, String[] arguments, PrintStream printStream) throws Exception
	{
		if (hasRawArgument(arguments, "cancel"))
		{
			Spamathon.getInstance().cancel();
			return;
		}
		
		if (Spamathon.getInstance().isSpamming())
			throw new IllegalStateException("Already an instance of spammer running");
		
		final String name = getRawArgument(arguments, "spammer", "unique");
		final Class<? extends Spammer> spamClass = Spamathon.getInstance().get(name);
		if (spamClass == null)
			throw new InstantiationException("Spam plugin class is not found: "+name);
		
		// Create custom options parser for this invocation
		final Options extendedOptions = new Options();
		for (Option option : Spam.options.getOptions())
			extendedOptions.addOption(option);
		
		// Add the additional spam options if there are any
		SpamConfig spamConfig = spamClass.getAnnotation(SpamConfig.class);
		if (spamConfig.options().length > 0)
		{
			for (SpamConfig.Option optionAnnotation : spamConfig.options()) 
			{
	            Option extendedOption = Option.builder(optionAnnotation.opt())
	            										.longOpt(optionAnnotation.longOpt().isEmpty() ? optionAnnotation.opt() : optionAnnotation.longOpt())
	            										.desc(optionAnnotation.description())
	            										.hasArg(optionAnnotation.hasArg())
	            										.required(optionAnnotation.required())
	            										.build();
	            
	            extendedOptions.addOption(extendedOption);
	        }
		}

		final CommandLine commandLine = Function.parser.parse(extendedOptions, arguments, false);
		final String[] extendedArguments = getExtendedArguments(commandLine, spamConfig);
				
		final Spammer spammer = Spamathon.getInstance().spam(name, 
															 Integer.parseInt(commandLine.getOptionValue("iterations")), 
															 Integer.parseInt(commandLine.getOptionValue("rate")), 
															 parseRange(commandLine.getOptionValue("saturation", "1")),
												   			 Double.parseDouble(commandLine.getOptionValue("shardfactor", "0")), 
												   			 commandLine.hasOption("shardTarget") ? ShardGroupID.from(Integer.parseInt(commandLine.getOptionValue("shardtarget"))) : null, 
												   			 extendedArguments);

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
	
	private String[] getExtendedArguments(CommandLine commandLine, SpamConfig spamConfig) 
	{
	    List<String> extendedArgs = new ArrayList<>();
	    
	    // Process all options from the SpamConfig annotation
	    if (spamConfig != null && spamConfig.options().length > 0) 
	    {
	        for (SpamConfig.Option optionAnnotation : spamConfig.options()) 
	        {
	            String opt = optionAnnotation.opt();
	            String longOpt = optionAnnotation.longOpt().isEmpty() ? opt : optionAnnotation.longOpt();
	            
	            // Check if this option was provided in the command line
	            if (commandLine.hasOption(opt) || commandLine.hasOption(longOpt)) 
	            {
	                // Add the option with appropriate prefix (- for short, -- for long)
	                String optKey = optionAnnotation.longOpt().isEmpty() ? "-" + opt : "--" + longOpt;
	                extendedArgs.add(optKey);
	                
	                // If the option has an argument, add its value
	                if (optionAnnotation.hasArg()) 
	                {
	                    String value = commandLine.getOptionValue(opt);
	                    if (value != null)
	                        extendedArgs.add(value);
	                }
	            }
	        }
	    }
	    
	    // Also include any remaining arguments
	    String[] remainingArgs = commandLine.getArgs();
	    if (remainingArgs != null && remainingArgs.length > 0)
	        Collections.addAll(extendedArgs, remainingArgs);
	    
	    return extendedArgs.toArray(new String[0]);
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

	boolean hasRawArgument(String[] arguments, String option)
	{
		for (int i = 0 ; i < arguments.length ; i++)
		{
			final String arg = arguments[i];
			String trimmedArg;
			if (arg.startsWith("-"))
				trimmedArg = arg.substring(1);
			else if (arg.startsWith("--"))
				trimmedArg = arg.substring(2);
			else trimmedArg = arg;
			
			if (trimmedArg.equalsIgnoreCase(option) == false)
				continue;
			
			return true;
		}
		
		return false;
	}
	
	<T> T getRawArgument(String[] arguments, String option, T _default)
	{
		for (int i = 0 ; i < arguments.length ; i++)
		{
			final String arg = arguments[i];
			String trimmedArg;
			if (arg.startsWith("-"))
				trimmedArg = arg.substring(1);
			else if (arg.startsWith("--"))
				trimmedArg = arg.substring(2);
			else trimmedArg = arg;
			
			if (trimmedArg.equalsIgnoreCase(option) == false)
				continue;
			
			return (T) arguments[i+1];
		}
		
		return _default;
	}
}

