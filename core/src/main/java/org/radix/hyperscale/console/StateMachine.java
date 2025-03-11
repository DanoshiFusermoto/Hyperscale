package org.radix.hyperscale.console;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FileUtils;
import org.json.JSONObject;
import org.radix.hyperscale.Context;
import org.radix.hyperscale.apps.SimpleWallet;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.crypto.Identity;
import org.radix.hyperscale.crypto.KeyPair;
import org.radix.hyperscale.crypto.ed25519.EDKeyPair;
import org.radix.hyperscale.ledger.Isolation;
import org.radix.hyperscale.ledger.StateAddress;
import org.radix.hyperscale.ledger.SubstateSearchQuery;
import org.radix.hyperscale.ledger.SubstateSearchResponse;
import org.radix.hyperscale.ledger.primitives.Atom;
import org.radix.hyperscale.ledger.primitives.Blob;
import org.radix.hyperscale.ledger.sme.ManifestParser;
import org.radix.hyperscale.serialization.Serialization;
import org.radix.hyperscale.serialization.DsonOutput.Output;

import com.google.common.io.BaseEncoding;

public class StateMachine extends Function
{
	private static final Options options = new Options().addOption(Option.builder("construct").desc("Starts an atom construction").build())
														.addOption(Option.builder("sign").desc("Signs an atom under construction").build())
														.addOption(Option.builder("view").desc("Views an atom under construction").build())
														.addOption(Option.builder("submit").desc("Submits a constructed atom").build())
														.addOption(Option.builder("discard").desc("Discards an atom under contruction").build())
														
														.addOption(Option.builder("deploy").desc("Deploy a blueprint").numberOfArgs(4).build())
														.addOption(Option.builder("instantiate").desc("Instantiate a blueprint").numberOfArgs(3).build())
														.addOption(Option.builder("constructor").desc("Calls a components constructor").numberOfArgs(1).build())
														.addOption(Option.builder("method").desc("Calls a component method").numberOfArgs(2).build())
														.addOption(Option.builder("substate").desc("Gets a substate").numberOfArgs(2).build());

	Atom atom = null;
	
	public StateMachine()
	{
		super("sm", options);
	}

	@Override
	public void execute(Context context, String[] arguments, PrintStream printStream) throws Exception
	{
		SimpleWallet wallet = Wallet.get(context);
		CommandLine commandLine = Function.parser.parse(options, arguments);

		if (commandLine.hasOption("construct"))
		{
			if(this.atom != null)
			{
				printStream.println("ERROR: An atom is already under construction");
				return;
			}

			this.atom = new Atom();
		}
		else if (commandLine.hasOption("sign"))
		{
			if(this.atom == null)
			{
				printStream.println("ERROR: An atom is not under construction");
				return;
			}
			
			KeyPair<?,?,?> key;
			if (commandLine.getArgs().length != 0)
			{
				String keyStr = commandLine.getArgs()[0];
				key = new EDKeyPair(BaseEncoding.base16().decode(keyStr));
			}
			else
			{
				if (wallet == null)
					throw new IllegalStateException("No wallet is open to sign");
				
				key = wallet.getKey();
			}
			
			this.atom.sign(key);
		}
		else if (commandLine.hasOption("submit"))
		{
			if(this.atom == null)
			{
				printStream.println("ERROR: An atom is not under construction");
				return;
			}
			
			printStream.println("Submitting atom "+this.atom.getHash()+" -> "+Serialization.getInstance().toJsonObject(atom, Output.API).toString(4));
			
			try
			{
				// Check manifest syntax
				ManifestParser.parse(this.atom.getManifest());
				
				if (context.getLedger().submit(this.atom) == false)
					printStream.println("Failed to submit atom "+this.atom);
			}
			finally
			{
				this.atom = null;
			}
		}
		else if (commandLine.hasOption("discard"))
		{
			if(this.atom == null)
			{
				printStream.println("ERROR: An atom is not under construction");
				return;
			}
			
			this.atom = null;
		}
		else if (commandLine.hasOption("view"))
		{
			if(this.atom == null)
			{
				printStream.println("ERROR: An atom is not under construction");
				return;
			}
			
			printStream.println(this.atom.getHash()+" -> "+Serialization.getInstance().toJsonObject(atom, Output.API).toString(4));
		}

		else if (commandLine.hasOption("deploy"))
		{
			String[] deployOptionValues = commandLine.getOptionValues("deploy");
			File blueprintFile = new File(deployOptionValues[0]);
			if (blueprintFile.exists() == false)
				throw new FileNotFoundException(blueprintFile.toString());
			if (blueprintFile.isDirectory())
				throw new IllegalArgumentException(blueprintFile+" is a directory");

			String blueprintLanguage = deployOptionValues[1].toLowerCase();

			String blueprintName = deployOptionValues[2].toLowerCase();
			if (Pattern.matches("^(?:[*]*[a-zA-Z0-9.-]){4}[*a-zA-Z0-9.-]*$", blueprintName) == false)
				throw new IllegalArgumentException("Blueprint name "+blueprintName+" is not valid");
				
			String blueprintCode = FileUtils.readFileToString(blueprintFile, StandardCharsets.UTF_8);
			Blob blueprintBlob = new Blob("text/javascript", blueprintCode);
			
			Identity identity;
			try
			{
				identity = Serialization.getInstance().fromTypedString(deployOptionValues[deployOptionValues.length-1]);
			}
			catch (Exception ex)
			{
				if (wallet == null)
					printStream.println("ERROR: No identity supplied");
				
				identity = wallet.getIdentity();
			}

			Atom atom = this.atom;
			if (atom == null)
			{
				printStream.println("Deploying blueprint "+blueprintFile+" as "+blueprintName+" data checksum "+blueprintBlob.getChecksum()+" size "+blueprintBlob.size());
				atom = new Atom();
			}
			
			atom.push(blueprintBlob);
			atom.push("blueprint::deploy('"+blueprintName+"', '"+blueprintLanguage+"', hash('"+blueprintBlob.getHash()+"'), identity('"+identity+"'))");
			
			if (this.atom == null)
			{
				if (wallet == null)
					throw new IllegalStateException("No wallet is open");

				wallet.submit(atom);
			}
		}
		else if (commandLine.hasOption("instantiate"))
		{
			String[] instantiateOptionValues = commandLine.getOptionValues("instantiate");
			String blueprintContext = instantiateOptionValues[0].toLowerCase();
			if (Pattern.matches("^(?:[*]*[a-zA-Z0-9.-]){4}[*a-zA-Z0-9.-]*$", blueprintContext) == false)
				throw new IllegalArgumentException("Blueprint name "+blueprintContext+" is not valid");
				
			String componentContext = instantiateOptionValues[1].toLowerCase();
			if (Pattern.matches("^(?:[*]*[a-zA-Z0-9.-]){4}[*a-zA-Z0-9.-]*$", componentContext) == false)
				throw new IllegalArgumentException("Component context "+componentContext+" is not valid");
			
			Identity identity;
			try
			{
				identity = Serialization.getInstance().fromTypedString(instantiateOptionValues[2]);
			}
			catch (Exception ex)
			{
				if (wallet == null)
					printStream.println("ERROR: No identity supplied");
				
				identity = wallet.getIdentity();
			}

			String constructorArgs = "["+String.join(", ", commandLine.getArgList())+"]";

			Atom atom = this.atom;
			if (atom == null)
			{
				printStream.println("Instantiating blueprint "+blueprintContext+" as context "+componentContext);
				atom = new Atom();
			}
			
			atom.push("blueprint::instantiate('"+blueprintContext+"', '"+componentContext+"', identity('"+identity+"'), "+constructorArgs+")");

			if (this.atom == null)
			{
				if (wallet == null)
					throw new IllegalStateException("No wallet is open");

				wallet.submit(atom);
			}
		}
		else if (commandLine.hasOption("constructor"))
		{
			Atom atom = this.atom;
			if (atom == null)
				atom = new Atom();

			StringBuilder builder = new StringBuilder();
			builder.append("native::constructor(");
			builder.append(String.join(", ", commandLine.getArgList()));
			builder.append(")");
			atom.push(builder.toString());
			
			if (this.atom == null)
			{
				if (wallet == null)
					throw new IllegalStateException("No wallet is open");

				wallet.submit(atom);
			}
		}
		else if (commandLine.hasOption("method"))
		{
			Atom atom = this.atom;
			if (atom == null)
				atom = new Atom();
			
			StringBuilder builder = new StringBuilder();
			builder.append(commandLine.getOptionValues("method")[0]).append('.').append(commandLine.getOptionValues("method")[1]).append('(');
			builder.append(String.join(", ", commandLine.getArgList()));
			builder.append(")");
			atom.push(builder.toString());

			if (this.atom == null)
			{
				if (wallet == null)
					throw new IllegalStateException("No wallet is open");

				// Check manifest syntax
				ManifestParser.parse(atom.getManifest());

				wallet.submit(atom);
			}
		}
		else if (commandLine.hasOption("substate"))
		{
			String substateContext = commandLine.getOptionValues("substate")[0].toLowerCase();
			if (Pattern.matches("^(?:[*]*[a-zA-Z0-9.-]){4}[*a-zA-Z0-9.-]*$", substateContext) == false)
				throw new IllegalArgumentException("Substate context "+substateContext+" is not valid");
			
			String substateScope = commandLine.getOptionValues("substate")[1];
			Object decodedSubstateScope = Serialization.getInstance().fromTypedString(substateScope);
			if (decodedSubstateScope instanceof String)
				decodedSubstateScope = ((String) decodedSubstateScope).toLowerCase();
			
			StateAddress stateAddress = StateAddress.from(substateContext, Hash.valueOf(decodedSubstateScope));
			Future<SubstateSearchResponse> searchFuture = context.getLedger().get(new SubstateSearchQuery(stateAddress, Isolation.COMMITTED));
			SubstateSearchResponse searchResult = searchFuture.get(5, TimeUnit.SECONDS);
			if (searchResult.getResult() == null) 
			{
				printStream.println("Substate "+substateContext+":"+substateScope+" mapping to "+stateAddress+" not found");
				return;
			}

			JSONObject substateJSON = Serialization.getInstance().toJsonObject(searchResult.getResult(), Output.API);
			printStream.println(substateJSON.toString(4));
		}
	}
}