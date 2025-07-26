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
import org.radix.hyperscale.Constants;
import org.radix.hyperscale.Context;
import org.radix.hyperscale.Universe;
import org.radix.hyperscale.apps.SimpleWallet;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.crypto.Identity;
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

	private Atom.Builder atomBuilder = null;
	
	public StateMachine()
	{
		super("sm", options);
	}

	@Override
	public void execute(Context context, String[] arguments, PrintStream printStream) throws Exception
	{
		final SimpleWallet wallet = Wallet.get(context);
		final CommandLine commandLine = Function.parser.parse(options, arguments);

		if (commandLine.hasOption("construct"))
		{
			if(this.atomBuilder != null)
			{
				printStream.println("ERROR: An atom is already under construction");
				return;
			}

			this.atomBuilder = new Atom.Builder();
		}
		else if (commandLine.hasOption("sign"))
		{
			if(this.atomBuilder == null)
			{
				printStream.println("ERROR: An atom is not under construction");
				return;
			}
			
			final EDKeyPair keyPair;
			if (commandLine.getArgs().length != 0)
			{
				String keyStr = commandLine.getArgs()[0];
				keyPair = new EDKeyPair(BaseEncoding.base16().decode(keyStr));
			}
			else
			{
				if (wallet == null)
					throw new IllegalStateException("No wallet is open to sign");
				keyPair = wallet.getKeyPair();
			}
			
			this.atomBuilder.signer(keyPair);
		}
		else if (commandLine.hasOption("submit"))
		{
			if(this.atomBuilder == null)
			{
				printStream.println("ERROR: An atom is not under construction");
				return;
			}
			
			try
			{
				// Check manifest syntax
				ManifestParser.parse(this.atomBuilder.getManifest());
				
				final Atom atom = this.atomBuilder.build(Universe.get().getPrimitivePOW());
				printStream.println("Submitting atom "+atom.getHash()+" -> "+Serialization.getInstance().toJsonObject(atom, Output.API).toString(4));

				context.getLedger().submit(atom);
			}
			finally
			{
				this.atomBuilder = null;
			}
		}
		else if (commandLine.hasOption("discard"))
		{
			if(this.atomBuilder == null)
			{
				printStream.println("ERROR: An atom is not under construction");
				return;
			}
			
			this.atomBuilder = null;
		}
		else if (commandLine.hasOption("view"))
		{
			if(this.atomBuilder == null)
			{
				printStream.println("ERROR: An atom is not under construction");
				return;
			}
			
			for (final String instruction : this.atomBuilder.getManifest())
				printStream.println(instruction);
		}

		else if (commandLine.hasOption("deploy"))
		{
			final String[] deployOptionValues = commandLine.getOptionValues("deploy");
			final File blueprintFile = new File(deployOptionValues[0]);
			if (blueprintFile.exists() == false)
				throw new FileNotFoundException(blueprintFile.toString());
			if (blueprintFile.isDirectory())
				throw new IllegalArgumentException(blueprintFile+" is a directory");

			final String blueprintLanguage = deployOptionValues[1].toLowerCase();

			final String blueprintName = deployOptionValues[2].toLowerCase();
			if (Pattern.matches("^(?:[*]*[a-zA-Z0-9.-]){4}[*a-zA-Z0-9.-]*$", blueprintName) == false)
				throw new IllegalArgumentException("Blueprint name "+blueprintName+" is not valid");
				
			final String blueprintCode = FileUtils.readFileToString(blueprintFile, StandardCharsets.UTF_8);
			final Blob blueprintBlob = new Blob("text/javascript", blueprintCode);
			
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

			Atom.Builder atomBuilder = this.atomBuilder;
			if (atomBuilder == null)
			{
				printStream.println("Deploying blueprint "+blueprintFile+" as "+blueprintName+" data checksum "+blueprintBlob.getChecksum()+" size "+blueprintBlob.size());
				atomBuilder = new Atom.Builder();
			}
			
			atomBuilder.push(blueprintBlob);
			atomBuilder.push("blueprint::deploy('"+blueprintName+"', '"+blueprintLanguage+"', hash('"+blueprintBlob.getHash()+"'), identity('"+identity+"'))");
			atomBuilder.signer(wallet.getKeyPair());
			
			if (this.atomBuilder == null)
			{
				ManifestParser.parse(atomBuilder.getManifest());
				wallet.submit(atomBuilder.build(Universe.get().getPrimitivePOW()));
			}
		}
		else if (commandLine.hasOption("instantiate"))
		{
			final String[] instantiateOptionValues = commandLine.getOptionValues("instantiate");
			final String blueprintContext = instantiateOptionValues[0].toLowerCase();
			if (Pattern.matches("^(?:[*]*[a-zA-Z0-9.-]){4}[*a-zA-Z0-9.-]*$", blueprintContext) == false)
				throw new IllegalArgumentException("Blueprint name "+blueprintContext+" is not valid");
				
			final String componentContext = instantiateOptionValues[1].toLowerCase();
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

			final String constructorArgs = "["+String.join(", ", commandLine.getArgList())+"]";

			Atom.Builder atomBuilder = this.atomBuilder;
			if (atomBuilder == null)
			{
				printStream.println("Instantiating blueprint "+blueprintContext+" as context "+componentContext);
				atomBuilder = new Atom.Builder();
			}
			
			atomBuilder.push("blueprint::instantiate('"+blueprintContext+"', '"+componentContext+"', identity('"+identity+"'), "+constructorArgs+")");
			atomBuilder.signer(wallet.getKeyPair());
			
			if (this.atomBuilder == null)
			{
				ManifestParser.parse(atomBuilder.getManifest());
				wallet.submit(atomBuilder.build(Universe.get().getPrimitivePOW()));
			}
		}
		else if (commandLine.hasOption("constructor"))
		{
			Atom.Builder atomBuilder = this.atomBuilder;
			if (atomBuilder == null)
				atomBuilder = new Atom.Builder();

			final StringBuilder builder = new StringBuilder();
			builder.append("native::constructor(");
			builder.append(String.join(", ", commandLine.getArgList()));
			builder.append(")");
			atomBuilder.push(builder.toString());
			atomBuilder.signer(wallet.getKeyPair());
			
			if (this.atomBuilder == null)
			{
				ManifestParser.parse(atomBuilder.getManifest());
				wallet.submit(atomBuilder.build(Universe.get().getPrimitivePOW()));
			}
		}
		else if (commandLine.hasOption("method"))
		{
			Atom.Builder atomBuilder = this.atomBuilder;
			if (atomBuilder == null)
				atomBuilder = new Atom.Builder();
			
			final StringBuilder builder = new StringBuilder();
			builder.append(commandLine.getOptionValues("method")[0]).append('.').append(commandLine.getOptionValues("method")[1]).append('(');
			builder.append(String.join(", ", commandLine.getArgList()));
			builder.append(")");
			atomBuilder.push(builder.toString());
			atomBuilder.signer(wallet.getKeyPair());
			
			if (this.atomBuilder == null)
			{
				ManifestParser.parse(atomBuilder.getManifest());
				wallet.submit(atomBuilder.build(Universe.get().getPrimitivePOW()));
			}
		}
		else if (commandLine.hasOption("substate"))
		{
			final String substateContext = commandLine.getOptionValues("substate")[0].toLowerCase();
			if (Pattern.matches("^(?:[*]*[a-zA-Z0-9.-]){4}[*a-zA-Z0-9.-]*$", substateContext) == false)
				throw new IllegalArgumentException("Substate context "+substateContext+" is not valid");
			
			final String substateScope = commandLine.getOptionValues("substate")[1];
			Object decodedSubstateScope = Serialization.getInstance().fromTypedString(substateScope);
			if (decodedSubstateScope instanceof String)
				decodedSubstateScope = ((String) decodedSubstateScope).toLowerCase();
			
			final StateAddress stateAddress = StateAddress.from(substateContext, Hash.valueOf(decodedSubstateScope));
			final SubstateSearchQuery substateSearchQuery = new SubstateSearchQuery(stateAddress, Isolation.COMMITTED);
			final Future<SubstateSearchResponse> searchSearchFuture = context.getLedger().get(substateSearchQuery, Constants.SEARCH_TIMEOUT_SECONDS, TimeUnit.SECONDS);
			final SubstateSearchResponse searchResult = searchSearchFuture.get(Constants.SEARCH_TIMEOUT_SECONDS, TimeUnit.SECONDS);
			if (searchResult.getResult() == null) 
			{
				printStream.println("Substate "+substateContext+":"+substateScope+" mapping to "+stateAddress+" not found");
				return;
			}

			final JSONObject substateJSON = Serialization.getInstance().toJsonObject(searchResult.getResult(), Output.API);
			printStream.println(substateJSON.toString(4));
		}
	}
}