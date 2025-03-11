package org.radix.hyperscale.console;

import java.io.PrintStream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.json.JSONObject;
import org.radix.hyperscale.Context;
import org.radix.hyperscale.apps.SimpleWallet;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.ledger.PendingAtom;
import org.radix.hyperscale.ledger.primitives.Atom;
import org.radix.hyperscale.serialization.Serialization;
import org.radix.hyperscale.serialization.DsonOutput.Output;

public class Atoms extends Function
{
	private static final Options options = new Options().addOption("submit", true, "Submit atom/atoms")
														.addOption("pending", false, "Returns info of all atoms pending")
														.addOption("pool", false, "Returns info of all atoms in the pool")
														.addOption("get", true, "Get an atom by hash");

	public Atoms()
	{
		super("atom", options);
	}

	@Override
	public void execute(Context context, String[] arguments, PrintStream printStream) throws Exception
	{
		CommandLine commandLine = Function.parser.parse(options, arguments);

		if (commandLine.hasOption("get"))
		{
			Atom atom = context.getLedger().get(Hash.from(commandLine.getOptionValue("get")), Atom.class);
			if (atom == null)
			{
				printStream.println("Atom "+commandLine.getOptionValue("get")+" not found");
				return;
			}
			
/*			AtomCertificate certificate = null;
			Future<SearchResult> certificateFuture = context.getLedger().get(new StateSearchQuery(new StateAddress(Atom.class, atom.getHash())));
			SearchResult searchResult = certificateFuture.get();
			if (searchResult != null) 
				certificate = searchResult.getPrimitive();
*/
			
			JSONObject atomJSONObject = Serialization.getInstance().toJsonObject(atom, Output.PERSIST);
//			if (certificate != null)
//				atomJSONObject.put("certificate", Serialization.getInstance().toJsonObject(certificate, Output.PERSIST));
				
			printStream.println(atomJSONObject.toString(4));
		}
		else if (commandLine.hasOption("pending"))
		{
			for (PendingAtom pendingAtom : context.getLedger().getAtomHandler().getAll())
 				printStream.println(pendingAtom.getHash()+" "+pendingAtom.getStatus());
		}
		else if (commandLine.hasOption("submit"))
		{
			String jsonString = commandLine.getOptionValue("submit");
			Atom atom = Serialization.getInstance().fromJson(jsonString, Atom.class);
			if (atom.isSealed() == false)
			{
				SimpleWallet wallet = Wallet.get(context);
				if (wallet == null)
					throw new IllegalStateException("No wallet is open");
				
				wallet.submit(atom);
			}
			else
				context.getLedger().submit(atom);
			
			printStream.println(Serialization.getInstance().toJsonObject(atom, Output.API).toString(4));
		}
	}
}