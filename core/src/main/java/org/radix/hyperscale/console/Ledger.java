package org.radix.hyperscale.console;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.io.RandomAccessFile;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.radix.hyperscale.Constants;
import org.radix.hyperscale.Context;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.crypto.Hashable;
import org.radix.hyperscale.ledger.AtomStatus;
import org.radix.hyperscale.ledger.Block;
import org.radix.hyperscale.ledger.BlockHeader;
import org.radix.hyperscale.ledger.PendingAtom;
import org.radix.hyperscale.ledger.PendingState;
import org.radix.hyperscale.ledger.ShardMapper;
import org.radix.hyperscale.ledger.StateAddress;
import org.radix.hyperscale.ledger.StateLockMode;
import org.radix.hyperscale.ledger.BlockHeader.InventoryType;
import org.radix.hyperscale.ledger.primitives.Atom;
import org.radix.hyperscale.ledger.primitives.StateCertificate;
import org.radix.hyperscale.serialization.Serialization;
import org.radix.hyperscale.serialization.DsonOutput.Output;

public class Ledger extends Function
{
	private static final Options options = new Options().addOption("pending", false, "Return pending ledger information")
														.addOption("states", false, "Return hash list of all pending states")
														.addOption("stored", false, "List quantities of primitives stored")
														.addOption("export", false, "Exports ledger as migration data")
														.addOption("import", false, "Imports ledger from migration data")
														.addOption(Option.builder("snapshot").desc("Outputs current state info of ledger").optionalArg(true).numberOfArgs(1).build())
														.addOption("block", true, "Return block at specified height")
														.addOption(Option.builder("desync").desc("Forces a desync for a period of time").hasArg(true).numberOfArgs(1).build())
														.addOption("branches", false, "Return pending branches");

	public Ledger()
	{
		super("ledger", options);
	}

	@Override
	public void execute(Context context, String[] arguments, PrintStream printStream) throws Exception
	{
		CommandLine commandLine = Function.parser.parse(options, arguments);

		if (commandLine.hasOption("desync"))
		{
			context.getLedger().getSyncHandler().desyncFor(Long.parseLong(commandLine.getOptionValue("desync")), TimeUnit.SECONDS);
		}
		else if (commandLine.hasOption("pending"))
		{
			if (commandLine.hasOption("branches"))
			{
				context.getLedger().getBlockHandler().getPendingBranches().forEach(pb -> printStream.println(pb.toString()));
			}
			else if (commandLine.hasOption("states"))
			{
				Collection<PendingAtom> pendingAtoms = context.getLedger().getAtomHandler().getAll();
				long numPendingStates = pendingAtoms.stream().filter(pa -> pa.getStatus().after(AtomStatus.State.PREPARED) && pa.getStatus().before(AtomStatus.State.FINALIZED)).count();
				printStream.println(numPendingStates+" pending states");
			}
		}
		else if (commandLine.hasOption("block"))
		{
			Block block;
			
			try
			{
				block = context.getLedger().getBlock(Long.parseLong(commandLine.getOptionValue("block")));
			}
			catch (NumberFormatException nfex)
			{
				block = context.getLedger().get(Hash.from(commandLine.getOptionValue("block")), Block.class);
			}
			printStream.println("Block: "+block.getHeader().toString());
		}
		else if (commandLine.hasOption("snapshot"))
		{
			boolean verbose = false;
			String option = commandLine.getOptionValue("snapshot");
			if (option != null && option.compareToIgnoreCase("verbose") == 0)
				verbose = true;
			
			Collection<PendingAtom> atomHandlerPending = context.getLedger().getAtomHandler().getAll();
			if (verbose) atomHandlerPending.forEach(pa -> printStream.println(pa.getHash()));
			printStream.println(atomHandlerPending.size()+" pending in atom handler "+atomHandlerPending.stream().map(PendingAtom::getHash).reduce((a, b) -> Hash.hash(a, b)));

			Collection<PendingState> stateHandlerPending = context.getLedger().getAtomHandler().getAll().stream().filter(pa -> pa.getStatus().after(AtomStatus.State.PREPARED) && pa.getStatus().before(AtomStatus.State.FINALIZED)).flatMap(pa -> pa.getStates(StateLockMode.WRITE).stream()).collect(Collectors.toList());
			if (verbose) stateHandlerPending.forEach(ps -> printStream.println(ps.getHash()));
			printStream.println(stateHandlerPending.size()+" pending in state handler "+stateHandlerPending.stream().map(ps -> ps.getHash()).reduce((a, b) -> Hash.hash(a, b)));

			Collection<StateCertificate> stateHandlerCertificates = context.getLedger().getAtomHandler().getAll().stream().filter(pa -> pa.getStatus().after(AtomStatus.State.PREPARED) && pa.getStatus().before(AtomStatus.State.FINALIZED)).flatMap(pa -> pa.getStates(StateLockMode.WRITE).stream()).map(PendingState::getStateOutput).map(StateCertificate.class::cast).collect(Collectors.toList());
			if (verbose) stateHandlerCertificates.forEach(sc -> printStream.println(sc.getHash()));
			printStream.println(stateHandlerCertificates.size()+" certificates in state handler "+stateHandlerCertificates.stream().map(Hashable::getHash).reduce((a, b) -> Hash.hash(a, b)));
			
			Collection<StateAddress> stateAccumulatorLocked = context.getLedger().getStateAccumulator().locked();
			if (verbose) stateAccumulatorLocked.forEach(p -> printStream.println(p.toString()));
			printStream.println(stateAccumulatorLocked.size()+" exclusive locks in accumulator "+stateAccumulatorLocked.stream().map(StateAddress::getHash).reduce((a, b) -> Hash.hash(a, b)));

			printStream.println("Current head: "+context.getLedger().getHead());
		}
		else if (commandLine.hasOption("export"))
		{
			exportAtoms(context, printStream);
		}
		else if (commandLine.hasOption("import"))
		{
			importAtoms(context, printStream);
		}
		else
		{
			printStream.println();
			printStream.println("Synced: "+context.getNode().isSynced());
			printStream.println("Identity: S-"+(ShardMapper.toShardGroup(context.getNode().getIdentity(), context.getLedger().numShardGroups()))+" <- "+context.getNode().getIdentity().toString(Constants.TRUNCATED_IDENTITY_LENGTH));
			printStream.println("Epoch: "+context.getLedger().getEpoch().getClock()+" "+(context.getLedger().getHead().getHeight() % Constants.BLOCKS_PER_EPOCH)+"/"+Constants.BLOCKS_PER_EPOCH);
			printStream.println("Head: "+context.getLedger().getHead());
			printStream.println("Timestamp: "+TimeUnit.MILLISECONDS.toSeconds(context.getLedger().getTimestamp())+" / "+new Date(context.getLedger().getTimestamp()));
			printStream.println("Transactions (P/A/AT/EL/ET/CT/T): "+context.getLedger().getAtomHandler().size()+"/"+context.getMetaData().get("ledger.processed.atoms.local", 0l)+"/"+context.getMetaData().get("ledger.processed.atoms.timedout.accept", 0l)+"/"+context.getMetaData().get("ledger.processed.atoms.latent.execution", 0l)+"/"+context.getMetaData().get("ledger.processed.atoms.timedout.execution", 0l)+"/"+context.getMetaData().get("ledger.processed.atoms.timedout.commit", 0l)+"/"+context.getMetaData().get("ledger.processed.atoms.total", 0l));
			printStream.println("Transaction throughput: "+context.getMetaData().get("ledger.throughput.atoms.local", 0l)+"/"+context.getMetaData().get("ledger.throughput.atoms.total", 0l));
			printStream.println("Execution throughput: "+context.getMetaData().get("ledger.throughput.executions.local", 0l)+"/"+context.getMetaData().get("ledger.throughput.executions.total", 0l));
			printStream.println("Certificates (A/R/Q): "+context.getMetaData().get("ledger.commits.certificates.accept", 0l)+"/"+context.getMetaData().get("ledger.commits.certificates.reject", 0l)+"/"+context.getMetaData().get("ledger.commits.certificates", 0l));
			printStream.println("Packages: "+context.getMetaData().get("ledger.packages.blueprints", 0l)+"/"+context.getMetaData().get("ledger.packages.components", 0l));
			
			printStream.println("Proposal size avg: "+(context.getMetaData().get("ledger.blocks.bytes", 0l)/(context.getLedger().getHead().getHeight()+1)));
			printStream.println("Proposal throughput: "+context.getMetaData().get("ledger.throughput.blocks", 0l)+"/"+context.getMetaData().get("ledger.interval.block", 0l));
			printStream.println("Proposal supers: "+context.getMetaData().get("ledger.session.blocks", 0l)+"/"+context.getMetaData().get("ledger.session.blocks.supers", 0l)+" "+(((double)context.getMetaData().get("ledger.session.blocks", 0l))/context.getMetaData().get("ledger.session.blocks.supers", 0l)));
			printStream.println("Proposal interval: "+(context.getMetaData().get("ledger.interval.progress", 0l) / Math.max(1, context.getLedger().getHead().getHeight()))+" / "+context.getMetaData().get("ledger.interval.round", 0l));
			printStream.println("Finality latency: "+context.getMetaData().get("ledger.throughput.latency.accepted", 0l)+"/"+context.getMetaData().get("ledger.throughput.latency.witnessed", 0l));
			if (context.getConfiguration().get("ledger.atompool", Boolean.TRUE) == Boolean.TRUE)
				printStream.println("Transaction pool (S/A/R/C/Q): "+context.getMetaData().get("ledger.pool.atom.size", 0l)+" / "+context.getMetaData().get("ledger.pool.atoms.added", 0l)+" / "+context.getMetaData().get("ledger.pool.atoms.removed", 0l)+" / "+context.getMetaData().get("ledger.pool.atoms.agreed", 0l)+" / "+context.getMetaData().get("ledger.pool.atom.certificates", 0l));
			printStream.println("Transaction vote collectors (T/E): "+context.getMetaData().get("ledger.pool.atom.vote.collectors.total", 0l)+" / "+(context.getMetaData().get("ledger.pool.atom.votes", 0l)/context.getMetaData().get("ledger.pool.atom.vote.collectors.total", 1l)));
			printStream.println("Proposal votes (PVR/PVP/V): "+context.getMetaData().get("ledger.pool.block.votes.received", 0l)+" / "+context.getMetaData().get("ledger.blockvote.processed", 0l)+" / "+context.getMetaData().get("ledger.pool.block.vote.verifications", 0l));
			printStream.println("State pool (S/A/R/V/C): "+context.getLedger().getAtomHandler().getAll().stream().filter(pa -> pa.getStatus().after(AtomStatus.State.PREPARED) && pa.getStatus().before(AtomStatus.State.FINALIZED)).flatMap(pa -> pa.getStateAddresses(StateLockMode.WRITE).stream()).count()+" / "+context.getMetaData().get("ledger.pool.state.added", 0l)+" / "+context.getMetaData().get("ledger.pool.state.removed", 0l)+" / "+context.getMetaData().get("ledger.pool.state.votes", 0l)+" / "+context.getMetaData().get("ledger.pool.state.certificates", 0l));
			printStream.println("State vote collectors (S/T/E/L/X): "+context.getMetaData().get("ledger.pool.state.vote.collectors", 0l)+" / "+context.getMetaData().get("ledger.pool.state.vote.collectors.total", 0l)+" / "+(context.getMetaData().get("ledger.pool.state.votes", 0l)/context.getMetaData().get("ledger.pool.state.vote.collectors.total", 1l))+" / "+(context.getMetaData().get("ledger.pool.state.vote.collectors.latency", 0l)/context.getMetaData().get("ledger.pool.state.vote.collectors.total", 1l)+" / "+context.getMetaData().get("ledger.pool.state.vote.collectors.latent", 0l)));
			printStream.println("State verifications (SVB/SCV/SCC): "+context.getMetaData().get("ledger.pool.state.vote.block.verifications", 0l)+" / "+context.getMetaData().get("ledger.pool.state.certificate.verifications", 0l)+" / "+context.getMetaData().get("ledger.pool.state.certificate.verifications.cached", 0l));
			printStream.println("Proposal pool (P/V/B): "+context.getLedger().getBlockHandler().getPendingHeaders().size()+" / "+context.getLedger().getBlockHandler().getPendingBlocks().size()+" / "+context.getLedger().getBlockHandler().getPendingBranches().size());
			printStream.println("Shard (G/A): "+context.getLedger().numShardGroups()+"/"+context.getMetaData().get("ledger.throughput.shards.touched", 1.0));
		}
	}
	
	private void exportAtoms(Context context, PrintStream printStream) throws Exception
	{
		File exportFile = new File("export.dat");
		if (exportFile.exists())
			throw new IOException("Export file "+exportFile+" exists");
		
		RandomAccessFile exportRAF = new RandomAccessFile(exportFile, "rw");
		try
		{
			long height = 1;
			long analysedAtoms = 0;
			long exportedAtoms = 0;
			BlockHeader blockHeader = context.getLedger().getBlockHeader(height);
			while(blockHeader != null)
			{
				if (height % 100 == 0)
					printStream.println("Analysed "+analysedAtoms+" Exported "+exportedAtoms+" atoms ... Currently exporting block "+height);
				
				for (Hash hash : blockHeader.getInventory(InventoryType.ACCEPTED))
				{
					Calendar exportTimeBound = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
					exportTimeBound.set(2022, 0, 1);

					Atom atom = context.getLedger().get(hash, Atom.class);
					byte[] atomBytes = Serialization.getInstance().toDson(atom, Output.WIRE);
					exportRAF.writeInt(atomBytes.length);
					exportRAF.write(atomBytes);
					exportedAtoms++;
				}
				
				height++;
				blockHeader = context.getLedger().getBlockHeader(height);
			}
		}
		finally
		{
			exportRAF.close();
		}
	}
	
	private void importAtoms(Context context, PrintStream printStream) throws Exception
	{
		File importFile = new File("export.dat");
		if (importFile.exists() == false)
			throw new FileNotFoundException("Import file "+importFile+" not found");
		
		RandomAccessFile importRAF = new RandomAccessFile(importFile, "rw");
		try
		{
			long position = 0;
			long importRAFLength = importRAF.length();
			long importedAtoms = 0;
			while(position < importRAFLength)
			{
				byte[] atomBytes = new byte[importRAF.readInt()];
				importRAF.readFully(atomBytes);
				position += Integer.BYTES + atomBytes.length;
				
				Atom importAtom = Serialization.getInstance().fromDson(atomBytes, Atom.class);
				if (context.getLedger().submit(importAtom) == true)
					importedAtoms++;
				
				if (importedAtoms % 1000 == 0)
					printStream.println("Imported "+importedAtoms+" atoms ... ");
				
				Thread.sleep(1000);
			}
		}
		finally
		{
			importRAF.close();
		}
	}

}