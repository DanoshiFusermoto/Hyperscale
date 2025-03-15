package org.radix.hyperscale.console;

import java.io.PrintStream;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.radix.hyperscale.Constants;
import org.radix.hyperscale.Context;
import org.radix.hyperscale.crypto.Identity;
import org.radix.hyperscale.ledger.BlockHeader;
import org.radix.hyperscale.ledger.Epoch;
import org.radix.hyperscale.ledger.ShardGroupID;
import org.radix.hyperscale.ledger.ShardMapper;
import org.radix.hyperscale.ledger.VotePowers;

public class Validators extends Function {
  private static final Options options =
      new Options()
          .addOption("remote", false, "Return remote ledger information")
          .addOption("known", false, "All known validator identities")
          .addOption(
              Option.builder("powers")
                  .desc("Validators and vote power")
                  .optionalArg(true)
                  .numberOfArgs(1)
                  .build());

  public Validators() {
    super("validators", options);
  }

  @Override
  public void execute(Context context, String[] arguments, PrintStream printStream)
      throws Exception {
    CommandLine commandLine = Function.parser.parse(options, arguments);

    if (commandLine.hasOption("known")) {
      Collection<Identity> identities = context.getLedger().getValidatorHandler().getIdentities();
      identities.forEach(id -> printStream.println(id.getHash()));
      printStream.println(identities.size() + " validator identities");
    } else if (commandLine.hasOption("powers")) {
      final Epoch epoch;
      if (commandLine.getOptionValue("powers") == null) {
        BlockHeader header = context.getLedger().getHead();
        epoch = Epoch.from(header);
      } else epoch = Epoch.from(Integer.parseInt(commandLine.getOptionValue("powers")));

      final int numShardGroups = context.getLedger().numShardGroups(epoch);
      final ShardGroupID localShardGroupID =
          ShardMapper.toShardGroup(context.getNode().getIdentity(), numShardGroups);
      final Set<ShardGroupID> remoteShardGroupIDs = new HashSet<ShardGroupID>();
      for (int sg = 0; sg < numShardGroups; sg++) {
        if (sg == localShardGroupID.intValue()) continue;

        remoteShardGroupIDs.add(ShardGroupID.from(sg));
      }

      final VotePowers powers = context.getLedger().getValidatorHandler().getVotePower(epoch);
      powers
          .getAll()
          .forEach(
              (identity, power) ->
                  printStream.println(
                      identity.toString(Constants.TRUNCATED_IDENTITY_LENGTH) + " = " + power));
      printStream.println(powers.size() + " validators with power");

      if (remoteShardGroupIDs.isEmpty())
        printStream.println(
            "Total vote power: "
                + context
                    .getLedger()
                    .getValidatorHandler()
                    .getTotalVotePower(epoch, localShardGroupID));
      else
        printStream.println(
            "Total vote power: "
                + context
                    .getLedger()
                    .getValidatorHandler()
                    .getTotalVotePower(epoch, localShardGroupID)
                + " / "
                + context
                    .getLedger()
                    .getValidatorHandler()
                    .getTotalVotePower(epoch, remoteShardGroupIDs));
    }
  }
}
