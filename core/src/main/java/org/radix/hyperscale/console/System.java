package org.radix.hyperscale.console;

import java.io.PrintStream;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.radix.hyperscale.Context;
import org.radix.hyperscale.concurrency.MonitoredLock;
import org.radix.hyperscale.crypto.bls12381.BLSKeyPair;
import org.radix.hyperscale.crypto.ed25519.EDKeyPair;
import org.radix.hyperscale.database.vamos.LockManager;

public final class System extends Function {
  private static final Options options =
      new Options()
          .addOption("exit", false, "Terminates the Cassandra process")
          .addOption("locks", false, "Outputs monitored lock information")
          .addOption("crypto", false, "Metrics on cryptography operations");

  public System() {
    super("system", options);
  }

  @Override
  public void execute(Context context, String[] arguments, PrintStream printStream)
      throws Exception {
    final CommandLine commandLine = Function.parser.parse(options, arguments);

    if (commandLine.hasOption("exit")) {
      // TODO better force shutdown procedure
      java.lang.System.exit(999);
    } else if (commandLine.hasOption("locks")) {
      final LockManager vamosLockManager = context.getVamosEnvironment().getLockManager();
      printStream.println(
          context.getName()
              + ": Vamos Key Locks "
              + vamosLockManager.getKeyLockCount()
              + " / "
              + vamosLockManager.getAverageKeyLockWaitTime()
              + "ms | "
              + vamosLockManager.getTotalKeyLockWaitTime()
              + "ms");
      printStream.println(
          context.getName()
              + ": Vamos Node Locks "
              + vamosLockManager.getNodeLockCount()
              + " / "
              + vamosLockManager.getAverageNodeLockWaitTime()
              + "ms | "
              + vamosLockManager.getTotalNodeLockWaitTime()
              + "ms");
      printStream.println();
      for (final MonitoredLock lock : MonitoredLock.getLocks())
        printStream.println(lock.toString());
    } else if (commandLine.hasOption("crypto")) {
      printStream.println(context.getName() + ": BLS signings " + BLSKeyPair.signCount());
      printStream.println(context.getName() + ": BLS verifications " + BLSKeyPair.verifyCount());
      printStream.println(context.getName() + ": ECC signings " + EDKeyPair.signCount());
      printStream.println(context.getName() + ": ECC verifications " + EDKeyPair.verifyCount());
    } else {
      printStream.println(
          "Up time: " + DurationFormatUtils.formatDurationWords(context.getUptime(), true, false));
      printStream.println("Identity: " + context.getNode().getIdentity());
      printStream.println("CPU: " + Runtime.getRuntime().availableProcessors());
      printStream.println(
          "Memory: "
              + Runtime.getRuntime().freeMemory()
              + "/"
              + Runtime.getRuntime().maxMemory()
              + "/"
              + Runtime.getRuntime().totalMemory());
    }
  }
}
