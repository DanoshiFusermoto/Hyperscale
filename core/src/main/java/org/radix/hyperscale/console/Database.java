package org.radix.hyperscale.console;

import com.sleepycat.je.EnvironmentStats;
import java.io.PrintStream;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.radix.hyperscale.Context;

public class Database extends Function {
  private static final Options options =
      new Options().addOption("stats", false, "Outputs database statistics");

  public Database() {
    super("database", options);
  }

  @Override
  public void execute(Context context, String[] arguments, PrintStream printStream)
      throws Exception {
    CommandLine commandLine = Function.parser.parse(options, arguments);

    if (commandLine.hasOption("stats")) {
      EnvironmentStats stats = context.getDatabaseEnvironment().getEnvironment().getStats(null);
      printStream.println(stats.toString());
    }
  }
}
