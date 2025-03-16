package org.radix.hyperscale.console;

import java.io.PrintStream;
import java.util.Map.Entry;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.radix.hyperscale.Context;
import org.radix.hyperscale.serialization.Serialization;

public class Serializer extends Function {
  private static final Options options =
      new Options().addOption("stats", false, "Outputs serializer statistics");

  public Serializer() {
    super("serializer", options);
  }

  @Override
  public void execute(Context context, String[] arguments, PrintStream printStream)
      throws Exception {
    CommandLine commandLine = Function.parser.parse(options, arguments);

    if (commandLine.hasOption("stats")) {
      for (Entry<String, Entry<Long, Long>> type : Serialization.getInstance().statistics())
        printStream.println(
            type.getKey()
                + ": "
                + type.getValue().getKey()
                + " -> "
                + type.getValue().getValue()
                + " / "
                + (type.getValue().getValue() / type.getValue().getKey()));
    }
  }
}
