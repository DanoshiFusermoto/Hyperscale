package org.radix.hyperscale.console;

import java.io.PrintStream;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.radix.hyperscale.Context;

public class Contexts extends Function {
  private static final Options options =
      new Options()
          .addOption("start", true, "Starts a new context <name>")
          .addOption("default", true, "Sets the default context to <name>")
          .addOption("end", true, "Ends a context <name>");

  public Contexts() {
    super("contexts", options);
  }

  @Override
  public void execute(Context context, String[] arguments, PrintStream printStream)
      throws Exception {
    CommandLine commandLine = Function.parser.parse(options, arguments);

    if (commandLine.hasOption("start")) {
      Context newContext = Context.get(commandLine.getOptionValue("start"));
      if (newContext != null)
        throw new IllegalStateException(
            "Context " + commandLine.getOptionValue("start").toLowerCase() + " already exists");

      Context.createAndStart(commandLine.getOptionValue("start"), context.getConfiguration());
    } else if (commandLine.hasOption("end")) {
      Context terminatingContext = Context.get(commandLine.getOptionValue("end"));
      if (terminatingContext == null)
        throw new IllegalStateException(
            "Context " + commandLine.getOptionValue("end").toLowerCase() + " does not exist");

      Context.stop(terminatingContext);
    } else if (commandLine.hasOption("default")) {
      Context defaultContext = Context.get(commandLine.getOptionValue("default"));
      if (defaultContext == null)
        throw new IllegalStateException(
            "Context " + commandLine.getOptionValue("default").toLowerCase() + " does not exist");

      Context.setDefault(defaultContext);
    }
  }
}
