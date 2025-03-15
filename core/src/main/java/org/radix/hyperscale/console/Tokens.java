package org.radix.hyperscale.console;

import java.io.PrintStream;
import java.util.Collection;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.json.JSONObject;
import org.radix.hyperscale.Context;
import org.radix.hyperscale.apps.SimpleWallet;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.crypto.Identity;
import org.radix.hyperscale.ledger.StateAddress;
import org.radix.hyperscale.ledger.StateContext;
import org.radix.hyperscale.ledger.Substate;
import org.radix.hyperscale.ledger.SubstateSearchQuery;
import org.radix.hyperscale.ledger.SubstateSearchResponse;
import org.radix.hyperscale.ledger.primitives.Atom;
import org.radix.hyperscale.ledger.sme.components.TokenComponent;
import org.radix.hyperscale.serialization.DsonOutput.Output;
import org.radix.hyperscale.serialization.Serialization;
import org.radix.hyperscale.utils.UInt256;

public class Tokens extends Function {
  private static final Options options =
      new Options()
          .addOption(
              Option.builder("balance")
                  .desc("Returns balance for a token")
                  .optionalArg(true)
                  .numberOfArgs(1)
                  .build())
          .addOption(
              Option.builder("transfer")
                  .desc("Transfers a quantity of tokens to an address")
                  .numberOfArgs(1)
                  .build())
          .addOption(
              Option.builder("mint").desc("Mints a quantity of tokens").numberOfArgs(2).build())
          .addOption("owned", false, "List owned tokens")
          .addOption(
              Option.builder("create")
                  .desc("Creates a new token definition")
                  .numberOfArgs(1)
                  .build())
          .addOption(
              Option.builder("debits")
                  .desc("List debits for specified token")
                  .optionalArg(true)
                  .numberOfArgs(1)
                  .build())
          .addOption(
              Option.builder("credits")
                  .desc("List credits for specified token")
                  .optionalArg(true)
                  .numberOfArgs(1)
                  .build());

  public Tokens() {
    super("tokens", options);
  }

  @Override
  public void execute(Context context, String[] arguments, PrintStream printStream)
      throws Exception {
    final CommandLine commandLine = Function.parser.parse(options, arguments);

    final SimpleWallet wallet = Wallet.get(context);
    if (wallet == null) throw new IllegalStateException("No wallet is open");

    if (commandLine.hasOption("balance")) {
      final String ISO = commandLine.getOptionValue("balance", "CASSIE");

      final Future<SubstateSearchResponse> tokenSearchFuture =
          context
              .getLedger()
              .get(
                  new SubstateSearchQuery(
                      StateAddress.from(TokenComponent.class, Hash.valueOf(ISO.toLowerCase()))));
      final SubstateSearchResponse tokenSearchResult = tokenSearchFuture.get(10, TimeUnit.SECONDS);
      if (tokenSearchResult.getResult() == null) {
        printStream.println("Token " + ISO + " not found");
        return;
      }

      printStream.println(wallet.getBalance(ISO) + " " + ISO);
    } else if (commandLine.hasOption("transfer")) {
      final Object[] options =
          Stream.of(commandLine.getOptionValues("transfer"), commandLine.getArgs())
              .flatMap(Stream::of)
              .toArray();

      final Atom.Builder atomBuilder = new Atom.Builder();
      for (int o = 0; o < options.length; o += 3) {
        Identity receiver = Identity.from((String) options[o]);
        UInt256 amount = UInt256.from((String) options[o + 1]);
        String symbol = (String) options[o + 2];
        atomBuilder.push(wallet.spend(symbol, amount, receiver));
      }

      final Atom atom = atomBuilder.build();
      wallet.submit(atom);

      JSONObject atomJSON = Serialization.getInstance().toJsonObject(atom, Output.API);
      printStream.println(atomJSON.toString());
    } else if (commandLine.hasOption("create")) {
      JSONObject json;
      try {
        String s =
            Stream.of(commandLine.getOptionValues("create"), commandLine.getArgs())
                .flatMap(Stream::of)
                .collect(Collectors.joining(" "));
        json = new JSONObject(s);
      } catch (Exception ex) {
        json = new JSONObject();
        json.put("symbol", commandLine.getOptionValue("create"));
        json.put("description", commandLine.getArgList().stream().collect(Collectors.joining(" ")));
      }

      final Atom.Builder atomBuilder = new Atom.Builder();
      atomBuilder.push(
          TokenComponent.class.getAnnotation(StateContext.class).value()
              + "::create('"
              + json.getString("symbol")
              + "', '"
              + json.getString("description")
              + "', account('"
              + wallet.getIdentity()
              + "'))");

      final Atom atom = atomBuilder.build();
      wallet.submit(atom);

      printStream.println(Serialization.getInstance().toJson(atom, Output.API));
    } else if (commandLine.hasOption("mint")) {
      final String[] options = commandLine.getOptionValues("mint");
      final UInt256 amount = UInt256.from(options[0]);
      final String ISO = options[1];

      final Future<SubstateSearchResponse> tokenSearchFuture =
          context
              .getLedger()
              .get(
                  new SubstateSearchQuery(
                      StateAddress.from(TokenComponent.class, Hash.valueOf(ISO.toLowerCase()))));
      final SubstateSearchResponse tokenSearchResult = tokenSearchFuture.get(10, TimeUnit.SECONDS);
      if (tokenSearchResult.getResult() == null) {
        printStream.println("Token " + ISO + " not found");
        return;
      }

      if (tokenSearchResult.getResult().getSubstate().getAuthority().equals(wallet.getIdentity())
          == false) {
        printStream.println(
            "Can not mint token " + ISO + " as not owned by " + wallet.getIdentity());
        return;
      }

      final Atom.Builder atomBuilder = new Atom.Builder();
      atomBuilder.push(
          TokenComponent.class.getAnnotation(StateContext.class).value()
              + "::mint('"
              + ISO
              + "', "
              + amount
              + ", account('"
              + wallet.getIdentity()
              + "'))");

      final Atom atom = atomBuilder.build();
      wallet.submit(atom);

      printStream.println(Serialization.getInstance().toJson(atom, Output.API));
    } else if (commandLine.hasOption("debits")) {
      final String ISO = commandLine.getOptionValue("debits", "CASSIE");

      final Future<SubstateSearchResponse> tokenSearchFuture =
          context
              .getLedger()
              .get(
                  new SubstateSearchQuery(
                      StateAddress.from(TokenComponent.class, Hash.valueOf(ISO.toLowerCase()))));
      final SubstateSearchResponse tokenSearchResult = tokenSearchFuture.get(10, TimeUnit.SECONDS);
      if (tokenSearchResult.getResult() == null) {
        printStream.println("Token " + ISO + " not found");
        return;
      }

      final Collection<Substate> transfers =
          wallet.get(
              TokenComponent.class.getAnnotation(StateContext.class).value() + ".transfer",
              TokenComponent.class.getAnnotation(StateContext.class).value() + ".burn");
      for (final Substate transfer : transfers) {
        if (ISO.equalsIgnoreCase(transfer.get("symbol")) == false) continue;

        if (wallet.getIdentity().equals(transfer.get("sender")) == false) continue;

        printStream.println(
            transfer.getHash()
                + " "
                + transfer.get("quantity")
                + " "
                + ISO
                + " -> "
                + transfer.get("receiver"));
      }
    } else if (commandLine.hasOption("credits")) {
      final String ISO = commandLine.getOptionValue("credits", "CASSIE");

      final Future<SubstateSearchResponse> tokenSearchFuture =
          context
              .getLedger()
              .get(
                  new SubstateSearchQuery(
                      StateAddress.from(TokenComponent.class, Hash.valueOf(ISO.toLowerCase()))));
      final SubstateSearchResponse tokenSearchResult = tokenSearchFuture.get(10, TimeUnit.SECONDS);
      if (tokenSearchResult.getResult() == null) {
        printStream.println("Token " + ISO + " not found");
        return;
      }

      final Collection<Substate> transfers =
          wallet.get(
              TokenComponent.class.getAnnotation(StateContext.class).value() + ".transfer",
              TokenComponent.class.getAnnotation(StateContext.class).value() + ".mint");
      for (final Substate transfer : transfers) {
        if (ISO.equalsIgnoreCase(transfer.get("symbol")) == false) continue;

        if (wallet.getIdentity().equals(transfer.get("receiver")) == false) continue;

        printStream.println(
            transfer.getHash()
                + " "
                + transfer.get("quantity")
                + " "
                + ISO
                + " <- "
                + transfer.get("sender"));
      }
    }
  }
}
