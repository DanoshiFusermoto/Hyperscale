package org.radix.hyperscale;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.TimeUnit;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.crypto.Identity;
import org.radix.hyperscale.crypto.KeyPair;
import org.radix.hyperscale.crypto.bls12381.BLSKeyPair;
import org.radix.hyperscale.crypto.bls12381.BLSPublicKey;
import org.radix.hyperscale.crypto.ed25519.EDKeyPair;
import org.radix.hyperscale.exceptions.ValidationException;
import org.radix.hyperscale.ledger.Block;
import org.radix.hyperscale.ledger.ShardGroupID;
import org.radix.hyperscale.ledger.ShardMapper;
import org.radix.hyperscale.ledger.VotePowers;
import org.radix.hyperscale.ledger.primitives.Atom;
import org.radix.hyperscale.ledger.primitives.Blob;
import org.radix.hyperscale.ledger.sme.ManifestParser;
import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;
import org.radix.hyperscale.serialization.DsonOutput.Output;
import org.radix.hyperscale.serialization.Serialization;
import org.radix.hyperscale.utils.Bytes;
import org.radix.hyperscale.utils.UInt128;
import org.radix.hyperscale.utils.UInt256;

public final class GenerateUniverses {
  private static final Logger LOGGER = Logging.getLogger("generate_universes");

  public static final String RADIX_ICON_URL =
      "https://assets.radixdlt.com/icons/icon-xrd-32x32.png";

  private static final CommandLineParser parser = new DefaultParser();
  private static final Options options =
      new Options()
          .addOption("shardgroups", true, "Number of initial shard groups of the universe")
          .addOption("nodes", true, "Number of genesis nodes of the universe")
          .addOption("keys", true, "The keys for the genesis nodes of the universe")
          .addOption("offset", true, "The offset for generating validator ID")
          .addOption("unikey", true, "The path to the universe key to use");

  private final EDKeyPair universeKey;
  private final int numValidators;
  private final int numShardGroups;
  private final CommandLine commandLine;
  private final Set<Identity> validators;

  public GenerateUniverses(String[] arguments) throws Exception {
    this.commandLine = GenerateUniverses.parser.parse(options, arguments);

    if (this.commandLine.hasOption("unikey"))
      this.universeKey =
          EDKeyPair.fromFile(new File(this.commandLine.getOptionValue("unikey")), false);
    else {
      this.universeKey = new EDKeyPair();
      KeyPair.toFile(new File("universe.key"), this.universeKey);
      System.out.println(
          "Universe identity "
              + this.universeKey.getIdentity().getHash()
              + " / "
              + this.universeKey.getIdentity().toString());
    }

    this.validators = new LinkedHashSet<Identity>();
    if (this.commandLine.hasOption("nodes") && this.commandLine.hasOption("shardgroups")) {
      this.numValidators = Integer.parseInt(this.commandLine.getOptionValue("nodes"));
      this.numShardGroups = Integer.parseInt(this.commandLine.getOptionValue("shardgroups"));
      int validatorsPerShardGroup = this.numValidators / this.numShardGroups;
      int validatorID = Integer.parseInt(this.commandLine.getOptionValue("offset", "0"));

      for (int sg = 0; sg < this.numShardGroups; sg++) {
        Set<BLSKeyPair> shardValidatorKeys = new HashSet<BLSKeyPair>();
        while (shardValidatorKeys.size() < validatorsPerShardGroup) {
          BLSKeyPair validatorKey = new BLSKeyPair();
          ShardGroupID shardGroupID =
              ShardMapper.toShardGroup(validatorKey.getIdentity(), this.numShardGroups);
          if (shardGroupID.intValue() == sg) shardValidatorKeys.add(validatorKey);
        }

        for (BLSKeyPair shardValidatorKey : shardValidatorKeys) {
          KeyPair.toFile(new File("node-" + validatorID + ".key"), shardValidatorKey);
          validatorID++;
          this.validators.add(shardValidatorKey.getIdentity());
        }
      }
    } else {
      // TODO want to be able to specify multiple nodes to get the genesis mass as bootstrapping
      String validatorKeys =
          this.commandLine.getOptionValue(
              "node.keys", "A4CN2+9CuPoCLxf8Hahacl4vWof8eePjZiAKrZgTiRHw");
      StringTokenizer valdidatorKeysTokenizer = new StringTokenizer(validatorKeys, ",");
      while (valdidatorKeysTokenizer.hasMoreTokens()) {
        String validatorKeyToken = valdidatorKeysTokenizer.nextToken();
        this.validators.add(BLSPublicKey.from(validatorKeyToken).getIdentity());
      }

      this.numShardGroups = 1;
      this.numValidators = this.validators.size();
    }
  }

  public GenerateUniverses() throws Exception {
    this(new String[] {"universe.key"});
  }

  public List<Universe> generateDeployments() throws Exception {
    LOGGER.info(
        "UNIVERSE KEY PRIVATE:  "
            + Bytes.toHexString(this.universeKey.getPrivateKey().toByteArray()));
    LOGGER.info(
        "UNIVERSE KEY PUBLIC:   "
            + Bytes.toHexString(this.universeKey.getPublicKey().toByteArray()));

    List<Universe> universes = new ArrayList<>();

    long universeTimestampSeconds =
        Long.parseLong(this.commandLine.getOptionValue("timestamp", "1136073600"));
    long universeTimestampMillis = TimeUnit.SECONDS.toMillis(universeTimestampSeconds);

    universes.add(
        buildUniverse(
            10000,
            "Mainnet",
            "The public universe",
            Universe.Type.PRODUCTION,
            universeTimestampMillis,
            this.numShardGroups,
            (int) TimeUnit.DAYS.toSeconds(1)));
    universes.add(
        buildUniverse(
            20000,
            "Testnet",
            "The test universe",
            Universe.Type.TEST,
            universeTimestampMillis,
            this.numShardGroups,
            (int) TimeUnit.HOURS.toSeconds(1)));
    universes.add(
        buildUniverse(
            30000,
            "Devnet",
            "The development universe",
            Universe.Type.DEVELOPMENT,
            universeTimestampMillis,
            this.numShardGroups,
            (int) TimeUnit.MINUTES.toSeconds(10)));

    return universes;
  }

  private Universe buildUniverse(
      int port,
      String name,
      String description,
      Universe.Type type,
      long timestamp,
      int shardGroups,
      int epoch)
      throws Exception {
    int universeMagic =
        Universe.computeMagic(
            this.universeKey.getIdentity(), timestamp, shardGroups, epoch, port, type);
    Block universeBlock = createGenesisBlock(universeMagic, timestamp);

    Universe universe =
        Universe.newBuilder()
            .port(port)
            .name(name)
            .description(description)
            .type(type)
            .timestamp(timestamp)
            .epoch(epoch)
            .shardGroups(shardGroups)
            .creator(this.universeKey.getIdentity())
            .setGenesis(universeBlock)
            .setValidators(this.validators)
            .build();
    universe.sign(this.universeKey);

    if (universe.verify(this.universeKey.getPublicKey()) == false)
      throw new ValidationException("Signature failed for " + name + " deployment");

    System.out.println(Serialization.getInstance().toJsonObject(universe, Output.WIRE).toString(4));
    byte[] deploymentBytes = Serialization.getInstance().toDson(universe, Output.WIRE);
    System.out.println("UNIVERSE - " + type + ": " + Bytes.toBase64String(deploymentBytes));

    return universe;
  }

  private Block createGenesisBlock(int magic, long timestamp) throws Exception {
    final BLSKeyPair ephemeralValidator = new BLSKeyPair();
    final EDKeyPair ephemeralIdentity = new EDKeyPair();

    final Atom.Builder genesisAtomBuilder = new Atom.Builder();

    final Multimap<ShardGroupID, Identity> validatorsToShardGroups = HashMultimap.create();
    for (Identity validator : this.validators) {
      ShardGroupID shardGroupID = ShardMapper.toShardGroup(validator, this.numShardGroups);
      validatorsToShardGroups.put(shardGroupID, validator);
    }

    for (ShardGroupID shardGroupID : validatorsToShardGroups.keySet()) {
      final Map<Identity, Long> genesisVotePowers = new HashMap<>();
      for (Identity validator : validatorsToShardGroups.get(shardGroupID))
        genesisVotePowers.put(validator, Constants.VOTE_POWER_BOOTSTRAP);

      final Blob votePowersBlob =
          new Blob(
              "application/json",
              Serialization.getInstance()
                  .toJson(new VotePowers(0, genesisVotePowers), Output.WIRE));
      genesisAtomBuilder.push(votePowersBlob.asDataURL());
      genesisAtomBuilder.push(
          "ledger::epoch("
              + 0
              + ", "
              + shardGroupID
              + ", hash('"
              + votePowersBlob.getHash()
              + "'))");
    }

    genesisAtomBuilder.push("account::create(account('" + this.universeKey.getIdentity() + "'))");
    genesisAtomBuilder.push(
        "token::create(token('XRD', WRITE), 'Radix token', account('"
            + this.universeKey.getIdentity()
            + "'))");
    genesisAtomBuilder.push(
        "token::mint(token('XRD', WRITE), "
            + UInt256.from(UInt128.HIGH_BIT).toString()
            + ", vault('"
            + this.universeKey.getIdentity()
            + "'), identity('"
            + this.universeKey.getIdentity()
            + "'))");

    genesisAtomBuilder.push(
        "token::create(token('CASSIE', WRITE), 'Cassie token', account('"
            + this.universeKey.getIdentity()
            + "'))");
    genesisAtomBuilder.push(
        "token::mint(token('CASSIE', WRITE), "
            + UInt256.from(UInt128.HIGH_BIT).toString()
            + ", vault('"
            + this.universeKey.getIdentity()
            + "'), identity('"
            + this.universeKey.getIdentity()
            + "'))");

    genesisAtomBuilder.push(
        "token::create(token('HUG', WRITE), 'Hug token', account('"
            + this.universeKey.getIdentity()
            + "'))");
    genesisAtomBuilder.push(
        "token::mint(token('HUG', WRITE), "
            + UInt256.from(UInt128.HIGH_BIT).toString()
            + ", vault('"
            + this.universeKey.getIdentity()
            + "'), identity('"
            + this.universeKey.getIdentity()
            + "'))");

    genesisAtomBuilder.push(
        "token::create(token('DFP2', WRITE), 'DefiPlaza token', account('"
            + this.universeKey.getIdentity()
            + "'))");
    genesisAtomBuilder.push(
        "token::mint(token('DFP2', WRITE), "
            + UInt256.from(UInt128.HIGH_BIT).toString()
            + ", vault('"
            + this.universeKey.getIdentity()
            + "'), identity('"
            + this.universeKey.getIdentity()
            + "'))");

    genesisAtomBuilder.push(
        "token::create(token('OCI', WRITE), 'Ociswap token', account('"
            + this.universeKey.getIdentity()
            + "'))");
    genesisAtomBuilder.push(
        "token::mint(token('OCI', WRITE), "
            + UInt256.from(UInt128.HIGH_BIT).toString()
            + ", vault('"
            + this.universeKey.getIdentity()
            + "'), identity('"
            + this.universeKey.getIdentity()
            + "'))");

    genesisAtomBuilder.push(
        "token::create(token('EARLY', WRITE), 'Early token', account('"
            + this.universeKey.getIdentity()
            + "'))");
    genesisAtomBuilder.push(
        "token::mint(token('EARLY', WRITE), "
            + UInt256.from(UInt128.HIGH_BIT).toString()
            + ", vault('"
            + this.universeKey.getIdentity()
            + "'), identity('"
            + this.universeKey.getIdentity()
            + "'))");

    genesisAtomBuilder.push(
        "token::create(token('WOWO', WRITE), 'WonderWoman token', account('"
            + this.universeKey.getIdentity()
            + "'))");
    genesisAtomBuilder.push(
        "token::mint(token('WOWO', WRITE), "
            + UInt256.from(UInt128.HIGH_BIT).toString()
            + ", vault('"
            + this.universeKey.getIdentity()
            + "'), identity('"
            + this.universeKey.getIdentity()
            + "'))");

    genesisAtomBuilder.push(
        "token::create(token('IDA', WRITE), 'XIDAR token', account('"
            + this.universeKey.getIdentity()
            + "'))");
    genesisAtomBuilder.push(
        "token::mint(token('IDA', WRITE), "
            + UInt256.from(UInt128.HIGH_BIT).toString()
            + ", vault('"
            + this.universeKey.getIdentity()
            + "'), identity('"
            + this.universeKey.getIdentity()
            + "'))");

    genesisAtomBuilder.push(
        "token::create(token('FOTON', WRITE), 'Foton token', account('"
            + this.universeKey.getIdentity()
            + "'))");
    genesisAtomBuilder.push(
        "token::mint(token('FOTON', WRITE), "
            + UInt256.from(UInt128.HIGH_BIT).toString()
            + ", vault('"
            + this.universeKey.getIdentity()
            + "'), identity('"
            + this.universeKey.getIdentity()
            + "'))");

    genesisAtomBuilder.push(
        "token::create(token('CAVIAR', WRITE), 'Caviar9 token', account('"
            + this.universeKey.getIdentity()
            + "'))");
    genesisAtomBuilder.push(
        "token::mint(token('CAVIAR', WRITE), "
            + UInt256.from(UInt128.HIGH_BIT).toString()
            + ", vault('"
            + this.universeKey.getIdentity()
            + "'), identity('"
            + this.universeKey.getIdentity()
            + "'))");

    genesisAtomBuilder.push(
        "token::create(token('DAN', WRITE), 'Dan token', account('"
            + this.universeKey.getIdentity()
            + "'))");
    genesisAtomBuilder.push(
        "token::mint(token('DAN', WRITE), "
            + UInt256.from(UInt128.HIGH_BIT).toString()
            + ", vault('"
            + this.universeKey.getIdentity()
            + "'), identity('"
            + this.universeKey.getIdentity()
            + "'))");

    genesisAtomBuilder.push(
        "token::create(token('PHNX', WRITE), 'Phoenix token', account('"
            + this.universeKey.getIdentity()
            + "'))");
    genesisAtomBuilder.push(
        "token::mint(token('PHNX', WRITE), "
            + UInt256.from(UInt128.HIGH_BIT).toString()
            + ", vault('"
            + this.universeKey.getIdentity()
            + "'), identity('"
            + this.universeKey.getIdentity()
            + "'))");

    genesisAtomBuilder.push(
        "token::create(token('DPH', WRITE), 'Delphibets token', account('"
            + this.universeKey.getIdentity()
            + "'))");
    genesisAtomBuilder.push(
        "token::mint(token('DPH', WRITE), "
            + UInt256.from(UInt128.HIGH_BIT).toString()
            + ", vault('"
            + this.universeKey.getIdentity()
            + "'), identity('"
            + this.universeKey.getIdentity()
            + "'))");

    genesisAtomBuilder.push(
        "token::create(token('ASTRL', WRITE), 'Astrolescent token', account('"
            + this.universeKey.getIdentity()
            + "'))");
    genesisAtomBuilder.push(
        "token::mint(token('ASTRL', WRITE), "
            + UInt256.from(UInt128.HIGH_BIT).toString()
            + ", vault('"
            + this.universeKey.getIdentity()
            + "'), identity('"
            + this.universeKey.getIdentity()
            + "'))");

    genesisAtomBuilder.push(
        "token::create(token('EDGE', WRITE), 'Edge token', account('"
            + this.universeKey.getIdentity()
            + "'))");
    genesisAtomBuilder.push(
        "token::mint(token('EDGE', WRITE), "
            + UInt256.from(UInt128.HIGH_BIT).toString()
            + ", vault('"
            + this.universeKey.getIdentity()
            + "'), identity('"
            + this.universeKey.getIdentity()
            + "'))");

    genesisAtomBuilder.push(
        "token::create(token('GAB', WRITE), 'Gable token', account('"
            + this.universeKey.getIdentity()
            + "'))");
    genesisAtomBuilder.push(
        "token::mint(token('GAB', WRITE), "
            + UInt256.from(UInt128.HIGH_BIT).toString()
            + ", vault('"
            + this.universeKey.getIdentity()
            + "'), identity('"
            + this.universeKey.getIdentity()
            + "'))");

    genesisAtomBuilder.push(
        "token::create(token('WEFT', WRITE), 'Weft token', account('"
            + this.universeKey.getIdentity()
            + "'))");
    genesisAtomBuilder.push(
        "token::mint(token('WEFT', WRITE), "
            + UInt256.from(UInt128.HIGH_BIT).toString()
            + ", vault('"
            + this.universeKey.getIdentity()
            + "'), identity('"
            + this.universeKey.getIdentity()
            + "'))");

    genesisAtomBuilder.push(
        "token::create(token('USD', WRITE), 'USD token', account('"
            + this.universeKey.getIdentity()
            + "'))");
    genesisAtomBuilder.push(
        "token::mint(token('USD', WRITE), "
            + UInt256.from(UInt128.HIGH_BIT).toString()
            + ", vault('"
            + this.universeKey.getIdentity()
            + "'), identity('"
            + this.universeKey.getIdentity()
            + "'))");

    // Can build with zero difficulty as signed by universe key
    final Atom genesisAtom = genesisAtomBuilder.build(0);

    // Test the manifest integrity and parsing
    ManifestParser.parse(genesisAtom.getManifest());

    genesisAtom.sign(this.universeKey);

    Block genesisBlock =
        new Block(
            0l,
            Hash.ZERO,
            0l,
            UInt256.ZERO,
            0,
            0,
            timestamp,
            ephemeralValidator.getPublicKey().getIdentity(),
            Collections.singletonList(genesisAtom),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList());
    genesisBlock.getHeader().sign(ephemeralValidator);
    return genesisBlock;
  }

  public static void main(String[] arguments) throws Exception {
    System.setProperty("universe.generation", "true");
    GenerateUniverses generateDeployments = new GenerateUniverses(arguments);
    generateDeployments.generateDeployments();
  }
}
