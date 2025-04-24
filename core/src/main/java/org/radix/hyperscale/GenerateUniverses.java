package org.radix.hyperscale;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.radix.hyperscale.Universe.Type;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.crypto.Identity;
import org.radix.hyperscale.crypto.KeyPair;
import org.radix.hyperscale.crypto.Signature;
import org.radix.hyperscale.crypto.Signature.VerificationResult;
import org.radix.hyperscale.crypto.bls12381.BLSKeyPair;
import org.radix.hyperscale.crypto.bls12381.BLSPublicKey;
import org.radix.hyperscale.crypto.ed25519.EDKeyPair;
import org.radix.hyperscale.exceptions.ValidationException;
import org.radix.hyperscale.ledger.Block;
import org.radix.hyperscale.ledger.QuorumCertificate;
import org.radix.hyperscale.ledger.ShardGroupID;
import org.radix.hyperscale.ledger.ShardMapper;
import org.radix.hyperscale.ledger.VotePowers;
import org.radix.hyperscale.ledger.primitives.Atom;
import org.radix.hyperscale.ledger.primitives.Blob;
import org.radix.hyperscale.ledger.sme.ManifestParser;
import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;
import org.radix.hyperscale.serialization.Serialization;
import org.radix.hyperscale.serialization.DsonOutput.Output;
import org.radix.hyperscale.utils.Bytes;
import org.radix.hyperscale.utils.UInt128;
import org.radix.hyperscale.utils.UInt256;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.TimeUnit;

public final class GenerateUniverses
{
	private static final Logger LOGGER = Logging.getLogger("generate_universes");

	public static final String RADIX_ICON_URL = "https://assets.radixdlt.com/icons/icon-xrd-32x32.png";
	
	// Universe defaults if not specified
	private final static int DEFAULT_PRIMITIVE_POW = 12;
	private final static int DEFAULT_ROUND_INTERVAL = 500;
	private final static long DEFAULT_EPOCH_DURATION = TimeUnit.DAYS.toSeconds(1);
	
	private static final CommandLineParser parser = new DefaultParser(); 
	private static final Options options = new Options().addOption("shardgroups", true, "Number of initial shard groups of the universe")
														.addOption("nodes", true, "Number of genesis nodes of the universe")
														.addOption("name", true, "The name of the universe")
														.addOption("description", true, "The universe description")
														.addOption("epoch", true, "Seconds duration of an epoch")
														.addOption("interval", true, "Millisecond target interval for consensus rounds")
														.addOption("ppow", true, "Number of leading bits for primitive POW")
														.addOption("type", true, "The universe type (PRODUCTION, TEST, DEVELOPMENT)")
														.addOption("port", true, "Primary port for connections within the universe")
			   											.addOption("validators", true, "Keys for the genesis nodes of the universe")
			   											.addOption("offset", true, "Offset for generating validator ID")
			   											.addOption("creator", true, "Path to the universe key to use");

	private final EDKeyPair creatorKey;
	private final CommandLine commandLine;

	public GenerateUniverses(final String[] arguments) throws Exception 
	{
		this.commandLine = GenerateUniverses.parser.parse(options, arguments);

		if (this.commandLine.hasOption("creator"))
			this.creatorKey = EDKeyPair.fromFile(new File(this.commandLine.getOptionValue("creator")), false);
		else
		{
			this.creatorKey = new EDKeyPair();
			KeyPair.toFile(new File("universe.key"), this.creatorKey);
			System.out.println("Universe identity "+this.creatorKey.getIdentity().getHash()+" / "+this.creatorKey.getIdentity().toString());
		}
		
		LOGGER.info("UNIVERSE KEY PRIVATE:  "+Bytes.toHexString(this.creatorKey.getPrivateKey().toByteArray()));
		LOGGER.info("UNIVERSE KEY PUBLIC:   "+Bytes.toHexString(this.creatorKey.getPublicKey().toByteArray()));
	}

	public GenerateUniverses() throws Exception 
	{
		this(new String[] { "universe.key" });
	}

	public Universe generateDeployment() throws Exception 
	{
		long createdAt = Long.parseLong(this.commandLine.getOptionValue("timestamp", Long.toString(System.currentTimeMillis())));

		final Universe.Builder builder = Universe.newBuilder();
		builder.name(this.commandLine.getOptionValue("name"));
		if (this.commandLine.hasOption("description"))
			builder.description(this.commandLine.getOptionValue("description"));
		
		builder.type(Type.valueOf(this.commandLine.getOptionValue("type").toUpperCase()));
		builder.port(Integer.parseInt(this.commandLine.getOptionValue("port")));
		builder.createdAt(createdAt);
		builder.creator(this.creatorKey.getIdentity());
		
		builder.primitivePOW(Integer.parseInt(this.commandLine.getOptionValue("ppow", Integer.toString(DEFAULT_PRIMITIVE_POW))));
		builder.roundInterval(Integer.parseInt(this.commandLine.getOptionValue("interval", Integer.toString(DEFAULT_ROUND_INTERVAL))));
		builder.epochDuration(Long.parseLong(this.commandLine.getOptionValue("epoch", Long.toString(DEFAULT_EPOCH_DURATION))));
		
		final Set<Identity> validators = new LinkedHashSet<Identity>();
		final int numShardGroups;
		if (this.commandLine.hasOption("nodes") && this.commandLine.hasOption("shardgroups"))
		{
			numShardGroups = Integer.parseInt(this.commandLine.getOptionValue("shardgroups"));
			int numValidators = Integer.parseInt(this.commandLine.getOptionValue("nodes"));
			int validatorsPerShardGroup = numValidators / numShardGroups;
			int validatorID = Integer.parseInt(this.commandLine.getOptionValue("offset", "0"));
			
			for (int sg = 0 ; sg < numShardGroups ; sg++)
			{
				final Set<BLSKeyPair> shardValidatorKeys = new HashSet<BLSKeyPair>();
				while(shardValidatorKeys.size() < validatorsPerShardGroup)
				{
					final BLSKeyPair validatorKey = new BLSKeyPair();
					final ShardGroupID shardGroupID = ShardMapper.toShardGroup(validatorKey.getIdentity(), numShardGroups);
					if (shardGroupID.intValue() == sg)
						shardValidatorKeys.add(validatorKey);
				}

				for (final BLSKeyPair shardValidatorKey : shardValidatorKeys)
				{
					KeyPair.toFile(new File("node-"+validatorID+".key"), shardValidatorKey);
					validatorID++;
					validators.add(shardValidatorKey.getIdentity());
				}
			}
			
			builder.setValidators(validators);
			builder.shardGroups(numShardGroups);
		}
		else
		{
			// TODO needs to ensure there are enough validator keys to satisfy the shard count
			final String validatorKeys = this.commandLine.getOptionValue("node.keys");
			if (validatorKeys == null)
				throw new IllegalArgumentException("Expected to be provided validator keys");
			
			final StringTokenizer valdidatorKeysTokenizer = new StringTokenizer(validatorKeys, ",");
			while (valdidatorKeysTokenizer.hasMoreTokens())
			{
				final String validatorKeyToken = valdidatorKeysTokenizer.nextToken();
				validators.add(BLSPublicKey.from(validatorKeyToken).getIdentity());
			}
			
			numShardGroups = 1;
			builder.setValidators(validators);
			builder.shardGroups(numShardGroups);
		}
		
		final Block genesisBlock = createGenesisBlock(validators, numShardGroups, createdAt);
		builder.setGenesis(genesisBlock);
		
		final Universe universe = builder.build();
		final Signature signature = universe.sign(this.creatorKey);
		signature.setVerified(VerificationResult.UNVERIFIED);
		if (universe.verify(this.creatorKey.getPublicKey()) == false)
			throw new ValidationException("Signature failed for "+this.commandLine.getOptionValue("name")+":"+this.commandLine.getOptionValue("type")+" universe deployment");
		
		System.out.println(Serialization.getInstance().toJsonObject(universe, Output.WIRE).toString(4));
		byte[] universeBytes = Serialization.getInstance().toDson(universe, Output.WIRE);
		System.out.println("UNIVERSE - "+this.commandLine.getOptionValue("name")+":"+this.commandLine.getOptionValue("type")+" : "+Bytes.toBase64String(universeBytes));
		
		return universe;
	}

	private Block createGenesisBlock(final Set<Identity> validators, final int numShardGroups, final long timestamp) throws Exception 
	{
		final Atom.Builder atomBuilder = new Atom.Builder();
		
		final Multimap<ShardGroupID, Identity> validatorsToShardGroups = HashMultimap.create();
		for (final Identity validator : validators)
		{
			final ShardGroupID shardGroupID = ShardMapper.toShardGroup(validator, numShardGroups);
			validatorsToShardGroups.put(shardGroupID, validator);
		}

		for (final ShardGroupID shardGroupID : validatorsToShardGroups.keySet())
		{
			final Map<Identity, Long> validatorVotePowers = new HashMap<>();
			for (final Identity validator : validatorsToShardGroups.get(shardGroupID))
				validatorVotePowers.put(validator, Constants.VOTE_POWER_BOOTSTRAP);

			final Blob votePowersBlob = new Blob("application/json", Serialization.getInstance().toJson(new VotePowers(0, validatorVotePowers), Output.WIRE));
			atomBuilder.push(votePowersBlob.asDataURL());
			atomBuilder.push("ledger::epoch("+0+", "+shardGroupID+", hash('"+votePowersBlob.getHash()+"'))");
		}
		
		atomBuilder.push("account::create(account('"+this.creatorKey.getIdentity()+"'))");
		atomBuilder.push("token::create(token('XRD', WRITE), 'Radix token', account('"+this.creatorKey.getIdentity()+"'))");
		atomBuilder.push("token::mint(token('XRD', WRITE), "+UInt256.from(UInt128.HIGH_BIT).toString()+", vault('"+this.creatorKey.getIdentity()+"'), identity('"+this.creatorKey.getIdentity()+"'))");

		atomBuilder.push("token::create(token('CASSIE', WRITE), 'Cassie token', account('"+this.creatorKey.getIdentity()+"'))");
		atomBuilder.push("token::mint(token('CASSIE', WRITE), "+UInt256.from(UInt128.HIGH_BIT).toString()+", vault('"+this.creatorKey.getIdentity()+"'), identity('"+this.creatorKey.getIdentity()+"'))");
		
		atomBuilder.push("token::create(token('HUG', WRITE), 'Hug token', account('"+this.creatorKey.getIdentity()+"'))");
		atomBuilder.push("token::mint(token('HUG', WRITE), "+UInt256.from(UInt128.HIGH_BIT).toString()+", vault('"+this.creatorKey.getIdentity()+"'), identity('"+this.creatorKey.getIdentity()+"'))");
		
		atomBuilder.push("token::create(token('DFP2', WRITE), 'DefiPlaza token', account('"+this.creatorKey.getIdentity()+"'))");
		atomBuilder.push("token::mint(token('DFP2', WRITE), "+UInt256.from(UInt128.HIGH_BIT).toString()+", vault('"+this.creatorKey.getIdentity()+"'), identity('"+this.creatorKey.getIdentity()+"'))");
		
		atomBuilder.push("token::create(token('OCI', WRITE), 'Ociswap token', account('"+this.creatorKey.getIdentity()+"'))");
		atomBuilder.push("token::mint(token('OCI', WRITE), "+UInt256.from(UInt128.HIGH_BIT).toString()+", vault('"+this.creatorKey.getIdentity()+"'), identity('"+this.creatorKey.getIdentity()+"'))");
		
		atomBuilder.push("token::create(token('EARLY', WRITE), 'Early token', account('"+this.creatorKey.getIdentity()+"'))");
		atomBuilder.push("token::mint(token('EARLY', WRITE), "+UInt256.from(UInt128.HIGH_BIT).toString()+", vault('"+this.creatorKey.getIdentity()+"'), identity('"+this.creatorKey.getIdentity()+"'))");
		
		atomBuilder.push("token::create(token('WOWO', WRITE), 'WonderWoman token', account('"+this.creatorKey.getIdentity()+"'))");
		atomBuilder.push("token::mint(token('WOWO', WRITE), "+UInt256.from(UInt128.HIGH_BIT).toString()+", vault('"+this.creatorKey.getIdentity()+"'), identity('"+this.creatorKey.getIdentity()+"'))");
		
		atomBuilder.push("token::create(token('IDA', WRITE), 'XIDAR token', account('"+this.creatorKey.getIdentity()+"'))");
		atomBuilder.push("token::mint(token('IDA', WRITE), "+UInt256.from(UInt128.HIGH_BIT).toString()+", vault('"+this.creatorKey.getIdentity()+"'), identity('"+this.creatorKey.getIdentity()+"'))");
		
		atomBuilder.push("token::create(token('FOTON', WRITE), 'Foton token', account('"+this.creatorKey.getIdentity()+"'))");
		atomBuilder.push("token::mint(token('FOTON', WRITE), "+UInt256.from(UInt128.HIGH_BIT).toString()+", vault('"+this.creatorKey.getIdentity()+"'), identity('"+this.creatorKey.getIdentity()+"'))");
		
		atomBuilder.push("token::create(token('CAVIAR', WRITE), 'Caviar9 token', account('"+this.creatorKey.getIdentity()+"'))");
		atomBuilder.push("token::mint(token('CAVIAR', WRITE), "+UInt256.from(UInt128.HIGH_BIT).toString()+", vault('"+this.creatorKey.getIdentity()+"'), identity('"+this.creatorKey.getIdentity()+"'))");
		
		atomBuilder.push("token::create(token('DAN', WRITE), 'Dan token', account('"+this.creatorKey.getIdentity()+"'))");
		atomBuilder.push("token::mint(token('DAN', WRITE), "+UInt256.from(UInt128.HIGH_BIT).toString()+", vault('"+this.creatorKey.getIdentity()+"'), identity('"+this.creatorKey.getIdentity()+"'))");
		
		atomBuilder.push("token::create(token('PHNX', WRITE), 'Phoenix token', account('"+this.creatorKey.getIdentity()+"'))");
		atomBuilder.push("token::mint(token('PHNX', WRITE), "+UInt256.from(UInt128.HIGH_BIT).toString()+", vault('"+this.creatorKey.getIdentity()+"'), identity('"+this.creatorKey.getIdentity()+"'))");
		
		atomBuilder.push("token::create(token('DPH', WRITE), 'Delphibets token', account('"+this.creatorKey.getIdentity()+"'))");
		atomBuilder.push("token::mint(token('DPH', WRITE), "+UInt256.from(UInt128.HIGH_BIT).toString()+", vault('"+this.creatorKey.getIdentity()+"'), identity('"+this.creatorKey.getIdentity()+"'))");
		
		atomBuilder.push("token::create(token('ASTRL', WRITE), 'Astrolescent token', account('"+this.creatorKey.getIdentity()+"'))");
		atomBuilder.push("token::mint(token('ASTRL', WRITE), "+UInt256.from(UInt128.HIGH_BIT).toString()+", vault('"+this.creatorKey.getIdentity()+"'), identity('"+this.creatorKey.getIdentity()+"'))");

		atomBuilder.push("token::create(token('EDGE', WRITE), 'Edge token', account('"+this.creatorKey.getIdentity()+"'))");
		atomBuilder.push("token::mint(token('EDGE', WRITE), "+UInt256.from(UInt128.HIGH_BIT).toString()+", vault('"+this.creatorKey.getIdentity()+"'), identity('"+this.creatorKey.getIdentity()+"'))");

		atomBuilder.push("token::create(token('GAB', WRITE), 'Gable token', account('"+this.creatorKey.getIdentity()+"'))");
		atomBuilder.push("token::mint(token('GAB', WRITE), "+UInt256.from(UInt128.HIGH_BIT).toString()+", vault('"+this.creatorKey.getIdentity()+"'), identity('"+this.creatorKey.getIdentity()+"'))");

		atomBuilder.push("token::create(token('WEFT', WRITE), 'Weft token', account('"+this.creatorKey.getIdentity()+"'))");
		atomBuilder.push("token::mint(token('WEFT', WRITE), "+UInt256.from(UInt128.HIGH_BIT).toString()+", vault('"+this.creatorKey.getIdentity()+"'), identity('"+this.creatorKey.getIdentity()+"'))");

		atomBuilder.push("token::create(token('USD', WRITE), 'USD token', account('"+this.creatorKey.getIdentity()+"'))");
		atomBuilder.push("token::mint(token('USD', WRITE), "+UInt256.from(UInt128.HIGH_BIT).toString()+", vault('"+this.creatorKey.getIdentity()+"'), identity('"+this.creatorKey.getIdentity()+"'))");

		// Test the manifest integrity and parsing
		ManifestParser.parse(atomBuilder.getManifest());

		// Can build with zero difficulty as signed by universe key
		atomBuilder.signer(this.creatorKey);
		
		final Atom genesisAtom = atomBuilder.build(0);
		final BLSKeyPair genesisKey = new BLSKeyPair();
		final Block genesisBlock = new Block(0l, Hash.ZERO, 0l, UInt256.ZERO, 0, 0, timestamp, genesisKey.getIdentity(), QuorumCertificate.NULL,
									   		 Collections.singletonList(genesisAtom), Collections.emptyList(),
									   		 Collections.emptyList(), Collections.emptyList(), 
									   		 Collections.emptyList(), Collections.emptyList(), 
									   		 Collections.emptyList(), Collections.emptyList());
		genesisBlock.getHeader().sign(genesisKey);
		return genesisBlock;
	}

	public static void main(final String[] arguments) throws Exception 
	{
		System.setProperty("universe.generation", "true");
		GenerateUniverses generateDeployments = new GenerateUniverses(arguments);
		generateDeployments.generateDeployment();
	}
}
