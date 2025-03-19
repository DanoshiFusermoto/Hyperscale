package org.radix.hyperscale;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.json.JSONObject;
import org.radix.hyperscale.common.BasicObject;
import org.radix.hyperscale.crypto.CryptoException;
import org.radix.hyperscale.crypto.Identity;
import org.radix.hyperscale.crypto.ed25519.EDKeyPair;
import org.radix.hyperscale.crypto.ed25519.EDPublicKey;
import org.radix.hyperscale.crypto.ed25519.EDSignature;
import org.radix.hyperscale.ledger.Block;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.Serialization;
import org.radix.hyperscale.serialization.SerializerId2;
import org.radix.hyperscale.serialization.DsonOutput.Output;
import org.radix.hyperscale.utils.Numbers;

@SerializerId2("universe")
public class Universe extends BasicObject
{
	private static Universe instance = null;
	
	private static final int NAME_MIN_LENGTH = 3;
	private static final int NAME_MAX_LENGTH = 32;
	private static final int DESCRIPTION_MAX_LENGTH = 256;
	private static final long EPOCH_MIN_DURATION = 60;
	private static final long EPOCH_MAX_DURATION = TimeUnit.DAYS.toSeconds(7);
	
	public static Universe get()
	{
		if (instance == null)
			throw new RuntimeException("Configuration not set");
		
		return instance;
	}

	public static Universe create(final JSONObject universe)
	{
		Objects.requireNonNull(universe, "Universe JSON is null");
		
		if (instance != null)
			throw new RuntimeException("Default configuration already set");
		
		instance = Serialization.getInstance().fromJsonObject(universe, Universe.class);
		
		return instance;
	}

	public static Universe create(final byte[] universe) throws IOException
	{
		Objects.requireNonNull(universe, "Universe bytes is null");
		Numbers.isZero(universe.length, "Universe bytes is empty");

		if (instance != null)
			throw new RuntimeException("Default configuration already set");
		
		instance = Serialization.getInstance().fromDson(universe, Universe.class);
		
		return instance;
	}
	
	/**
	 * Universe builder.
	 */
	public static class Builder 
	{
		private Integer port;
		private String name;
		private String description;
		private Type type;
		private Long createdAt;
		private Integer epochDuration;
		private Integer shardGroups;
		private Identity creator;
		private Block genesis;
		private LinkedHashSet<Identity> validators;

		private Builder() 
		{
			// Nothing to do here
		}

		/**
		 * Sets the TCP/UDP port for the universe.
		 *
		 * @param port The TCP/UDP port for the universe to use, {@code 0 <= port <= 65,535}.
		 * @return A reference to {@code this} to allow method chaining.
		 */
		public Builder port(final int port) 
		{
			Numbers.inRange(port, 1, 65535, "Invalid port number: " + port);
			this.port = port;
			return this;
		}

		/**
		 * Sets the name of the universe.
		 * Ideally the universe name is a short identifier for the universe.
		 *
		 * @param name The name of the universe.
		 * @return A reference to {@code this} to allow method chaining.
		 */
		public Builder name(final String name) 
		{
			Objects.requireNonNull(name, "Universe name is null");
			Numbers.inRange(name.length(), NAME_MIN_LENGTH, NAME_MAX_LENGTH, "Universe name has invalid length");
			
			this.name = name;
			return this;
		}

		/**
		 * Set the description of the universe.
		 * The universe description is a longer description of the universe.
		 *
		 * @param description The description of the universe.
		 * @return A reference to {@code this} to allow method chaining.
		 */
		public Builder description(final String description) 
		{
			Objects.requireNonNull(description, "Universe description is null");
			Numbers.inRange(description.length(), 0, DESCRIPTION_MAX_LENGTH, "Universe description has invalid length");

			this.description = description;
			return this;
		}

		/**
		 * Sets the type of the universe, one of {@link Universe.Type}.
		 *
		 * @param type The type of the universe.
		 * @return A reference to {@code this} to allow method chaining.
		 */
		public Builder type(final Type type) 
		{
			Objects.requireNonNull(type, "Universe type is null");
			this.type = type;
			return this;
		}

		/**
		 * Sets the creation timestamp of the universe.
		 *
		 * @param timestamp The creation timestamp of the universe.
		 * @return A reference to {@code this} to allow method chaining.
		 */
		public Builder createdAt(final long createdAt) 
		{
			Numbers.isNegative(createdAt, "Creation timestamp is invalid: " + createdAt);
			this.createdAt = createdAt;
			return this;
		}

		/**
		 * Sets the epoch duration in seconds of the universe.
		 *
		 * @param epoch The duration for an epoch in seconds of the universe.
		 * @return A reference to {@code this} to allow method chaining.
		 */
		public Builder epochDuration(final int epochDuration) 
		{
			Numbers.inRange(epochDuration, EPOCH_MIN_DURATION, EPOCH_MAX_DURATION, "Invalid epoch duration: "+epochDuration);
			this.epochDuration = epochDuration;
			return this;
		}

		/**
		 * Sets the initial shard group count of the universe.
		 *
		 * @param shardGroups The quantity of initial shard groups.
		 * @return A reference to {@code this} to allow method chaining.
		 */
		public Builder shardGroups(final int shardGroups) 
		{
			Numbers.lessThan(shardGroups, 1, "Invalid shard groups: " + shardGroups);
			this.shardGroups = shardGroups;
			return this;
		}

		/**
		 * Sets the universe creators public key.
		 *
		 * @param creator The universe creators public key.
		 * @return A reference to {@code this} to allow method chaining.
		 */
		public Builder creator(final Identity creator) 
		{
			this.creator = Objects.requireNonNull(creator, "Universe creator identity is null");
			return this;
		}

		/**
		 * Sets the genesis.
		 *
		 * @param genesis The genesis.
		 * @return A reference to {@code this} to allow method chaining.
		 */
		public Builder setGenesis(final Block genesis) 
		{
			Objects.requireNonNull(genesis, "Universe genesis block is null");
			this.genesis = genesis;
			return this;
		}

		/**
		 * Sets the Genesis validators.
		 *
		 * @param validators The Genesis validators.
		 * @return A reference to {@code this} to allow method chaining.
		 */
		public Builder setValidators(final Set<Identity> validators) 
		{
			Objects.requireNonNull(validators, "Universe validators is null");
			Numbers.lessThan(validators.size(), 1, "Universe validators is null");
			this.validators = new LinkedHashSet<Identity>(validators);
			return this;
		}
		
		/**
		 * Validate and build a universe from the specified data.
		 *
		 * @return The freshly build universe object.
		 */
		public Universe build() 
		{
			require(this.port, "Port number");
			require(this.name, "Name");
			require(this.description, "Description");
			require(this.type, "Type");
			require(this.createdAt, "Created timestamp");
			require(this.creator, "Creator");
			require(this.genesis , "Genesis block");
			require(this.validators, "Genesis validators");
			require(this.shardGroups, "Initial shard groups");
			require(this.epochDuration, "Epoch duration");
			return new Universe(this);
		}

		private void require(Object item, String what) 
		{
			if (item == null)
				throw new IllegalStateException(what+" must be specified");
		}
	}

	/**
	 * Construct a new {@link Builder}.
	 *
	 * @return The freshly constructed builder.
	 */
	public static Builder newBuilder() 
	{
		return new Builder();
	}

	/**
	 * Computes universe magic number from specified parameters.
	 *
	 * @param creator {@link ECPublicKey} of universe creator to use when calculating universe magic
	 * @param createdAt universe timestamp to use when calculating universe magic
	 * @param port universe port to use when calculating universe magic
	 * @param type universe type to use when calculating universe magic
	 * @return The universe magic
	 */
	public static int computeMagic(Identity creator, long createdAt, int shardGroups, int epochDuration, int port, Type type) 
	{
		return (int) (31l * creator.getHash().asLong() * 19l * createdAt * 13l * epochDuration * 7l * port * 5l * shardGroups + type.ordinal());
	}

	public enum Type
	{
		PRODUCTION,
		TEST,
		DEVELOPMENT;
		
		@JsonValue
		@Override
		public String toString() 
		{
			return this.name();
		}
	}

	@JsonProperty("name")
	@DsonOutput(Output.ALL)
	private String name;

	@JsonProperty("description")
	@DsonOutput(Output.ALL)
	private String description;

	@JsonProperty("created_at")
	@DsonOutput(Output.ALL)
	private long createdAt;

	@JsonProperty("port")
	@DsonOutput(Output.ALL)
	private int	port;

	@JsonProperty("epoch_duration")
	@DsonOutput(Output.ALL)
	private int	epochDuration;

	@JsonProperty("shard_groups")
	@DsonOutput(Output.ALL)
	private int	shardGroups;

	@JsonProperty("type")
	@DsonOutput(Output.ALL)
	private Type type;

	@JsonProperty("genesis")
	@DsonOutput(Output.ALL)
	private Block genesis;

	@JsonProperty("validators")
	@DsonOutput(Output.ALL)
	@JsonDeserialize(as=LinkedHashSet.class)
	private Set<Identity> validators;

	@JsonProperty("creator")
	@DsonOutput(Output.ALL)
	private Identity creator;

	@JsonProperty("signature")
	@DsonOutput(Output.ALL)
	private EDSignature signature;

	Universe() {
		// No-arg constructor for serializer
	}

	private Universe(final Builder builder) 
	{
		super();

		this.port = builder.port;
		this.name = builder.name;
		this.description = builder.description;
		this.type = builder.type;
		this.epochDuration = builder.epochDuration;
		this.createdAt = builder.createdAt;
		this.creator = builder.creator;
		this.genesis = builder.genesis;
		this.validators = builder.validators;
		this.shardGroups = builder.shardGroups;
	}

	/**
	 * Magic identifier for universe.
	 *
	 * @return
	 */
	@JsonProperty("magic")
	@DsonOutput(value = Output.HASH, include = false)
	public int getMagic() 
	{
		return computeMagic(this.creator, this.createdAt, this.shardGroups, this.epochDuration, this.port, this.type);
	}

	/**
	 * The duration of an epoch in seconds.
	 *
	 * @return
	 */
	public int getEpochDuration()
	{
		return this.epochDuration;
	}

	/**
	 * The number of initial shard groups.
	 *
	 * @return
	 */
	public int shardGroupCount()
	{
		return this.shardGroups;
	}

	/**
	 * The name of the universe.
	 *
	 * @return
	 */
	public String getName()
	{
		return this.name;
	}

	/**
	 * The universe description.
	 *
	 * @return
	 */
	public String getDescription()
	{
		return this.description;
	}

	/**
	 * The default TCP/UDP port for the universe.
	 *
	 * @return
	 */
	public int getPort()
	{
		return this.port;
	}

	/**
	 * The UTC 'BigBang' timestamp for the universe.
	 *
	 * @return
	 */
	public long getCreatedAt()
	{
		return this.createdAt;
	}

	/**
	 * Whether this is a production universe.
	 *
	 * @return
	 */
	public boolean isProduction()
	{
		return this.type.equals(Type.PRODUCTION);
	}

	/**
	 * Whether this is a test universe.
	 *
	 * @return
	 */
	public boolean isTest()
	{
		return this.type.equals(Type.TEST);
	}

	/**
	 * Whether this is a development universe.
	 *
	 * @return
	 */
	public boolean isDevelopment()
	{
		return this.type.equals(Type.DEVELOPMENT);
	}

	/**
	 * Gets this Universe Genesis.
	 *
	 * @return
	 */
	public Block getGenesis()
	{
		return this.genesis;
	}
	
	/**
	 * Gets this Universe Genesis validators.
	 *
	 * @return
	 */
	public Set<Identity> getValidators()
	{
		return this.validators;
	}

	/**
	 * Get creator key.
	 *
	 * @return
	 */
	public Identity getCreator()
	{
		return this.creator;
	}

	public EDSignature getSignature()
	{
		return this.signature;
	}

	public void setSignature(final EDSignature signature)
	{
		this.signature = signature;
	}

	public void sign(final EDKeyPair key) throws CryptoException
	{
		this.signature = key.getPrivateKey().sign(getHash());
	}

	public boolean verify(final EDPublicKey key) throws CryptoException
	{
		return key.verify(getHash(), this.signature);
	}
	
	/**
	 * Check whether a given universe is valid
	 * @throws CryptoException 
	 */
	public void validate() throws CryptoException 
	{
		// Check signature
		if (this.creator.<EDPublicKey>getKey().verify(getHash(), this.signature) == false)
			throw new IllegalStateException("Invalid universe signature");
		
		if (this.genesis == null)
			throw new IllegalStateException("No genesis block defined");
	}
}
