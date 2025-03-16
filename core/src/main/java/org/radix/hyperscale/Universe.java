package org.radix.hyperscale;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import org.json.JSONObject;
import org.radix.hyperscale.common.BasicObject;
import org.radix.hyperscale.crypto.CryptoException;
import org.radix.hyperscale.crypto.Identity;
import org.radix.hyperscale.crypto.ed25519.EDKeyPair;
import org.radix.hyperscale.crypto.ed25519.EDPublicKey;
import org.radix.hyperscale.crypto.ed25519.EDSignature;
import org.radix.hyperscale.ledger.Block;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.DsonOutput.Output;
import org.radix.hyperscale.serialization.Serialization;
import org.radix.hyperscale.serialization.SerializerId2;
import org.radix.hyperscale.utils.Numbers;

@SerializerId2("universe")
public class Universe extends BasicObject {
  private static Universe instance = null;

  public static Universe getDefault() {
    if (instance == null) throw new RuntimeException("Configuration not set");

    return instance;
  }

  public static Universe createAsDefault(JSONObject universe) {
    if (instance != null) throw new RuntimeException("Default configuration already set");

    instance = Serialization.getInstance().fromJsonObject(universe, Universe.class);

    return instance;
  }

  public static Universe createAsDefault(byte[] universe) throws IOException {
    if (instance != null) throw new RuntimeException("Default configuration already set");

    instance = Serialization.getInstance().fromDson(universe, Universe.class);

    return instance;
  }

  public static Universe clearDefault() {
    Universe universe = getDefault();
    instance = null;
    return universe;
  }

  /** Universe builder. */
  public static class Builder {
    private Integer port;
    private String name;
    private String description;
    private Type type;
    private Long timestamp;
    private Integer epoch;
    private Integer shardGroups;
    private Identity creator;
    private Block genesis;
    private LinkedHashSet<Identity> validators;

    private Builder() {
      // Nothing to do here
    }

    /**
     * Sets the TCP/UDP port for the universe.
     *
     * @param port The TCP/UDP port for the universe to use, {@code 0 <= port <= 65,535}.
     * @return A reference to {@code this} to allow method chaining.
     */
    public Builder port(int port) {
      Numbers.inRange(port, 1, 65535, "Invalid port number: " + port);
      this.port = port;
      return this;
    }

    /**
     * Sets the name of the universe. Ideally the universe name is a short identifier for the
     * universe.
     *
     * @param name The name of the universe.
     * @return A reference to {@code this} to allow method chaining.
     */
    public Builder name(String name) {
      this.name = Objects.requireNonNull(name);
      return this;
    }

    /**
     * Set the description of the universe. The universe description is a longer description of the
     * universe.
     *
     * @param description The description of the universe.
     * @return A reference to {@code this} to allow method chaining.
     */
    public Builder description(String description) {
      this.description = Objects.requireNonNull(description);
      return this;
    }

    /**
     * Sets the type of the universe, one of {@link Universe.Type}.
     *
     * @param type The type of the universe.
     * @return A reference to {@code this} to allow method chaining.
     */
    public Builder type(Type type) {
      this.type = Objects.requireNonNull(type);
      return this;
    }

    /**
     * Sets the creation timestamp of the universe.
     *
     * @param timestamp The creation timestamp of the universe.
     * @return A reference to {@code this} to allow method chaining.
     */
    public Builder timestamp(long timestamp) {
      Numbers.isNegative(timestamp, "Invalid timestamp: " + timestamp);
      this.timestamp = timestamp;
      return this;
    }

    /**
     * Sets the epoch duration in events of the universe.
     *
     * @param epoch The event duration for an epoch of the universe.
     * @return A reference to {@code this} to allow method chaining.
     */
    public Builder epoch(int epoch) {
      Numbers.isNegative(epoch, "Invalid epoch: " + timestamp);
      this.epoch = epoch;
      return this;
    }

    /**
     * Sets the initial shard group count of the universe.
     *
     * @param shardGroups The quantity of initial shard groups.
     * @return A reference to {@code this} to allow method chaining.
     */
    public Builder shardGroups(int shardGroups) {
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
    public Builder creator(Identity creator) {
      this.creator = Objects.requireNonNull(creator);
      return this;
    }

    /**
     * Sets the genesis.
     *
     * @param genesis The genesis.
     * @return A reference to {@code this} to allow method chaining.
     */
    public Builder setGenesis(Block genesis) {
      Objects.requireNonNull(genesis);
      this.genesis = genesis;
      return this;
    }

    /**
     * Sets the Genesis validators.
     *
     * @param validators The Genesis validators.
     * @return A reference to {@code this} to allow method chaining.
     */
    public Builder setValidators(Set<Identity> validators) {
      Objects.requireNonNull(validators);
      this.validators = new LinkedHashSet<Identity>(validators);
      return this;
    }

    /**
     * Validate and build a universe from the specified data.
     *
     * @return The freshly build universe object.
     */
    public Universe build() {
      require(this.port, "Port number");
      require(this.name, "Name");
      require(this.description, "Description");
      require(this.type, "universeype");
      require(this.timestamp, "Timestamp");
      require(this.creator, "Creator");
      require(this.genesis, "Genesis block");
      require(this.validators, "Genesis validators");
      require(this.shardGroups, "Initial shard groups");
      return new Universe(this);
    }

    private void require(Object item, String what) {
      if (item == null) throw new IllegalStateException(what + " must be specified");
    }
  }

  /**
   * Construct a new {@link Builder}.
   *
   * @return The freshly constructed builder.
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Computes universe magic number from specified parameters.
   *
   * @param creator {@link ECPublicKey} of universe creator to use when calculating universe magic
   * @param timestamp universe timestamp to use when calculating universe magic
   * @param port universe port to use when calculating universe magic
   * @param type universe type to use when calculating universe magic
   * @return The universe magic
   */
  public static int computeMagic(
      Identity creator, long timestamp, int shardGroups, int epoch, int port, Type type) {
    return (int)
        (31l
                * creator.getHash().asLong()
                * 19l
                * timestamp
                * 13l
                * epoch
                * 7l
                * port
                * 5l
                * shardGroups
            + type.ordinal());
  }

  public enum Type {
    PRODUCTION,
    TEST,
    DEVELOPMENT;

    @JsonValue
    @Override
    public String toString() {
      return this.name();
    }
  }

  @JsonProperty("name")
  @DsonOutput(Output.ALL)
  private String name;

  @JsonProperty("description")
  @DsonOutput(Output.ALL)
  private String description;

  @JsonProperty("timestamp")
  @DsonOutput(Output.ALL)
  private long timestamp;

  @JsonProperty("port")
  @DsonOutput(Output.ALL)
  private int port;

  @JsonProperty("epoch")
  @DsonOutput(Output.ALL)
  private int epoch;

  @JsonProperty("shard_groups")
  @DsonOutput(Output.ALL)
  private int shardGroups;

  @JsonProperty("type")
  @DsonOutput(Output.ALL)
  private Type type;

  @JsonProperty("genesis")
  @DsonOutput(Output.ALL)
  private Block genesis;

  @JsonProperty("validators")
  @DsonOutput(Output.ALL)
  @JsonDeserialize(as = LinkedHashSet.class)
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

  private Universe(Builder builder) {
    super();

    this.port = builder.port.intValue();
    this.name = builder.name;
    this.description = builder.description;
    this.type = builder.type;
    this.epoch = builder.epoch;
    this.timestamp = builder.timestamp.longValue();
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
  public int getMagic() {
    return computeMagic(
        this.creator, this.timestamp, this.shardGroups, this.epoch, this.port, this.type);
  }

  /**
   * The size of an epoch in events.
   *
   * @return
   */
  public int getEpoch() {
    return this.epoch;
  }

  public long toEpoch(long height) {
    return height / this.epoch;
  }

  /**
   * The number of initial shard groups.
   *
   * @return
   */
  public int shardGroupCount() {
    return this.shardGroups;
  }

  /**
   * The name of the universe.
   *
   * @return
   */
  public String getName() {
    return this.name;
  }

  /**
   * The universe description.
   *
   * @return
   */
  public String getDescription() {
    return this.description;
  }

  /**
   * The default TCP/UDP port for the universe.
   *
   * @return
   */
  public int getPort() {
    return this.port;
  }

  /**
   * The UTC 'BigBang' timestamp for the universe.
   *
   * @return
   */
  public long getTimestamp() {
    return this.timestamp;
  }

  /**
   * Whether this is a production universe.
   *
   * @return
   */
  public boolean isProduction() {
    return this.type.equals(Type.PRODUCTION);
  }

  /**
   * Whether this is a test universe.
   *
   * @return
   */
  public boolean isTest() {
    return this.type.equals(Type.TEST);
  }

  /**
   * Whether this is a development universe.
   *
   * @return
   */
  public boolean isDevelopment() {
    return this.type.equals(Type.DEVELOPMENT);
  }

  /**
   * Gets this Universe Genesis.
   *
   * @return
   */
  public Block getGenesis() {
    return this.genesis;
  }

  /**
   * Gets this Universe Genesis validators.
   *
   * @return
   */
  public Set<Identity> getValidators() {
    return this.validators;
  }

  /**
   * Get creator key.
   *
   * @return
   */
  public Identity getCreator() {
    return this.creator;
  }

  public EDSignature getSignature() {
    return this.signature;
  }

  public void setSignature(EDSignature signature) {
    this.signature = signature;
  }

  public void sign(EDKeyPair key) throws CryptoException {
    this.signature = key.getPrivateKey().sign(getHash());
  }

  public boolean verify(EDPublicKey key) throws CryptoException {
    return key.verify(getHash(), this.signature);
  }

  /**
   * Check whether a given universe is valid
   *
   * @throws CryptoException
   */
  public void validate() throws CryptoException {
    // Check signature
    if (this.creator.<EDPublicKey>getKey().verify(getHash(), this.signature) == false)
      throw new IllegalStateException("Invalid universe signature");

    if (this.genesis == null) throw new IllegalStateException("No genesis block defined");
  }
}
