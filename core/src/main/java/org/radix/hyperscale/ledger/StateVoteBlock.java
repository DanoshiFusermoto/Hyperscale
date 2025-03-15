package org.radix.hyperscale.ledger;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.radix.hyperscale.Constants;
import org.radix.hyperscale.common.EphemeralPrimitive;
import org.radix.hyperscale.common.ExtendedObject;
import org.radix.hyperscale.crypto.CryptoException;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.crypto.MerkleTree;
import org.radix.hyperscale.crypto.bls12381.BLSPublicKey;
import org.radix.hyperscale.crypto.bls12381.BLSSignature;
import org.radix.hyperscale.exceptions.ValidationException;
import org.radix.hyperscale.network.TransportParameters;
import org.radix.hyperscale.serialization.DsonCached;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.DsonOutput.Output;
import org.radix.hyperscale.serialization.SerializerId2;
import org.radix.hyperscale.utils.Numbers;

@SerializerId2("ledger.state.vote.block")
@TransportParameters(urgent = true)
@DsonCached
public final class StateVoteBlock extends ExtendedObject implements EphemeralPrimitive {
  @JsonProperty("id")
  @DsonOutput(Output.ALL)
  private Hash ID;

  @JsonProperty("block")
  @DsonOutput(Output.ALL)
  private Hash block;

  @JsonProperty("states")
  @DsonOutput(Output.ALL)
  private Hash states;

  @JsonProperty("merkle")
  @DsonOutput(Output.ALL)
  private Hash merkle;

  @JsonProperty("owner")
  @DsonOutput(Output.ALL)
  private BLSPublicKey owner;

  @JsonProperty("signature")
  @DsonOutput(value = Output.HASH, include = false)
  private BLSSignature signature;

  @JsonProperty("size")
  @DsonOutput(Output.ALL)
  private int size;

  @JsonProperty("votes")
  @DsonOutput(value = Output.HASH, include = false)
  private List<StateVote> votes;

  private StateVoteBlock() {
    super();
  }

  /**
   * Create a compact version of the provided state vote block
   *
   * @param svb
   */
  StateVoteBlock(final StateVoteBlock svb) {
    this();

    this.ID = svb.ID;
    this.block = svb.block;
    this.states = svb.states;
    this.merkle = svb.merkle;
    this.owner = svb.owner;
    this.signature = svb.signature;
    this.size = svb.size;
  }

  public StateVoteBlock(final Hash ID, final Collection<StateVote> votes) {
    this();

    Objects.requireNonNull(votes, "State votes is null");
    Numbers.isZero(votes.size(), "State votes is empty");

    Hash expectedBlock = null;
    Hash expectedMerkle = null;
    BLSPublicKey expectedKey = null;
    BLSSignature expectedSignature = null;
    MerkleTree stateMerkleTree = new MerkleTree(votes.size());
    for (StateVote vote : votes) {
      if (expectedBlock == null) expectedBlock = vote.getBlock();
      else if (vote.getBlock().equals(expectedBlock) == false)
        throw new IllegalStateException(
            "Expected vote block " + expectedBlock + " but found " + vote.getBlock());

      if (expectedMerkle == null) expectedMerkle = vote.getVoteMerkle();
      else if (vote.getVoteMerkle().equals(expectedMerkle) == false)
        throw new IllegalStateException(
            "Expected vote merkle " + expectedMerkle + " but found " + vote.getVoteMerkle());

      if (expectedKey == null) expectedKey = vote.getOwner();
      else if (vote.getOwner().equals(expectedKey) == false)
        throw new IllegalStateException(
            "Expected owner " + expectedKey + " but found " + vote.getOwner());

      if (expectedSignature == null) expectedSignature = vote.getSignature();
      else if (vote.getSignature().equals(expectedSignature) == false)
        throw new IllegalStateException(
            "Expected signature " + expectedSignature + " but found " + vote.getSignature());

      stateMerkleTree.appendLeaf(vote.getAddress().getHash());
    }

    this.votes = new ArrayList<StateVote>(votes);
    this.size = this.votes.size();
    this.ID = ID;
    this.block = expectedBlock;
    this.merkle = expectedMerkle;
    this.owner = expectedKey;
    this.signature = expectedSignature;
    this.states = stateMerkleTree.buildTree();
  }

  public StateVoteBlock getHeader() {
    if (this.votes == null) return this;

    return new StateVoteBlock(this);
  }

  public Hash getID() {
    return this.ID;
  }

  public Hash getBlock() {
    return this.block;
  }

  public long getHeight() {
    return Block.toHeight(this.block);
  }

  public Hash getStateMerkle() {
    return this.states;
  }

  public Hash getExecutionMerkle() {
    return this.merkle;
  }

  public BLSPublicKey getOwner() {
    return this.owner;
  }

  public BLSSignature getSignature() {
    return this.signature;
  }

  public int size() {
    return this.size;
  }

  public void clear() {
    this.votes = null;
  }

  public boolean isHeader() {
    return this.votes == null;
  }

  public boolean isStale() {
    return getAge(TimeUnit.MILLISECONDS)
        > TimeUnit.MILLISECONDS.convert(Constants.PRIMITIVE_STALE, TimeUnit.MINUTES);
  }

  public List<StateAddress> getStateAddresses() {
    List<StateAddress> stateAddresses = new ArrayList<StateAddress>(this.votes.size());
    for (StateVote vote : this.votes) stateAddresses.add(vote.getAddress());

    return Collections.unmodifiableList(stateAddresses);
  }

  public List<StateVote> getVotes() throws ValidationException {
    if (this.votes == null)
      throw new ValidationException(this, "State vote block does not hold votes " + this);

    return Collections.unmodifiableList(this.votes);
  }

  List<StateVote> deriveVotes(final StateVoteBlock referenceStateVoteBlock, final long votePower)
      throws ValidationException {
    if (referenceStateVoteBlock == this) return this.votes;

    if (referenceStateVoteBlock.isHeader())
      throw new IllegalArgumentException("Source state vote block is a header type");

    if (this.votes != null)
      throw new ValidationException(this, "State vote block already has votes in " + this);

    if (this.block.equals(referenceStateVoteBlock.block) == false)
      throw new ValidationException(
          referenceStateVoteBlock,
          "Target block " + this.block + " mismatch in " + referenceStateVoteBlock);

    if (this.merkle.equals(referenceStateVoteBlock.merkle) == false)
      throw new ValidationException(
          referenceStateVoteBlock,
          "Vote merkle " + this.merkle + " mismatch in " + referenceStateVoteBlock);

    if (this.states.equals(referenceStateVoteBlock.states) == false)
      throw new ValidationException(
          referenceStateVoteBlock,
          "State merkle " + this.states + " mismatch in " + referenceStateVoteBlock);

    if (this.size != referenceStateVoteBlock.size)
      throw new ValidationException(
          referenceStateVoteBlock,
          "State size " + this.size + " mismatch in " + referenceStateVoteBlock);

    ArrayList<StateVote> votes = new ArrayList<StateVote>(referenceStateVoteBlock.votes.size());
    for (int i = 0; i < referenceStateVoteBlock.votes.size(); i++) {
      StateVote vote = referenceStateVoteBlock.votes.get(i);
      votes.add(
          new StateVote(
              vote.getObject(),
              vote.getAddress(),
              vote.getAtom(),
              this.block,
              vote.getExecution(),
              vote.getVoteMerkle(),
              vote.getVoteAudit(),
              this.owner,
              votePower,
              this.signature));
    }
    return Collections.unmodifiableList(votes);
  }

  public void validate() throws ValidationException {
    if (this.votes == null)
      throw new UnsupportedOperationException("State vote block is as compact header type");

    MerkleTree merkleTree = new MerkleTree(this.votes.size());
    // IMPORTANT Performed this way to maximise determinism between replicas
    for (StateVote stateVote : this.votes) merkleTree.appendLeaf(stateVote.getVoteMerkle());

    Hash merkleHash = merkleTree.buildTree();
    if (merkleHash.equals(this.merkle) == false)
      throw new ValidationException(
          this, "Expected merkle hash " + this.merkle + " but computed " + merkleHash);
  }

  public boolean verify() throws ValidationException, CryptoException {
    if (this.votes != null) validate();

    return this.owner.verify(this.merkle, this.signature);
  }

  @Override
  public String toString() {
    return getHash()
        + " "
        + size()
        + " "
        + getHeight()
        + "@"
        + getBlock()
        + " -> "
        + getID()
        + ":"
        + getStateMerkle()
        + ":"
        + getExecutionMerkle()
        + " "
        + getOwner();
  }
}
