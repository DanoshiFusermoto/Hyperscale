package org.radix.hyperscale.ledger.sme;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.TreeMap;
import org.radix.hyperscale.Constants;
import org.radix.hyperscale.Universe;
import org.radix.hyperscale.crypto.ComputeKey;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.crypto.Identity;
import org.radix.hyperscale.ledger.ShardGroupID;
import org.radix.hyperscale.ledger.ShardMapper;
import org.radix.hyperscale.ledger.StateAddress;
import org.radix.hyperscale.ledger.StateLockMode;
import org.radix.hyperscale.ledger.Substate.NativeField;
import org.radix.hyperscale.ledger.VotePowers;
import org.radix.hyperscale.ledger.sme.exceptions.StateMachineExecutionException;
import org.radix.hyperscale.utils.Numbers;

public class Epoch implements GlobalInstruction {
  private final long clock;
  private final ShardGroupID shardGroupID;
  private final VotePowers powers;

  public Epoch(final long clock, final ShardGroupID shardGroupID, final VotePowers powers) {
    Objects.requireNonNull(powers, "Powers is null");
    Objects.requireNonNull(shardGroupID, "Shard group ID is null");
    Numbers.isNegative(clock, "Clock is negative");

    this.clock = clock;
    this.powers = powers;
    this.shardGroupID = shardGroupID;
  }

  public long getClock() {
    return this.clock;
  }

  public ShardGroupID getShardGroupID() {
    return this.shardGroupID;
  }

  public VotePowers getPowers() {
    return this.powers;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + (int) (this.clock & 0xFFFFFFFF);
    result = prime * result + this.powers.hashCode();
    result = prime * result + this.shardGroupID.hashCode();
    return result;
  }

  @Override
  public boolean equals(Object object) {
    if (object == null) return false;

    if (this == object) return true;

    if (object instanceof Epoch epoch) {
      if (this.clock != epoch.clock) return false;

      if (this.shardGroupID.equals(epoch.shardGroupID) == false) return false;

      if (this.powers.equals(epoch.powers) == false) return false;

      return true;
    }

    return false;
  }

  void lock(final StateMachine stateMachine) {
    stateMachine.lock(StateAddress.from("epoch", Long.toString(getClock())), StateLockMode.WRITE);
    stateMachine.lock(
        StateAddress.from("epoch", getClock() + ":" + getShardGroupID()), StateLockMode.WRITE);

    // Genesis
    if (getClock() == 0
        && Universe.getDefault().getGenesis().contains(stateMachine.getPendingAtom().getHash())) {
      for (int e = 1; e < Constants.VOTE_POWER_MATURITY_EPOCHS; e++)
        stateMachine.lock(
            StateAddress.from("epoch", e + ":" + getShardGroupID()), StateLockMode.WRITE);
    }

    for (final Entry<Identity, Long> votePower : this.powers.getAll().entrySet())
      stateMachine.lock(StateAddress.from("validator", votePower.getKey()), StateLockMode.WRITE);
  }

  void execute(final StateMachine stateMachine) throws StateMachineExecutionException {
    final org.radix.hyperscale.ledger.Epoch localExpectedEpoch =
        org.radix.hyperscale.ledger.Epoch.from(stateMachine.getPendingAtom().getBlockHeader())
            .increment(Constants.VOTE_POWER_MATURITY_EPOCHS - 1);
    final int numShardGroups = stateMachine.getContext().getLedger().numShardGroups(getClock());
    final Identity localIdentity = stateMachine.getContext().getNode().getIdentity();

    // Ensure the epoch substate is created
    final StateAddress epochAddress = StateAddress.from("epoch", Long.toString(getClock()));
    if (stateMachine.exists(epochAddress)) {
      stateMachine.assertExists(epochAddress);
      if (stateMachine.get(epochAddress, NativeField.VOTE_POWER) == null)
        throw new StateMachineExecutionException(
            "Expected field VOTE_POWER in epoch:" + getClock() + " substate not found");
    } else {
      stateMachine.assertCreate(epochAddress, ComputeKey.NULL.getIdentity());
      stateMachine.set(
          epochAddress,
          NativeField.VOTE_POWER,
          new TreeMap<ShardGroupID, VotePowers>(),
          ComputeKey.NULL.getIdentity());
    }

    // TODO look up shard group ID by epoch clock
    final ShardGroupID localShardGroupID = ShardMapper.toShardGroup(localIdentity, numShardGroups);

    // Genesis
    if (getClock() == 0
        && Universe.getDefault().getGenesis().contains(stateMachine.getPendingAtom().getHash())) {
      for (int e = 0; e < Constants.VOTE_POWER_MATURITY_EPOCHS; e++) {
        final StateAddress epochShardGroupAddress =
            StateAddress.from("epoch", e + ":" + getShardGroupID());
        stateMachine.assertCreate(epochShardGroupAddress, ComputeKey.NULL.getIdentity());
        stateMachine.set(
            epochShardGroupAddress,
            NativeField.ATOM,
            stateMachine.getPendingAtom().getHash(),
            ComputeKey.NULL.getIdentity());
      }

      for (final Entry<Identity, Long> votePower : this.powers.getAll().entrySet()) {
        StateAddress validatorAddress = StateAddress.from("validator", votePower.getKey());
        stateMachine.assertCreate(validatorAddress, ComputeKey.NULL.getIdentity());

        ShardGroupID validatorShardGroupID =
            ShardMapper.toShardGroup(votePower.getKey(), numShardGroups);
        stateMachine.set(
            validatorAddress,
            NativeField.SHARD_GROUP,
            validatorShardGroupID,
            ComputeKey.NULL.getIdentity());
        stateMachine.set(
            validatorAddress,
            NativeField.VOTE_POWER,
            votePower.getValue(),
            ComputeKey.NULL.getIdentity());
      }
    } else {
      // Epoch vote power map
      final Map<ShardGroupID, VotePowers> epochPowers =
          stateMachine.get(epochAddress, NativeField.VOTE_POWER);

      // The Epoch substate for the referenced shard group
      final StateAddress epochShardGroupAddress =
          StateAddress.from("epoch", getClock() + ":" + getShardGroupID());

      // Mutate the substates with the relevant stake/vote power changes
      stateMachine.assertCreate(epochShardGroupAddress, ComputeKey.NULL.getIdentity());

      // Set this atom as a reference
      stateMachine.set(
          epochShardGroupAddress,
          NativeField.ATOM,
          stateMachine.getPendingAtom().getHash(),
          ComputeKey.NULL.getIdentity());

      // Verifications by validators present in referenced shard group
      if (localShardGroupID.equals(getShardGroupID())) {
        if (getClock() != localExpectedEpoch.getClock())
          throw new StateMachineExecutionException(
              "Epoch clock is invalid for shard group "
                  + getShardGroupID()
                  + " Expected "
                  + localExpectedEpoch.getClock()
                  + "  Presented "
                  + getClock());
      }

      // Update the stake/vote powers for the validators referenced in the applicable shard group
      for (final Entry<Identity, Long> votePower : this.powers.getAll().entrySet()) {
        final StateAddress validatorAddress = StateAddress.from("validator", votePower.getKey());

        // TODO look up shard group ID by epoch clock
        final ShardGroupID validatorShardGroupID =
            ShardMapper.toShardGroup(votePower.getKey(), numShardGroups);

        // Verifications by validators present in referenced shard group
        // Other validators in other groups rely on the outcome of this verification
        if (localShardGroupID.equals(getShardGroupID())) {
          // Verify target validator is present in the shard group for the epoch
          if (validatorShardGroupID.equals(getShardGroupID()) == false)
            throw new StateMachineExecutionException(
                "Validator "
                    + votePower.getKey()
                    + " not present in shard group "
                    + getShardGroupID()
                    + " for epoch "
                    + getClock());

          // Verify target validator vote power change matches to local expectation
          final long validatorPendingVotePower =
              stateMachine
                  .getContext()
                  .getLedger()
                  .getValidatorHandler()
                  .verify(localExpectedEpoch, votePower.getKey());
          if (validatorPendingVotePower != votePower.getValue())
            throw new StateMachineExecutionException(
                "Vote power mismatch for validator "
                    + votePower.getKey()
                    + " in shard group "
                    + getShardGroupID()
                    + " for epoch "
                    + getClock()
                    + ":  Expected "
                    + validatorPendingVotePower
                    + "  Presented "
                    + votePower.getValue());
        }

        stateMachine.set(
            validatorAddress,
            NativeField.VOTE_POWER,
            votePower.getValue(),
            ComputeKey.NULL.getIdentity());

        // TODO What additional checks can validators not currently in the specificied shard group
        // perform.
        // TODO What additional checks can validators responsible for substate perform.
      }

      // Update the epoch object vote powers
      if (epochPowers.containsKey(getShardGroupID()) == true)
        throw new StateMachineExecutionException(
            "Epoch "
                + getClock()
                + " vote powers for shard group "
                + getShardGroupID()
                + " already accepted");
      epochPowers.put(getShardGroupID(), getPowers());

      // Add the shuffle seed proposed by the validator presenting this epoch update
      final Map<ShardGroupID, Hash> epochShuffleSeeds =
          stateMachine.get(
              epochAddress, NativeField.SHUFFLE_SEEDS, new TreeMap<ShardGroupID, Hash>());
      epochShuffleSeeds.put(getShardGroupID(), stateMachine.getPendingAtom().getHash());
      stateMachine.set(
          epochAddress,
          NativeField.SHUFFLE_SEEDS,
          epochShuffleSeeds,
          ComputeKey.NULL.getIdentity());
    }
  }
}
