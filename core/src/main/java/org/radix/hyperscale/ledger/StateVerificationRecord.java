package org.radix.hyperscale.ledger;

import java.util.Objects;
import org.radix.hyperscale.collections.Bloom;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.crypto.bls12381.BLSPublicKey;
import org.radix.hyperscale.crypto.bls12381.BLSSignature;

final class StateVerificationRecord {
  private final Hash merkleHash;
  private final Bloom signers;
  private final BLSSignature signature;
  private final BLSPublicKey key;

  StateVerificationRecord(
      final Hash merkleHash,
      final Bloom signers,
      final BLSPublicKey key,
      final BLSSignature signature) {
    this.merkleHash = Objects.requireNonNull(merkleHash, "Vote merkle root is null");
    Hash.notZero(merkleHash, "Vote merkle is zero");

    this.key = Objects.requireNonNull(key, "Aggregated key is null");
    this.signers = Objects.requireNonNull(signers, "Signers bloom is null");
    this.signature = Objects.requireNonNull(signature, "Aggregated signature is null");
  }

  public Hash getMerkleHash() {
    return this.merkleHash;
  }

  public BLSSignature getSignature() {
    return this.signature;
  }

  public Bloom getSigners() {
    return this.signers;
  }

  public BLSPublicKey getKey() {
    return this.key;
  }
}
