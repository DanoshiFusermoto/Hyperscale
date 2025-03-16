package org.radix.hyperscale.crypto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Objects;
import org.radix.hyperscale.collections.Bloom;
import org.radix.hyperscale.crypto.bls12381.BLS12381;
import org.radix.hyperscale.crypto.bls12381.BLSPublicKey;
import org.radix.hyperscale.crypto.bls12381.BLSSignature;
import org.radix.hyperscale.ledger.CommitDecision;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.DsonOutput.Output;
import org.radix.hyperscale.utils.Numbers;

public abstract class VoteCertificate extends Certificate {
  @JsonProperty("key")
  @DsonOutput(value = {Output.API, Output.WIRE, Output.PERSIST})
  private BLSPublicKey key;

  @JsonProperty("signers")
  @DsonOutput(value = {Output.API, Output.WIRE, Output.PERSIST})
  private Bloom signers;

  @JsonProperty("signature")
  @DsonOutput(value = {Output.API, Output.WIRE, Output.PERSIST})
  private BLSSignature signature;

  protected VoteCertificate() {
    super();
  }

  protected VoteCertificate(final CommitDecision decision) throws CryptoException {
    super(decision);
  }

  protected VoteCertificate(
      final CommitDecision decision,
      final Bloom signers,
      final BLSPublicKey key,
      final BLSSignature signature) {
    super(decision);

    Objects.requireNonNull(key, "Aggregated key is null");
    Objects.requireNonNull(signature, "Signature is null");
    Objects.requireNonNull(signers, "Identities is null");
    Numbers.isZero(signers.count(), "Signers is empty");

    this.key = key;
    this.signers = signers;
    this.signature = signature;
  }

  public final Bloom getSigners() {
    return this.signers;
  }

  public final BLSPublicKey getKey() {
    return this.key;
  }

  public final BLSSignature getSignature() {
    return this.signature;
  }

  protected abstract Hash getTarget() throws CryptoException;

  final boolean verify(final List<BLSPublicKey> identities) throws CryptoException {
    Objects.requireNonNull(identities, "Identity is null");
    Numbers.isZero(identities.size(), "Identities is empty");

    BLSPublicKey aggregated = BLS12381.aggregatePublicKey(identities);
    return BLS12381.verify(aggregated, this.signature, getTarget().toByteArray());
  }
}
