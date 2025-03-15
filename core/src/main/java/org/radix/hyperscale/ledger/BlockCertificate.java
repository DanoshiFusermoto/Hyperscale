package org.radix.hyperscale.ledger;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;
import org.radix.hyperscale.collections.Bloom;
import org.radix.hyperscale.crypto.CryptoException;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.crypto.VoteCertificate;
import org.radix.hyperscale.crypto.bls12381.BLSPublicKey;
import org.radix.hyperscale.crypto.bls12381.BLSSignature;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.DsonOutput.Output;
import org.radix.hyperscale.serialization.SerializerId2;

@SerializerId2("ledger.block.certificate")
@StateContext("block.certificate")
public final class BlockCertificate extends VoteCertificate {
  @JsonProperty("block")
  @DsonOutput(Output.ALL)
  private Hash block;

  @SuppressWarnings("unused")
  private BlockCertificate() {
    super();
  }

  BlockCertificate(final Hash block) {
    super();

    Objects.requireNonNull(block, "Block is null");
    Hash.notZero(block, "Block is ZERO");

    this.block = block;
  }

  BlockCertificate(
      final Hash block, final Bloom signers, final BLSPublicKey key, final BLSSignature signature) {
    super(CommitDecision.ACCEPT, signers, key, signature);

    Objects.requireNonNull(block, "Block is null");
    Hash.notZero(block, "Block is ZERO");

    this.block = block;
  }

  public Hash getBlock() {
    return this.block;
  }

  public long getHeight() {
    return Block.toHeight(this.block);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T getObject() {
    return (T) this.block;
  }

  @Override
  protected Hash getTarget() throws CryptoException {
    return this.block;
  }
}
