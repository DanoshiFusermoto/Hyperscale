package org.radix.hyperscale.crypto;

import java.util.Arrays;
import org.radix.hyperscale.serialization.Polymorphic;

public abstract class Signature implements Polymorphic {
  public enum VerificationResult {
    UNVERIFIED,
    SUCCESS,
    FAILED
  }

  private volatile long hashCode = Long.MAX_VALUE;

  private volatile VerificationResult verified = VerificationResult.UNVERIFIED;

  public abstract byte[] toByteArray();

  public final void reset() {
    this.verified = VerificationResult.UNVERIFIED;
  }

  public final VerificationResult isVerified() {
    return this.verified;
  }

  public final void setVerified(VerificationResult result) {
    this.verified = result;
  }

  @Override
  public final synchronized int hashCode() {
    if (this.hashCode == Long.MAX_VALUE) this.hashCode = Arrays.hashCode(toByteArray());

    return (int) this.hashCode;
  }

  @Override
  public final boolean equals(Object object) {
    if (object == this) return true;

    if (object instanceof Signature signature)
      return Arrays.equals(signature.toByteArray(), toByteArray());

    return false;
  }
}
