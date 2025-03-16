package org.radix.hyperscale.crypto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.DsonOutput.Output;
import org.radix.hyperscale.serialization.Serializable;
import org.radix.hyperscale.serialization.Serialization;
import org.radix.hyperscale.serialization.SerializationException;
import org.radix.hyperscale.serialization.SerializerId2;

@SerializerId2("crypto.signed_object")
public final class SignedObject<K extends PublicKey, T> extends Serializable {
  @JsonProperty("object")
  @DsonOutput(Output.ALL)
  private T object;

  @JsonProperty("owner")
  @DsonOutput(Output.ALL)
  private K owner;

  @JsonProperty("signature")
  @DsonOutput(value = {Output.API, Output.WIRE, Output.PERSIST})
  private Signature signature;

  @SuppressWarnings("unused")
  private SignedObject() {
    // For serializer
  }

  public SignedObject(final T object, final K owner) {
    this.object = Objects.requireNonNull(object, "Object is null");

    // TODO check object is serializable

    this.owner = Objects.requireNonNull(owner, "Owner is null");
  }

  public SignedObject(final T object, final K owner, final Signature signature)
      throws CryptoException {
    this.object = Objects.requireNonNull(object, "Object is null");
    this.owner = Objects.requireNonNull(owner, "Owner is null");
    this.signature = Objects.requireNonNull(signature, "Signature is null");

    // TODO check object is serializable

    try {
      if (verify(owner) == false) throw new CryptoException("Signed object invalid / not verified");
    } catch (SerializationException ex) {
      throw new CryptoException("Signed object invalid / not verified", ex);
    }
  }

  public final T getObject() {
    return this.object;
  }

  public final K getOwner() {
    return this.owner;
  }

  public final synchronized void sign(final KeyPair<?, ?, ?> key)
      throws CryptoException, SerializationException {
    Objects.requireNonNull(key, "Signing key is null");

    if (key.getPublicKey().equals(getOwner()) == false)
      throw new CryptoException(
          "Attempting to sign wrapped object with key that doesn't match owner");

    Hash objectHash;
    if (this.object instanceof Hashable hashable) objectHash = hashable.getHash();
    else objectHash = Hash.hash(Serialization.getInstance().toDson(this.object, Output.HASH));

    this.signature = key.getPrivateKey().sign(objectHash);
  }

  public final synchronized boolean verify(final K key)
      throws CryptoException, SerializationException {
    Objects.requireNonNull(key, "Verification key is null");

    if (this.signature == null) throw new CryptoException("Signature is not present");

    if (getOwner() == null) throw new CryptoException("Owner is not present");

    if (key.equals(getOwner()) == false) throw new CryptoException("Owner does not match key");

    Hash objectHash;
    if (this.object instanceof Hashable hashable) objectHash = hashable.getHash();
    else objectHash = Hash.hash(Serialization.getInstance().toDson(this.object, Output.HASH));

    return key.verify(objectHash, this.signature);
  }

  boolean requiresSignature() {
    return true;
  }

  public final synchronized Signature getSignature() {
    return this.signature;
  }

  public String toString() {
    return super.toString() + " " + this.owner;
  }
}
