package org.radix.hyperscale.common;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.crypto.Hashable;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.DsonOutput.Output;
import org.radix.hyperscale.serialization.Serializable;
import org.radix.hyperscale.serialization.Serialization;

public abstract class BasicObject extends Serializable
    implements Cloneable, Comparable<Object>, Hashable {
  private transient volatile Hash hash = Hash.ZERO;

  protected BasicObject() {
    super();
  }

  protected BasicObject(final BasicObject other) {
    super();

    if (this.hash != null && this.hash.equals(Hash.ZERO) == false) this.hash = other.hash;
  }

  @Override
  protected Object clone() throws CloneNotSupportedException {
    BasicObject object = (BasicObject) super.clone();
    object.hash = null;
    return object;
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj == null) return false;

    if (obj == this) return true;

    if (obj instanceof BasicObject other) {
      if (getHash().equals(other.getHash())) return true;
    }

    return super.equals(obj);
  }

  @Override
  public int hashCode() {
    return getHash().hashCode();
  }

  // HASHABLE //
  @Override
  @JsonProperty("hash")
  @DsonOutput(value = {Output.API})
  public final synchronized Hash getHash() {
    try {
      if (this.hash == null || this.hash.equals(Hash.ZERO)) this.hash = computeHash();

      return this.hash;
    } catch (Exception e) {
      throw new RuntimeException("Error generating hash: " + e, e);
    }
  }

  protected synchronized Hash computeHash() {
    try {
      return Serialization.getInstance().toHash(this);
    } catch (Exception e) {
      throw new RuntimeException("Error generating hash: " + e, e);
    }
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + " hash=" + getHash().toString();
  }

  @Override
  public int compareTo(final Object obj) {
    Objects.requireNonNull(obj, "Object to compare to is null");

    if (obj instanceof BasicObject other) return getHash().compareTo(other.getHash());

    return 0;
  }
}
