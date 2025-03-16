package org.radix.hyperscale.database.vamos;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import org.radix.hyperscale.utils.Numbers;

abstract class ExtensionObject {
  static final int BYTES = InternalKey.BYTES + Long.BYTES;

  private final long extPosition;
  private final InternalKey key;

  ExtensionObject(InternalKey key, long extPosition) {
    Objects.requireNonNull(key, "Hash key is null");
    Numbers.isNegative(extPosition, "Extension position is negative");

    this.key = key;
    this.extPosition = extPosition;
  }

  InternalKey getKey() {
    return this.key;
  }

  long getExtPosition() {
    return this.extPosition;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + this.key.hashCode();
    result = (int) (prime * result + this.extPosition);
    return result;
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj == null) return false;

    if (this == obj) return true;

    if (obj instanceof ExtensionObject other) {
      if (this.key.equals(other.key) == false) return false;

      if (this.extPosition != other.extPosition) return false;

      return true;
    }

    return false;
  }

  @Override
  public String toString() {
    return "[extPosition=" + this.extPosition + ", key=" + this.key + "]";
  }

  abstract int size();

  abstract void write(final ByteBuffer output);

  abstract byte[] toByteArray() throws IOException;
}
