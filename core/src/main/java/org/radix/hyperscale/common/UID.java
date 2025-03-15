package org.radix.hyperscale.common;

public final class UID {
  private final long uid;

  public UID(long uid) {
    this.uid = uid;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (int) (uid ^ (uid >>> 32));
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;

    if (obj == null) return false;

    if (getClass() != obj.getClass()) return false;

    UID other = (UID) obj;
    if (uid != other.uid) return false;

    return true;
  }

  @Override
  public String toString() {
    return "UID [uid=" + uid + "]";
  }
}
