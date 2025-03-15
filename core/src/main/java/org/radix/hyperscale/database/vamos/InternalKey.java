package org.radix.hyperscale.database.vamos;

import com.sleepycat.je.DatabaseEntry;
import org.radix.hyperscale.crypto.SipHash24;

final class InternalKey {
  private static final InternalKey[][] cache = new InternalKey[8192][16];

  private static final InternalKey cache(final int databaseID, final byte[] key) {
    final long hash = hash(databaseID, key);
    return cache(databaseID, hash);
  }

  private static final InternalKey cache(final int databaseID, final long hash) {
    final int bucketIndex = (int) Math.abs((hash % cache.length));
    final InternalKey[] bucket = cache[bucketIndex];
    synchronized (bucket) {
      final int itemIndex = (int) Math.abs((hash % bucket.length));
      InternalKey cacheKey = bucket[itemIndex];
      if (cacheKey != null) {
        if (cacheKey.databaseID == databaseID && cacheKey.value == hash) return cacheKey;
      }

      cacheKey = new InternalKey(databaseID, hash);
      bucket[itemIndex] = cacheKey;
      return cacheKey;
    }
  }

  public static final InternalKey from(final Database database, final DatabaseEntry key) {
    return cache(database.getID(), key.getData());
  }

  public static final InternalKey from(final int databaseID, final byte[] key) {
    return cache(databaseID, key);
  }

  static final InternalKey from(final int databaseID, final long value) {
    return cache(databaseID, value);
  }

  private static long hash(final int databaseID, final byte[] key) {
    return SipHash24.hash24(databaseID, 0, key);
  }

  static final int BYTES = Integer.BYTES + Long.BYTES;

  private final int databaseID;
  private final long value;
  private final int hashCode;

  private InternalKey(final int databaseID, final long value) {
    this.databaseID = databaseID;
    this.value = value;

    final int prime = 31;
    int result = 1;
    result = prime * result + this.databaseID;
    result = prime * result + (int) (this.value ^ (this.value >>> 32));
    this.hashCode = result;
  }

  long value() {
    return this.value;
  }

  int getDatabaseID() {
    return this.databaseID;
  }

  @Override
  public int hashCode() {
    return this.hashCode;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) return true;

    if (obj == null) return false;

    if (obj instanceof InternalKey other) {
      if (this.databaseID != other.databaseID) return false;

      if (this.value != other.value) return false;

      return true;
    }

    return false;
  }

  @Override
  public String toString() {
    return "[dbid=" + this.databaseID + " value=" + this.value + "]";
  }
}
