package org.radix.hyperscale.database.vamos;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;
import org.eclipse.collections.api.factory.Maps;
import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;
import org.radix.hyperscale.utils.Numbers;

class IndexNode {
  private static final Logger vamosLog = Logging.getLogger("vamos");

  /* Thread local ByteBuffer for serializing to and from byte arrays. Set to the max IndexNode size of 64kb*/
  private static final ThreadLocal<ByteBuffer> byteBuffer =
      ThreadLocal.withInitial(() -> ByteBuffer.allocate(65535));

  private final IndexNodeID id;
  private final short capacity;
  private final short length;
  private final Map<InternalKey, IndexItem> items;

  private transient long modifiedAt;
  private transient int modifications;

  IndexNode(final IndexNodeID id, final int length) {
    Objects.requireNonNull(id, "Index node ID is null");
    Numbers.lessThan(length, 1024, "Max index node length is less than 1kb");
    Numbers.greaterThan(length, 65535, "Max index node length is greater than 64kb");

    this.id = id;
    this.length = (short) length;
    this.capacity = (short) (this.length / IndexItem.BYTES);
    this.items = Maps.mutable.ofInitialCapacity(16);

    this.modifications = 0;
    this.modifiedAt = -1;
  }

  IndexNode(final ByteBuffer buffer) throws IOException {
    Objects.requireNonNull(buffer, "Index node constructor buffer is null");

    this.id = IndexNodeID.from(buffer.getInt());
    this.length = buffer.getShort();
    this.capacity = buffer.getShort();

    short numItems = buffer.getShort();
    int capacity = Math.min(numItems + 16, this.capacity);
    this.items = Maps.mutable.ofInitialCapacity(capacity);
    for (short i = 0; i < numItems; i++) {
      IndexItem indexNodeItem = new IndexItem(buffer);
      this.items.put(indexNodeItem.getKey(), indexNodeItem);
    }

    this.modifications = 0;
    this.modifiedAt = -1;
  }

  public IndexNodeID getID() {
    return this.id;
  }

  public int length() {
    return this.length;
  }

  public int capacity() {
    return this.capacity;
  }

  int modifications() {
    return this.modifications;
  }

  long modifiedAt() {
    return this.modifiedAt;
  }

  private void modified() {
    synchronized (this) {
      this.modifications++;
      if (this.modifiedAt == -1) this.modifiedAt = System.currentTimeMillis();
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + this.id.hashCode();
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;

    if (obj == null) return false;

    if (getClass() != obj.getClass()) return false;

    IndexNode other = (IndexNode) obj;
    if (!this.id.equals(other.id)) return false;

    return true;
  }

  public int size() {
    synchronized (this) {
      return this.items.size();
    }
  }

  IndexItem get(InternalKey key) {
    synchronized (this) {
      return this.items.get(key);
    }
  }

  IndexItem getOrDefault(InternalKey key, IndexItem _default) {
    synchronized (this) {
      return this.items.getOrDefault(key, _default);
    }
  }

  IndexItem delete(InternalKey key) {
    synchronized (this) {
      IndexItem removed = this.items.remove(key);
      if (removed != null) modified();

      return removed;
    }
  }

  IndexItem put(final IndexItem item) throws IOException {
    synchronized (this) {
      if (this.items.size() == this.capacity) throw new IndexNodeCapacityException(this);
      else if (this.items.size() == ((this.capacity / 4) * 3))
        vamosLog.warn(
            "IndexNodeID " + this.id.value() + " is at 75% of " + this.capacity + " capacity");
      else if (this.items.size() == ((this.capacity / 10) * 9))
        vamosLog.warn(
            "IndexNodeID " + this.id.value() + " is at 90% of " + this.capacity + " capacity");

      modified();

      // Update the existing IndexItem position rather than "mutating" the map and
      // causing new Entry objects to be created
      final IndexItem existingItem = this.items.get(item.getKey());
      if (existingItem == null) {
        this.items.put(item.getKey(), item);
        return item;
      } else {
        existingItem.update(item);
        return existingItem;
      }
    }
  }

  void write(final ByteBuffer output) throws IOException {
    write(output, true);
  }

  private void write(final ByteBuffer output, final boolean padding) throws IOException {
    final int startPos = output.position();

    output.putInt(this.id.value());
    output.putShort(this.length);
    output.putShort(this.capacity);
    synchronized (this) {
      output.putShort((short) this.items.size());
      for (IndexItem indexNodeItem : this.items.values()) indexNodeItem.write(output);
    }

    if (padding) output.position(startPos + length());
  }

  void flushed() {
    synchronized (this) {
      this.modifications = 0;
      this.modifiedAt = -1;
    }
  }

  byte[] toByteArray() throws IOException {
    final ByteBuffer buffer = byteBuffer.get();
    buffer.clear();
    write(buffer, false);

    buffer.flip();
    final byte[] bytes = new byte[buffer.limit()];
    buffer.get(bytes);
    return bytes;
  }

  @Override
  public String toString() {
    return "[id="
        + this.id.value()
        + " size="
        + this.length
        + " capacity="
        + this.capacity
        + " items="
        + this.items.size()
        + "]";
  }
}
