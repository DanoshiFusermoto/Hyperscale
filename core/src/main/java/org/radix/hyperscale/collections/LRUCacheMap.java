package org.radix.hyperscale.collections;

import java.util.Arrays;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Function;

public class LRUCacheMap<K, V> {
  private static final int DEFAULT_COLLISION_SCALAR = 4;

  private final int capacity;
  private final Node<K, V>[] table;
  private final Node<K, V> head;
  private final Node<K, V> tail;
  private final Object mutex;

  private volatile int size;

  private volatile Node<K, V> nodePool;

  public LRUCacheMap(int capacity) {
    this(capacity, DEFAULT_COLLISION_SCALAR);
  }

  @SuppressWarnings("unchecked")
  public LRUCacheMap(int capacity, int scalar) {
    if (capacity <= 0) throw new IllegalArgumentException("Capacity must be greater than zero");

    this.capacity = capacity;
    this.size = 0;
    this.table = new Node[capacity * scalar]; // scalar * capacity to reduce collisions
    this.head = new Node<>(null, null, -1); // Dummy head
    this.tail = new Node<>(null, null, -1); // Dummy tail
    this.head.next = this.tail;
    this.tail.prev = this.head;
    this.mutex = new Object();
  }

  private int bucketIndex(final K key) {
    return (murmur3Hash(key.hashCode()) & Integer.MAX_VALUE) % this.table.length;
  }

  private int murmur3Hash(final int input) {
    int hash = input;
    hash ^= hash >>> 16;
    hash *= 0x85ebca6b;
    hash ^= hash >>> 13;
    hash *= 0xc2b2ae35;
    hash ^= hash >>> 16;
    return hash;
  }

  private Node<K, V> getNodeFromPool(final K key, final V value, final int bucket) {
    if (this.nodePool == null) return new Node<>(key, value, bucket);

    Node<K, V> node = this.nodePool;
    this.nodePool = this.nodePool.chainNext;
    node.key = key;
    node.value = value;
    node.bucket = bucket;
    node.prev = null;
    node.next = null;
    node.chainNext = null;
    return node;
  }

  private void returnNodeToPool(final Node<K, V> node) {
    node.key = null;
    node.value = null;
    node.bucket = -1;
    node.prev = null;
    node.next = null;
    node.chainNext = this.nodePool;
    this.nodePool = node;
  }

  public V get(final K key) {
    Objects.requireNonNull(key, "Key is null");

    final int bucketIndex = bucketIndex(key);
    synchronized (this.mutex) {
      Node<K, V> node = this.table[bucketIndex];
      while (node != null) {
        if (node.key.equals(key)) {
          moveToHead(node);
          return node.value;
        }

        node = node.chainNext;
      }

      return null;
    }
  }

  public V getOrDefault(final K key, final V defaultValue) {
    final V value = get(key);
    return value != null ? value : defaultValue;
  }

  public V computeIfAbsent(final K key, final Function<? super K, ? extends V> function) {
    Objects.requireNonNull(key, "Key is null");
    Objects.requireNonNull(function, "Function is null");

    synchronized (this.mutex) {
      V value = get(key);
      if (value == null) {
        value = function.apply(key);
        put(key, value);
      }

      return value;
    }
  }

  public V putIfAbsent(final K key, final V value) {
    synchronized (this.mutex) {
      V existingValue = get(key);
      if (existingValue != null) return existingValue;

      put(key, value);
      return null;
    }
  }

  public V put(final K key, final V value) {
    Objects.requireNonNull(key, "Key is null");
    Objects.requireNonNull(value, "Value is null");

    final int bucketIndex = bucketIndex(key);
    synchronized (this.mutex) {
      final Node<K, V> existingNode = findNode(key, bucketIndex);
      if (existingNode != null) {
        V existingValue = existingNode.value;
        existingNode.value = value;
        moveToHead(existingNode);
        return existingValue;
      }

      final Node<K, V> newNode = getNodeFromPool(key, value, bucketIndex);
      newNode.chainNext = this.table[bucketIndex];
      this.table[bucketIndex] = newNode;
      addToHead(newNode);

      if (this.size == this.capacity) {
        final Node<K, V> lru = removeTail();
        if (lru != null) {
          removeFromBucket(lru, lru.bucket);
          returnNodeToPool(lru);
        }
      } else this.size++;

      return null;
    }
  }

  public V remove(final K key) {
    Objects.requireNonNull(key, "Key is null");

    final int bucketIndex = bucketIndex(key);
    synchronized (this.mutex) {
      final Node<K, V> node = findNode(key, bucketIndex);
      final V value;
      if (node != null) {
        value = node.value;

        removeNode(node);
        removeFromBucket(node, bucketIndex);
        returnNodeToPool(node);
        this.size--;
      } else value = null;

      return value;
    }
  }

  public void forEach(final BiConsumer<? super K, ? super V> action) {
    Objects.requireNonNull(action, "Action is null");

    synchronized (this.mutex) {
      Node<K, V> current = this.head.next;
      while (current != this.tail) {
        // Save reference to next node because action might modify the cache
        Node<K, V> next = current.next;
        action.accept(current.key, current.value);
        current = next;
      }
    }
  }

  public void clear() {
    synchronized (this.mutex) {
      Arrays.fill(this.table, null);

      this.head.next = this.tail;
      this.tail.prev = this.head;
      this.size = 0;

      // Return all nodes to the pool
      Node<K, V> current = this.head.next;
      while (current != this.tail) {
        Node<K, V> next = current.next;
        returnNodeToPool(current);
        current = next;
      }
    }
  }

  public int size() {
    synchronized (this.mutex) {
      return this.size;
    }
  }

  private Node<K, V> findNode(final K key, final int bucketIndex) {
    Node<K, V> node = this.table[bucketIndex];
    while (node != null) {
      if (node.key.equals(key)) return node;

      node = node.chainNext;
    }

    return null;
  }

  private void addToHead(final Node<K, V> node) {
    node.next = this.head.next;
    this.head.next.prev = node;
    this.head.next = node;
    node.prev = this.head;
  }

  private void moveToHead(final Node<K, V> node) {
    removeNode(node);
    addToHead(node);
  }

  private void removeNode(final Node<K, V> node) {
    if (node.prev != null) node.prev.next = node.next;

    if (node.next != null) node.next.prev = node.prev;

    node.prev = null;
    node.next = null;
  }

  private Node<K, V> removeTail() {
    if (this.tail.prev == this.head) return null;

    Node<K, V> lru = this.tail.prev;
    removeNode(lru);
    return lru;
  }

  private void removeFromBucket(final Node<K, V> node, final int bucketIndex) {
    Node<K, V> current = this.table[bucketIndex];
    Node<K, V> prev = null;

    while (current != null) {
      if (current == node) {
        if (prev == null) this.table[bucketIndex] = current.chainNext;
        else prev.chainNext = current.chainNext;

        break;
      }

      prev = current;
      current = current.chainNext;
    }
  }

  // Doubly linked list node class
  private static class Node<K, V> {
    K key;
    V value;
    int bucket;

    Node<K, V> prev;
    Node<K, V> next;
    Node<K, V> chainNext; // For hash table chaining

    Node(K key, V value, int bucket) {
      this.key = key;
      this.value = value;
      this.bucket = bucket;
    }
  }
}
