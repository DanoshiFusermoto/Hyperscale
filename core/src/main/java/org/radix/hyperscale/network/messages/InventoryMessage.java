package org.radix.hyperscale.network.messages;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import org.eclipse.collections.api.multimap.Multimap;
import org.eclipse.collections.api.multimap.MutableMultimap;
import org.eclipse.collections.impl.factory.Multimaps;
import org.radix.hyperscale.Constants;
import org.radix.hyperscale.common.Primitive;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.network.InventoryItem;
import org.radix.hyperscale.network.TransportParameters;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.DsonOutput.Output;
import org.radix.hyperscale.serialization.Serialization;
import org.radix.hyperscale.serialization.SerializerId2;
import org.radix.hyperscale.utils.Numbers;

@SerializerId2("gossip.inventory")
@TransportParameters(cache = true)
public final class InventoryMessage extends Message {
  @JsonProperty("inventory")
  @DsonOutput(Output.ALL)
  private HashMap<String, HashSet<Hash>> inventory;

  private transient volatile List<InventoryItem> inventoryItems = null;

  public InventoryMessage() {
    super();
  }

  public InventoryMessage(final Collection<Hash> inventory, final Class<? extends Primitive> type) {
    this(inventory, 0, inventory.size(), type);
  }

  public InventoryMessage(
      final Collection<Hash> inventory, int start, int end, final Class<? extends Primitive> type) {
    this(
        inventory,
        start,
        end,
        Serialization.getInstance().getIdForClass(Objects.requireNonNull(type, "Type is null")));
  }

  public InventoryMessage(final Collection<Hash> inventory, int start, int end, final String type) {
    this();

    Objects.requireNonNull(type, "Type is null");
    Numbers.isZero(type.length(), "Type is empty");

    Objects.requireNonNull(inventory, "Inventory is null");
    if (inventory.isEmpty()) throw new IllegalArgumentException("Inventory is empty");

    Numbers.isNegative(end - start, "Delta is negative");
    Numbers.greaterThan(
        end - start,
        Constants.MAX_BROADCAST_INVENTORY_ITEMS,
        "Items is greater than allowed max of " + Constants.MAX_BROADCAST_INVENTORY_ITEMS);

    this.inventory = new HashMap<String, HashSet<Hash>>();
    int i = 0;
    for (Hash item : inventory) {
      if (i >= start)
        this.inventory.computeIfAbsent(type, t -> new HashSet<>(end - start)).add(item);

      i++;
      if (i == end) break;
    }
  }

  public boolean add(final Class<? extends Primitive> type, final Hash item) {
    Objects.requireNonNull(type, "Type is null");
    Objects.requireNonNull(item, "Item is null");
    Hash.notZero(item, "Item has is ZERO");

    if (this.inventory == null) this.inventory = new HashMap<String, HashSet<Hash>>();

    Numbers.greaterThan(
        this.inventory.size(),
        Constants.MAX_BROADCAST_INVENTORY_ITEMS,
        "Broadcast items greater than allowed max of " + Constants.MAX_BROADCAST_INVENTORY_ITEMS);

    return this.inventory
        .computeIfAbsent(Serialization.getInstance().getIdForClass(type), t -> new HashSet<>())
        .add(item);
  }

  public List<InventoryItem> asInventory() {
    synchronized (this) {
      if (this.inventoryItems == null) {
        final List<InventoryItem> inventoryItems =
            new ArrayList<InventoryItem>(this.inventory.size());
        for (final Entry<String, HashSet<Hash>> items : this.inventory.entrySet()) {
          for (final Hash item : items.getValue())
            inventoryItems.add(new InventoryItem(items.getKey(), item));
        }

        this.inventoryItems = Collections.unmodifiableList(inventoryItems);
      }

      return this.inventoryItems;
    }
  }

  public Multimap<Class<? extends Primitive>, Hash> getTyped() {
    final MutableMultimap<Class<? extends Primitive>, Hash> typed = Multimaps.mutable.list.empty();
    if (this.inventory != null && this.inventory.isEmpty() == false) {
      for (final Entry<String, HashSet<Hash>> items : this.inventory.entrySet()) {
        final Class<?> clazz = Serialization.getInstance().getClassForId(items.getKey());
        for (final Hash item : items.getValue()) typed.put(clazz.asSubclass(Primitive.class), item);
      }
    }

    return typed;
  }

  @Override
  public boolean isUrgent() {
    for (final String type : this.inventory.keySet()) {
      final TransportParameters transportParameters =
          Serialization.getInstance().getClassForId(type).getAnnotation(TransportParameters.class);
      if (transportParameters == null || transportParameters.urgent() == false) continue;

      return true;
    }

    return super.isUrgent();
  }
}
