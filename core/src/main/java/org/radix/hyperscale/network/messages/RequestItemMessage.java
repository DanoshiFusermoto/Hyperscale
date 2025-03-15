package org.radix.hyperscale.network.messages;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.radix.hyperscale.common.Primitive;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.DsonOutput.Output;
import org.radix.hyperscale.serialization.Serialization;
import org.radix.hyperscale.serialization.SerializerId2;

@SerializerId2("gossip.item.request")
public final class RequestItemMessage extends Message {
  @JsonProperty("type")
  @DsonOutput(Output.ALL)
  private String type;

  @JsonProperty("item")
  @DsonOutput(Output.ALL)
  private Hash item;

  @JsonProperty("references")
  @JsonInclude(Include.NON_NULL)
  @DsonOutput(Output.ALL)
  private Map<String, Object> references;

  RequestItemMessage() {
    // Serializer only
  }

  public RequestItemMessage(final Class<? extends Primitive> type, final Hash item) {
    super();

    Objects.requireNonNull(type, "Type is null");
    Objects.requireNonNull(item, "Item is null");

    this.type = Serialization.getInstance().getIdForClass(type);
    this.item = item;
  }

  public RequestItemMessage(
      final Class<? extends Primitive> type,
      final Hash item,
      final Map<String, Object> references) {
    super();

    Objects.requireNonNull(type, "Type is null");
    Objects.requireNonNull(item, "Item is null");
    Objects.requireNonNull(references, "References is null");

    this.item = item;
    this.type = Serialization.getInstance().getIdForClass(type);
    this.references =
        references.isEmpty() ? Collections.emptyMap() : new HashMap<String, Object>(references);
  }

  public String getType() {
    return this.type;
  }

  public Hash getItem() {
    return this.item;
  }

  public Map<String, Object> getReferences() {
    return Collections.unmodifiableMap(this.references);
  }
}
