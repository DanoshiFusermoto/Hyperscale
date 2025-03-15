package org.radix.hyperscale.serialization.mapper;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import java.io.IOException;
import org.radix.hyperscale.ledger.ShardGroupID;

/**
 * Deserializer for translation from JSON encoded {@code ShardGroupID} data to a {@code
 * ShardGroupID} object.
 */
public class JacksonJsonShardGroupIDDeserializer extends StdDeserializer<ShardGroupID> {
  private static final long serialVersionUID = 1611764022697297622L;

  JacksonJsonShardGroupIDDeserializer() {
    this(null);
  }

  JacksonJsonShardGroupIDDeserializer(Class<ShardGroupID> t) {
    super(t);
  }

  @Override
  public ShardGroupID deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
    String value = p.getValueAsString();
    if (!value.startsWith(JacksonCodecConstants.SHARD_GROUP_ID_STR_VALUE))
      throw new InvalidFormatException(p, "Expecting Shard Group ID", value, ShardGroupID.class);

    return ShardGroupID.from(
        Integer.parseInt(value.substring(JacksonCodecConstants.STR_VALUE_LEN)));
  }
}
