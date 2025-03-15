package org.radix.hyperscale.serialization.mapper;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.io.IOException;
import org.radix.hyperscale.ledger.ShardGroupID;

/** Serializer for conversion from {@code ShardGroupID} data to the appropriate JSON encoding. */
class JacksonJsonShardGroupIDSerializer extends StdSerializer<ShardGroupID> {
  private static final long serialVersionUID = 1611764022697297622L;

  JacksonJsonShardGroupIDSerializer() {
    this(null);
  }

  JacksonJsonShardGroupIDSerializer(Class<ShardGroupID> t) {
    super(t);
  }

  @Override
  public void serialize(ShardGroupID value, JsonGenerator jgen, SerializerProvider provider)
      throws IOException {
    jgen.writeString(JacksonCodecConstants.SHARD_GROUP_ID_STR_VALUE + value.intValue());
  }
}
