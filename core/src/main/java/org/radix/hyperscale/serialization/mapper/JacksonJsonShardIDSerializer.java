package org.radix.hyperscale.serialization.mapper;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.io.IOException;
import org.radix.hyperscale.ledger.ShardID;

/** Serializer for conversion from {@code UInt256} data to the appropriate JSON encoding. */
class JacksonJsonShardIDSerializer extends StdSerializer<ShardID> {
  private static final long serialVersionUID = 4368521680562074478L;

  JacksonJsonShardIDSerializer() {
    this(null);
  }

  JacksonJsonShardIDSerializer(Class<ShardID> t) {
    super(t);
  }

  @Override
  public void serialize(ShardID value, JsonGenerator jgen, SerializerProvider provider)
      throws IOException {
    jgen.writeString(JacksonCodecConstants.SHARD_ID_STR_VALUE + value.toString());
  }
}
