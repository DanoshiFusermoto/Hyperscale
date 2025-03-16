package org.radix.hyperscale.serialization.mapper;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import java.io.IOException;
import org.radix.hyperscale.ledger.ShardGroupID;
import org.radix.hyperscale.utils.Ints;

public class JacksonCborShardGroupIDDeserializer extends StdDeserializer<ShardGroupID> {
  private static final long serialVersionUID = 6716766456336170593L;

  JacksonCborShardGroupIDDeserializer() {
    this(null);
  }

  JacksonCborShardGroupIDDeserializer(Class<ShardGroupID> t) {
    super(t);
  }

  @Override
  public ShardGroupID deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
    byte[] bytes = p.getBinaryValue();
    if (bytes[0] != JacksonCodecConstants.SHARD_GROUP_ID_VALUE)
      throw new InvalidFormatException(
          p,
          "Expecting " + JacksonCodecConstants.SHARD_GROUP_ID_VALUE,
          bytes[0],
          this.handledType());

    int value = Ints.fromByteArray(bytes, 1);
    return ShardGroupID.from(value);
  }
}
