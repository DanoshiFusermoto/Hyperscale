package org.radix.hyperscale.serialization.mapper;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import java.io.IOException;
import org.radix.hyperscale.utils.UInt256;

public class JacksonCborUInt256Deserializer extends StdDeserializer<UInt256> {
  private static final long serialVersionUID = -1555027261207368739L;

  JacksonCborUInt256Deserializer() {
    this(null);
  }

  JacksonCborUInt256Deserializer(Class<UInt256> t) {
    super(t);
  }

  @Override
  public UInt256 deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
    byte[] bytes = p.getBinaryValue();
    if (bytes == null || bytes.length == 0 || bytes[0] != JacksonCodecConstants.U20_VALUE)
      throw new InvalidFormatException(
          p, "Expecting " + JacksonCodecConstants.U20_VALUE, bytes[0], this.handledType());

    return UInt256.from(bytes, 1);
  }
}
