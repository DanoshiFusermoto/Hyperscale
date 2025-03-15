package org.radix.hyperscale.serialization.mapper;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import java.io.IOException;
import org.radix.hyperscale.ledger.StateAddress;

public class JacksonCborStateAddressDeserializer extends StdDeserializer<StateAddress> {
  private static final long serialVersionUID = -2364801461720049831L;

  JacksonCborStateAddressDeserializer() {
    this(null);
  }

  JacksonCborStateAddressDeserializer(Class<StateAddress> t) {
    super(t);
  }

  @Override
  public StateAddress deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
    byte[] bytes = p.getBinaryValue();
    if (bytes[0] != JacksonCodecConstants.STATE_ADDRESS_VALUE)
      throw new InvalidFormatException(
          p,
          "Expecting " + JacksonCodecConstants.STATE_ADDRESS_VALUE,
          bytes[0],
          this.handledType());

    return StateAddress.from(bytes, 1);
  }
}
