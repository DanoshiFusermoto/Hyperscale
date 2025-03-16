package org.radix.hyperscale.serialization.mapper;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import java.io.IOException;
import org.radix.hyperscale.crypto.Identity;

/**
 * Deserializer for translation from JSON encoded {@code Identity} data to a {@code Identity}
 * object.
 */
public class JacksonCborIdentityDeserializer extends StdDeserializer<Identity> {
  private static final long serialVersionUID = -1555027261207368739L;

  JacksonCborIdentityDeserializer() {
    this(null);
  }

  JacksonCborIdentityDeserializer(Class<Identity> t) {
    super(t);
  }

  @Override
  public Identity deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
    byte[] bytes = p.getBinaryValue();
    if (bytes == null || bytes.length == 0 || bytes[0] != JacksonCodecConstants.IDENTITY_VALUE)
      throw new InvalidFormatException(
          p, "Expecting " + JacksonCodecConstants.IDENTITY_VALUE, bytes[0], this.handledType());

    try {
      return Identity.from(bytes, 1);
    } catch (Exception cex) {
      throw new IOException(cex);
    }
  }
}
