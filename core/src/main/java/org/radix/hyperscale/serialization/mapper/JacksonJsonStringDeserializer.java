package org.radix.hyperscale.serialization.mapper;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import java.io.IOException;

/**
 * Deserializer for translation from JSON encoded {@code String} data to a {@code String} object.
 */
class JacksonJsonStringDeserializer extends StdDeserializer<String> {
  private static final long serialVersionUID = -2472482347700365657L;

  JacksonJsonStringDeserializer() {
    this(null);
  }

  JacksonJsonStringDeserializer(Class<String> t) {
    super(t);
  }

  @Override
  public String deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
    return p.getValueAsString();
  }
}
