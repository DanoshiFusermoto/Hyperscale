package org.radix.hyperscale.serialization.mapper;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.io.IOException;
import org.radix.hyperscale.crypto.Identity;

/** Serializer for conversion from {@code Identity} data to the appropriate JSON encoding. */
class JacksonJsonIdentitySerializer extends StdSerializer<Identity> {
  private static final long serialVersionUID = -1555027261207368739L;

  JacksonJsonIdentitySerializer() {
    this(null);
  }

  JacksonJsonIdentitySerializer(Class<Identity> t) {
    super(t);
  }

  @Override
  public void serialize(Identity value, JsonGenerator jgen, SerializerProvider provider)
      throws IOException {
    jgen.writeString(JacksonCodecConstants.IDENTITY_STR_VALUE + value.toString());
  }
}
