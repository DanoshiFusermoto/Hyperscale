package org.radix.hyperscale.serialization.mapper;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.ledger.StateAddress;

public class JacksonCborStateAddressSerializer extends StdSerializer<StateAddress> {
  private static final ThreadLocal<ByteBuffer> byteBuffer =
      ThreadLocal.withInitial(() -> ByteBuffer.allocate(Hash.BYTES + Hash.BYTES));

  private static final long serialVersionUID = 7402249585696303926L;

  JacksonCborStateAddressSerializer() {
    this(null);
  }

  JacksonCborStateAddressSerializer(Class<StateAddress> t) {
    super(t);
  }

  @Override
  public void serialize(StateAddress value, JsonGenerator jgen, SerializerProvider provider)
      throws IOException {
    final ByteBuffer buffer = byteBuffer.get();
    buffer.clear();
    buffer.put(JacksonCodecConstants.STATE_ADDRESS_VALUE);
    value.write(buffer);
    jgen.writeBinary(buffer.array(), 0, buffer.position());
  }
}
