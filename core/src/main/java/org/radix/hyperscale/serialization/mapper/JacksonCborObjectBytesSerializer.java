package org.radix.hyperscale.serialization.mapper;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.io.IOException;
import java.util.function.Function;

public class JacksonCborObjectBytesSerializer<T> extends StdSerializer<T> {
  /** */
  private static final long serialVersionUID = 6505463689941696424L;

  private final byte prefix;
  private final Function<T, byte[]> toByteArrayMapper;

  JacksonCborObjectBytesSerializer(Class<T> t, byte prefix, Function<T, byte[]> toByteArrayMapper) {
    super(t);
    this.prefix = prefix;
    this.toByteArrayMapper = toByteArrayMapper;
  }

  @Override
  public void serialize(T value, JsonGenerator jgen, SerializerProvider provider)
      throws IOException {
    byte[] objectBytes = this.toByteArrayMapper.apply(value);

    if (jgen instanceof RadixCBORGenerator rgen) {
      rgen.writeBinaryWithPrefix(this.prefix, objectBytes, 0, objectBytes.length);
    } else {
      byte[] bytes = new byte[1 + objectBytes.length];
      bytes[0] = this.prefix;
      System.arraycopy(objectBytes, 0, bytes, 1, objectBytes.length);
      jgen.writeBinary(bytes);
    }
  }
}
