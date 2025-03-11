package org.radix.hyperscale.serialization.mapper;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.radix.hyperscale.crypto.Identity;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

public class JacksonCborIdentitySerializer extends StdSerializer<Identity> 
{
	private static final ThreadLocal<ByteBuffer> byteBuffer = ThreadLocal.withInitial(() -> ByteBuffer.allocate(1024));

	private static final long serialVersionUID = 7402249585696303926L;

	JacksonCborIdentitySerializer() 
	{
		this(null);
	}

	JacksonCborIdentitySerializer(Class<Identity> t) 
	{
		super(t);
	}

	@Override
	public void serialize(Identity value, JsonGenerator jgen, SerializerProvider provider) throws IOException 
	{
		final ByteBuffer buffer = byteBuffer.get();
		buffer.clear();
		buffer.put(JacksonCodecConstants.IDENTITY_VALUE);
		buffer.put(value.getPrefix());
		buffer.put(value.getKey().toByteArray());
		jgen.writeBinary(buffer.array(), 0, buffer.position());
	}
}
