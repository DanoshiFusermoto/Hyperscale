package org.radix.hyperscale.serialization.mapper;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.radix.hyperscale.ledger.ShardGroupID;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

public class JacksonCborShardGroupIDSerializer extends StdSerializer<ShardGroupID> 
{
	private static final ThreadLocal<ByteBuffer> byteBuffer = ThreadLocal.withInitial(() -> ByteBuffer.allocate(Byte.BYTES + Integer.BYTES));

	private static final long serialVersionUID = 6716766456336170593L;

	JacksonCborShardGroupIDSerializer() 
	{
		this(null);
	}

	JacksonCborShardGroupIDSerializer(Class<ShardGroupID> t) 
	{
		super(t);
	}

	@Override
	public void serialize(ShardGroupID value, JsonGenerator jgen, SerializerProvider provider) throws IOException 
	{
		final ByteBuffer buffer = byteBuffer.get();
		buffer.clear();
		buffer.put(JacksonCodecConstants.SHARD_GROUP_ID_VALUE);
		buffer.putInt(value.intValue());
		jgen.writeBinary(buffer.array());
	}
}
