package org.radix.hyperscale.serialization.mapper;

import java.io.IOException;

import org.radix.hyperscale.ledger.ShardID;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;

public class JacksonCborShardIDDeserializer extends StdDeserializer<ShardID> 
{
	private static final long serialVersionUID = -1555027261207368739L;

	JacksonCborShardIDDeserializer() 
	{
		this(null);
	}

	JacksonCborShardIDDeserializer(Class<ShardID> t) 
	{
		super(t);
	}

	@Override
	public ShardID deserialize(JsonParser p, DeserializationContext ctxt) throws IOException 
	{
		byte[] bytes = p.getBinaryValue();
		if (bytes == null || bytes.length == 0 || bytes[0] != JacksonCodecConstants.SHARD_ID_VALUE)
			throw new InvalidFormatException(p, "Expecting " + JacksonCodecConstants.SHARD_ID_VALUE, bytes[0], this.handledType());
		
		return ShardID.from(bytes, 1);
	}
}