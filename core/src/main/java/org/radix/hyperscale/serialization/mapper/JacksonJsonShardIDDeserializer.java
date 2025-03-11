package org.radix.hyperscale.serialization.mapper;

import java.io.IOException;

import org.radix.hyperscale.ledger.ShardID;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;

/**
 * Deserializer for translation from JSON encoded {@code UInt256} data
 * to a {@code UInt256} object.
 */
public class JacksonJsonShardIDDeserializer extends StdDeserializer<ShardID> 
{
	private static final long serialVersionUID = -7087668323524790276L;

	JacksonJsonShardIDDeserializer() 
	{
		this(null);
	}

	JacksonJsonShardIDDeserializer(Class<ShardID> t) 
	{
		super(t);
	}

	@Override
	public ShardID deserialize(JsonParser p, DeserializationContext ctxt) throws IOException 
	{
		String value = p.getValueAsString();
		if (!value.startsWith(JacksonCodecConstants.SHARD_ID_STR_VALUE))
			throw new InvalidFormatException(p, "Expecting ShardID", value, ShardID.class);
		
		return ShardID.from(value, JacksonCodecConstants.STR_VALUE_LEN);
	}
}
