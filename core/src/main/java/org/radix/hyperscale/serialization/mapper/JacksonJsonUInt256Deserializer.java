package org.radix.hyperscale.serialization.mapper;

import java.io.IOException;

import org.radix.hyperscale.utils.UInt256;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;

/**
 * Deserializer for translation from JSON encoded {@code UInt256} data
 * to a {@code UInt256} object.
 */
public class JacksonJsonUInt256Deserializer extends StdDeserializer<UInt256> 
{
	private static final long serialVersionUID = 8190847216433553689L;

	JacksonJsonUInt256Deserializer() 
	{
		this(null);
	}

	JacksonJsonUInt256Deserializer(Class<UInt256> t) 
	{
		super(t);
	}

	@Override
	public UInt256 deserialize(JsonParser p, DeserializationContext ctxt) throws IOException 
	{
		String value = p.getValueAsString();
		if (!value.startsWith(JacksonCodecConstants.U256_STR_VALUE))
			throw new InvalidFormatException(p, "Expecting UInt256", value, UInt256.class);
		
		return UInt256.from(value, JacksonCodecConstants.STR_VALUE_LEN);
	}
}
