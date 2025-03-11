package org.radix.hyperscale.serialization.mapper;

import java.io.IOException;

import org.radix.hyperscale.crypto.Hash;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;

public class JacksonCborHashDeserializer extends StdDeserializer<Hash> 
{
	private static final long serialVersionUID = -1555027261207368739L;

	JacksonCborHashDeserializer() 
	{
		this(null);
	}

	JacksonCborHashDeserializer(Class<Hash> t) 
	{
		super(t);
	}

	@Override
	public Hash deserialize(JsonParser p, DeserializationContext ctxt) throws IOException 
	{
		byte[] bytes = p.getBinaryValue();
		if (bytes == null || bytes.length == 0 || bytes[0] != JacksonCodecConstants.HASH_VALUE)
			throw new InvalidFormatException(p, "Expecting " + JacksonCodecConstants.HASH_VALUE, bytes[0], this.handledType());
		
		return Hash.from(bytes, 1);
	}
}