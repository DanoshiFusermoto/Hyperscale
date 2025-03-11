package org.radix.hyperscale.serialization.mapper;

import java.io.IOException;

import org.radix.hyperscale.crypto.Identity;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;

/**
 * Deserializer for translation from JSON encoded {@code Identity} data
 * to a {@code Identity} object.
 */
public class JacksonJsonIdentityDeserializer extends StdDeserializer<Identity> 
{
	private static final long serialVersionUID = -1555027261207368739L;

	JacksonJsonIdentityDeserializer() 
	{
		this(null);
	}

	JacksonJsonIdentityDeserializer(Class<Identity> t) 
	{
		super(t);
	}

	@Override
	public Identity deserialize(JsonParser p, DeserializationContext ctxt) throws IOException 
	{
		String value = p.getValueAsString();
		if (!value.startsWith(JacksonCodecConstants.IDENTITY_STR_VALUE))
			throw new InvalidFormatException(p, "Expecting Identity", value, Identity.class);
		
		try
		{
			return Identity.from(value, JacksonCodecConstants.STR_VALUE_LEN);
		}
		catch (Exception cex)
		{
			throw new IOException(cex);
		}
	}
}
