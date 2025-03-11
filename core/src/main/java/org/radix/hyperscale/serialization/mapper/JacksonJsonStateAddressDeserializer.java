package org.radix.hyperscale.serialization.mapper;

import java.io.IOException;

import org.radix.hyperscale.ledger.StateAddress;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;

/**
 * Deserializer for translation from JSON encoded {@code StateAddress} data
 * to a {@code StateAddress} object.
 */
public class JacksonJsonStateAddressDeserializer extends StdDeserializer<StateAddress> 
{
	private static final long serialVersionUID = 1611764022697297622L;

	JacksonJsonStateAddressDeserializer() 
	{
		this(null);
	}

	JacksonJsonStateAddressDeserializer(Class<StateAddress> t) 
	{
		super(t);
	}

	@Override
	public StateAddress deserialize(JsonParser p, DeserializationContext ctxt) throws IOException 
	{
		String value = p.getValueAsString();
		if (!value.startsWith(JacksonCodecConstants.STATE_ADDRESS_STR_VALUE))
			throw new InvalidFormatException(p, "Expecting StateAddress ", value, StateAddress.class);
		
		return StateAddress.from(value, JacksonCodecConstants.STR_VALUE_LEN);
	}
}
