package org.radix.hyperscale.serialization.mapper;

import java.io.IOException;

import org.radix.hyperscale.ledger.StateAddress;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

/**
 * Serializer for conversion from {@code StateAddress} data to the appropriate JSON encoding.
 */
class JacksonJsonStateAddressSerializer extends StdSerializer<StateAddress> 
{
	private static final long serialVersionUID = -7151996856688271083L;

	JacksonJsonStateAddressSerializer() 
	{
		this(null);
	}

	JacksonJsonStateAddressSerializer(Class<StateAddress> t) 
	{
		super(t);
	}

	@Override
	public void serialize(StateAddress value, JsonGenerator jgen, SerializerProvider provider) throws IOException 
	{
		jgen.writeString(JacksonCodecConstants.STATE_ADDRESS_STR_VALUE + value.context() + ':' + value.scope().toString());
	}
}
