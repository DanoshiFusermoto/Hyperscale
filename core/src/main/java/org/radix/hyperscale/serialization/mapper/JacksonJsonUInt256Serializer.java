package org.radix.hyperscale.serialization.mapper;

import java.io.IOException;

import org.radix.hyperscale.utils.UInt256;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

/**
 * Serializer for conversion from {@code UInt256} data to the appropriate JSON encoding.
 */
class JacksonJsonUInt256Serializer extends StdSerializer<UInt256> 
{
	private static final long serialVersionUID = 8190847216433553689L;

	JacksonJsonUInt256Serializer() 
	{
		this(null);
	}

	JacksonJsonUInt256Serializer(Class<UInt256> t) 
	{
		super(t);
	}

	@Override
	public void serialize(UInt256 value, JsonGenerator jgen, SerializerProvider provider) throws IOException 
	{
		jgen.writeString(JacksonCodecConstants.U256_STR_VALUE + value.toString());
	}
}
