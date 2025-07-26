package org.radix.hyperscale.network.messages;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.SerializerId2;
import org.radix.hyperscale.utils.Numbers;
import org.radix.hyperscale.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("network.message.blob")
public final class BlobMessage extends Message
{
	@JsonProperty("data")
	@DsonOutput(Output.ALL)
	private byte[] data;
	
	private BlobMessage()
	{
		super();
	}
	
	public BlobMessage(final byte[] data)
	{
		super();
		
		Objects.requireNonNull(data, "Data blob is null");
		Numbers.isZero(data.length, "Data blob length is zero");
		
		this.data = Arrays.copyOf(data, data.length);
	}

	public byte[] getData()
	{
		return this.data;
	}
}
