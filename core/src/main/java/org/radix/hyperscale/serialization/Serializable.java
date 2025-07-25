package org.radix.hyperscale.serialization;

import java.nio.ByteBuffer;

import org.radix.hyperscale.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonPropertyOrder({ SerializerConstants.SERIALIZER_TYPE_NAME })
public abstract class Serializable 
{
	private volatile ByteBuffer cachedOutput;
	
	@JsonProperty(SerializerConstants.SERIALIZER_TYPE_NAME)
	@DsonOutput(Output.ALL)
	private final SerializerDummy serializer = SerializerDummy.DUMMY;
	
	public void onSerialize(Output output) 
	{ }
	
	public void onDeserialized() 
	{ }
	
	public boolean shouldCacheDsonOutput()
	{
		return getClass().getAnnotation(DsonCached.class) != null;
	}
	
	public final ByteBuffer getCachedDsonOutput()
	{
		return this.cachedOutput;
	}

	public final void setCachedDsonOutput(final ByteBuffer cachedOutput)
	{
		this.cachedOutput = cachedOutput;
	}
	
	public final void flushCachedDsonOutput()
	{
		final ByteBuffer byteBuffer = this.cachedOutput;
		if (byteBuffer != null)
		{
			this.cachedOutput = null;
			Serialization.bufferPool.release(this, byteBuffer);
		}
	}
}
