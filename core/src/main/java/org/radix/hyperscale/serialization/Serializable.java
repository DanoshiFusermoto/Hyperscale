package org.radix.hyperscale.serialization;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.radix.hyperscale.serialization.DsonOutput.Output;

@JsonPropertyOrder({SerializerConstants.SERIALIZER_TYPE_NAME})
public abstract class Serializable {
  private volatile byte[] cachedDsonOutput;

  @JsonProperty(SerializerConstants.SERIALIZER_TYPE_NAME)
  @DsonOutput(Output.ALL)
  private final SerializerDummy serializer = SerializerDummy.DUMMY;

  public void onSerialize(Output output) {}

  public void onDeserialized() {}

  public boolean shouldCacheDsonOutput() {
    return getClass().getAnnotation(DsonCached.class) != null;
  }

  public final byte[] getCachedDsonOutput() {
    return this.cachedDsonOutput;
  }

  public final void setCachedDsonOutput(byte[] output) {
    this.cachedDsonOutput = output;
  }

  public final void flushCachedDsonOutput() {
    this.cachedDsonOutput = null;
  }
}
