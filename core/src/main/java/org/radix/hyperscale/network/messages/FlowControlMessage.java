package org.radix.hyperscale.network.messages;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.DsonOutput.Output;
import org.radix.hyperscale.serialization.SerializerId2;

@SerializerId2("network.message.flow.control")
public final class FlowControlMessage extends Message {
  @JsonProperty("sequences")
  @DsonOutput(Output.ALL)
  private Set<Long> sequences;

  FlowControlMessage() {
    super();
  }

  public FlowControlMessage(final Collection<Long> sequences) {
    super();

    if (Objects.requireNonNull(sequences, "Sequences is null").isEmpty())
      throw new IllegalArgumentException("Sequences is empty");

    this.sequences = new HashSet<Long>(sequences);
  }

  public Set<Long> getSequences() {
    return this.sequences;
  }
}
