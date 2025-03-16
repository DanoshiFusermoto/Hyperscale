package org.radix.hyperscale.ledger.messages;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;
import org.radix.hyperscale.ledger.PrimitiveSearchResponse;
import org.radix.hyperscale.network.messages.Message;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.DsonOutput.Output;
import org.radix.hyperscale.serialization.SerializerId2;

@SerializerId2("ledger.messages.search.response.primitive")
public class PrimitiveSearchResponseMessage extends Message {
  @JsonProperty("response")
  @DsonOutput(Output.ALL)
  private PrimitiveSearchResponse response;

  PrimitiveSearchResponseMessage() {
    super();
  }

  public PrimitiveSearchResponseMessage(final PrimitiveSearchResponse response) {
    super();

    this.response = Objects.requireNonNull(response, "Response is null");
  }

  public PrimitiveSearchResponse getResponse() {
    return this.response;
  }
}
