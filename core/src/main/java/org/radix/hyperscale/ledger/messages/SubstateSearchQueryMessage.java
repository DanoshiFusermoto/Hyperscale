package org.radix.hyperscale.ledger.messages;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;
import org.radix.hyperscale.ledger.SubstateSearchQuery;
import org.radix.hyperscale.network.messages.Message;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.DsonOutput.Output;
import org.radix.hyperscale.serialization.SerializerId2;

@SerializerId2("ledger.messages.search.query.substate")
public class SubstateSearchQueryMessage extends Message {
  @JsonProperty("query")
  @DsonOutput(Output.ALL)
  private SubstateSearchQuery query;

  SubstateSearchQueryMessage() {
    super();
  }

  public SubstateSearchQueryMessage(final SubstateSearchQuery query) {
    super();

    this.query = Objects.requireNonNull(query, "Substate search query is null");
  }

  public SubstateSearchQuery getQuery() {
    return this.query;
  }
}
