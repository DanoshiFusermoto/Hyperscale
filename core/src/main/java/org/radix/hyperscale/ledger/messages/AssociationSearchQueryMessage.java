package org.radix.hyperscale.ledger.messages;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;
import org.radix.hyperscale.ledger.AssociationSearchQuery;
import org.radix.hyperscale.network.messages.Message;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.DsonOutput.Output;
import org.radix.hyperscale.serialization.SerializerId2;

@SerializerId2("ledger.messages.search.query.association")
public class AssociationSearchQueryMessage extends Message {
  @JsonProperty("query")
  @DsonOutput(Output.ALL)
  private AssociationSearchQuery query;

  AssociationSearchQueryMessage() {
    super();
  }

  public AssociationSearchQueryMessage(final AssociationSearchQuery query) {
    super();

    this.query = Objects.requireNonNull(query, "Association query is null");
  }

  public AssociationSearchQuery getQuery() {
    return this.query;
  }
}
