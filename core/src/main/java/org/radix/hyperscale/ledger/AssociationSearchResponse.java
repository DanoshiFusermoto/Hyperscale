package org.radix.hyperscale.ledger;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import org.radix.hyperscale.common.BasicObject;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.DsonOutput.Output;
import org.radix.hyperscale.serialization.SerializerId2;

@SerializerId2("ledger.search.response.association")
public final class AssociationSearchResponse extends BasicObject {
  @JsonProperty("query")
  @DsonOutput(Output.ALL)
  private AssociationSearchQuery query;

  @JsonProperty("next_offset")
  @DsonOutput(Output.ALL)
  private long nextOffset;

  @JsonProperty("results")
  @DsonOutput(Output.ALL)
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonDeserialize(as = LinkedHashSet.class)
  private Set<SubstateCommit> results;

  @JsonProperty("eor")
  @DsonOutput(Output.ALL)
  private boolean EOR;

  AssociationSearchResponse() {
    super();
  }

  public AssociationSearchResponse(final AssociationSearchQuery query) {
    super();

    this.nextOffset = -1;

    this.query = Objects.requireNonNull(query, "Association search query is null");
    this.results = Collections.emptySet();
    this.EOR = true;
  }

  public AssociationSearchResponse(
      final AssociationSearchQuery query,
      final long nextOffset,
      final Collection<SubstateCommit> results,
      final boolean EOR) {
    super();

    this.nextOffset = nextOffset;
    this.query = Objects.requireNonNull(query, "Association search query is null");
    this.results =
        new LinkedHashSet<SubstateCommit>(
            Objects.requireNonNull(results, "Association search results is null"));
    this.EOR = EOR;
  }

  public AssociationSearchQuery getQuery() {
    return this.query;
  }

  public long getNextOffset() {
    return this.nextOffset;
  }

  public boolean isEmpty() {
    return this.results.isEmpty();
  }

  public boolean isEOR() {
    return this.EOR;
  }

  public int size() {
    return this.results.size();
  }

  public Collection<SubstateCommit> getResults() {
    return Collections.unmodifiableCollection(this.results);
  }

  @Override
  public int hashCode() {
    return (int) (this.query.hashCode() * this.nextOffset)
        + this.results.hashCode()
        + (this.EOR == true ? 1 : 0);
  }

  @Override
  public boolean equals(Object object) {
    if (object == null) return false;

    if (object == this) return true;

    if (object instanceof AssociationSearchResponse associationSearchResponse) {
      if (hashCode() != object.hashCode()) return false;

      if (this.query.equals(associationSearchResponse.getQuery()) == false) return false;

      if (this.nextOffset != associationSearchResponse.getNextOffset()) return false;

      if (this.EOR != associationSearchResponse.isEOR()) return false;

      if (this.results.equals(associationSearchResponse.getResults()) == false) return false;

      return true;
    }

    return false;
  }

  @Override
  public String toString() {
    return this.query + " " + this.nextOffset + " " + this.results.size();
  }
}
