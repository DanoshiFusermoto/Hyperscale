package org.radix.hyperscale.ledger;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.radix.hyperscale.common.BasicObject;
import org.radix.hyperscale.common.Match;
import org.radix.hyperscale.common.Order;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.DsonOutput.Output;
import org.radix.hyperscale.serialization.SerializerId2;
import org.radix.hyperscale.utils.Numbers;

@SerializerId2("ledger.search.query.association")
public final class AssociationSearchQuery extends BasicObject {
  public static final int MAX_LIMIT = 50;
  public static final int MAX_ASSOCIATIONS = 50;

  @JsonProperty("associations")
  @DsonOutput(Output.ALL)
  private List<Hash> associations;

  @JsonProperty("matchon")
  @DsonOutput(Output.ALL)
  private Match matchOn;

  @JsonProperty("filter")
  @DsonOutput(Output.ALL)
  private String filterContext;

  @JsonProperty("order")
  @DsonOutput(Output.ALL)
  private Order order;

  @JsonProperty("offset")
  @DsonOutput(Output.ALL)
  private long offset;

  @JsonProperty("limit")
  @DsonOutput(Output.ALL)
  private int limit;

  AssociationSearchQuery() {
    super();
  }

  public AssociationSearchQuery(
      final Hash association, final String filterContext, final Order order) {
    this(
        Collections.singletonList(Objects.requireNonNull(association, "Association is null")),
        Match.ANY,
        filterContext,
        order,
        -1,
        AssociationSearchQuery.MAX_LIMIT);
  }

  public AssociationSearchQuery(
      final Hash association, final String filterContext, final Order order, final int limit) {
    this(
        Collections.singletonList(Objects.requireNonNull(association, "Association is null")),
        Match.ANY,
        filterContext,
        order,
        -1,
        limit);
  }

  public AssociationSearchQuery(
      final Hash association,
      final String filterContext,
      final Order order,
      final long offset,
      final int limit) {
    this(
        Collections.singletonList(Objects.requireNonNull(association, "Association is null")),
        Match.ANY,
        filterContext,
        order,
        offset,
        limit);
  }

  public AssociationSearchQuery(
      final Hash association,
      final Match matchOn,
      final String filterContext,
      final Order order,
      final long offset,
      final int limit) {
    this(
        Collections.singletonList(Objects.requireNonNull(association, "Association is null")),
        matchOn,
        filterContext,
        order,
        offset,
        limit);
  }

  public AssociationSearchQuery(
      final Collection<Hash> associations,
      final Match matchOn,
      final String filterContext,
      final Order order,
      final int limit) {
    this(associations, matchOn, filterContext, order, -1, limit);
  }

  // TODO can offset limit just be zero for new searches?
  public AssociationSearchQuery(
      final Collection<Hash> associations,
      final Match matchOn,
      final String filterContext,
      final Order order,
      final long offset,
      final int limit) {
    super();

    Numbers.lessThan(offset, -1, "Offset is less than -1");
    Numbers.isNegative(limit, "Limit is negative");
    Numbers.greaterThan(
        limit, AssociationSearchQuery.MAX_LIMIT, "Limit is greater than " + MAX_LIMIT);
    Numbers.greaterThan(
        associations.size(),
        AssociationSearchQuery.MAX_ASSOCIATIONS,
        "Associations size is greater than " + MAX_ASSOCIATIONS);

    this.matchOn = Objects.requireNonNull(matchOn, "Match is null");
    this.associations = new ArrayList<Hash>(Objects.requireNonNull(associations));
    this.filterContext = Objects.requireNonNull(filterContext, "Filter context is null");
    this.order = Objects.requireNonNull(order, "Order is null");
    this.offset = offset;
    this.limit = limit;

    if (this.offset < 0) {
      if (this.order.equals(Order.DESCENDING)) this.offset = Long.MAX_VALUE;
      else this.offset = -1;
    }
  }

  public List<Hash> getAssociations() {
    return this.associations;
  }

  public long getOffset() {
    return this.offset;
  }

  public Order getOrder() {
    return this.order;
  }

  public int getLimit() {
    return this.limit;
  }

  public String getFilterContext() {
    return this.filterContext;
  }

  public Match getMatchOn() {
    return this.matchOn;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + this.order.hashCode();
    result = prime * result + this.limit;
    result = (int) (prime * result + this.offset);
    result = prime * result + this.associations.hashCode();
    result = prime * result + this.matchOn.hashCode();
    result = prime * result + this.filterContext.hashCode();
    return result;
  }

  @Override
  public boolean equals(Object object) {
    if (object == null) return false;

    if (object == this) return true;

    if (object instanceof AssociationSearchQuery associationSearchQuery) {
      if (this.filterContext.equalsIgnoreCase(associationSearchQuery.filterContext) == false)
        return false;

      if (hashCode() != object.hashCode()) return false;

      if (this.associations.equals(associationSearchQuery.getAssociations()) == false) return false;

      if (this.matchOn.equals(associationSearchQuery.getMatchOn()) == false) return false;

      if (this.offset != associationSearchQuery.getOffset()) return false;

      if (this.limit != associationSearchQuery.getLimit()) return false;

      if (this.order.equals(associationSearchQuery.getOrder()) == false) return false;

      return true;
    }

    return false;
  }

  @Override
  public String toString() {
    return this.associations.toString()
        + " "
        + this.matchOn
        + " "
        + this.filterContext
        + " "
        + this.offset
        + " "
        + this.limit;
  }
}
