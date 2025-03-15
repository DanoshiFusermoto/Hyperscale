package org.radix.hyperscale.ledger;

import java.util.concurrent.Future;

public interface LedgerInterface {
  Future<PrimitiveSearchResponse> get(final PrimitiveSearchQuery query);

  Future<SubstateSearchResponse> get(final SubstateSearchQuery query);

  Future<AssociationSearchResponse> get(final AssociationSearchQuery query);
}
