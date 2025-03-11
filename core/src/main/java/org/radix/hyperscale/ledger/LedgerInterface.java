package org.radix.hyperscale.ledger;

import java.util.concurrent.Future;

public interface LedgerInterface
{
	public Future<PrimitiveSearchResponse> get(final PrimitiveSearchQuery query);

	public Future<SubstateSearchResponse> get(final SubstateSearchQuery query);
	
	public Future<AssociationSearchResponse> get(final AssociationSearchQuery query);
}