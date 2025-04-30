package org.radix.hyperscale.ledger;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public interface LedgerSearchInterface
{
	public Future<PrimitiveSearchResponse> get(final PrimitiveSearchQuery query, final long timeout, final TimeUnit timeunit);

	public Future<SubstateSearchResponse> get(final SubstateSearchQuery query, final long timeout, final TimeUnit timeunit);
	
	public Future<AssociationSearchResponse> get(final AssociationSearchQuery query, final long timeout, final TimeUnit timeunit);
}