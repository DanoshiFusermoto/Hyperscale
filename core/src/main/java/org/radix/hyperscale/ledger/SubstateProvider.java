package org.radix.hyperscale.ledger;

import java.io.IOException;

interface SubstateProvider
{
	boolean has(final StateAddress stateAddress) throws IOException;
	Substate get(final StateAddress stateAddress) throws IOException; 
}
