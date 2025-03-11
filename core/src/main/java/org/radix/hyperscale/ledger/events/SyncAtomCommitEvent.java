package org.radix.hyperscale.ledger.events;

import org.radix.hyperscale.ledger.BlockHeader;
import org.radix.hyperscale.ledger.CommitDecision;
import org.radix.hyperscale.ledger.PendingAtom;

public final class SyncAtomCommitEvent extends AtomEventWithProposalHeader 
{
	public SyncAtomCommitEvent(final BlockHeader header, final PendingAtom pendingAtom)
	{
		super(header, pendingAtom);
		
		if (pendingAtom.getCertificate() == null)
			throw new IllegalArgumentException("Atom certificate not present for "+pendingAtom.getHash());

		if (pendingAtom.getCertificate().getDecision().equals(CommitDecision.ACCEPT) == false && 
			pendingAtom.getCertificate().getDecision().equals(CommitDecision.REJECT) == false)
			throw new UnsupportedOperationException("Commit decision of type "+pendingAtom.getCertificate().getDecision()+" is not supported");
	}
	
	public CommitDecision getDecision()
	{
		return getPendingAtom().getCertificate().getDecision();
	}
}