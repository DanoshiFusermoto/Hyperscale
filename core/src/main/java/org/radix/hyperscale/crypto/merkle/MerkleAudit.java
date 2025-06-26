package org.radix.hyperscale.crypto.merkle;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.crypto.merkle.MerkleProof.Branch;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.Serializable;
import org.radix.hyperscale.serialization.SerializerId2;
import org.radix.hyperscale.utils.Numbers;
import org.radix.hyperscale.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("merkle.audit")
public final class MerkleAudit extends Serializable implements Iterable<MerkleProof>
{
	public static final MerkleAudit NULL = singleton(MerkleProof.from(Hash.ZERO, Branch.ROOT));

	public static final MerkleAudit singleton(MerkleProof proof)
	{
		Objects.requireNonNull(proof, "Proof is null");
		return new MerkleAudit(Collections.singletonList(proof));
	}
	
	@JsonProperty("proofs")
	@DsonOutput(Output.ALL)
	private List<MerkleProof> proofs;
	
	private MerkleAudit()
	{
		// FOR SERIALIZER
	}
	
	MerkleAudit(final List<MerkleProof> proofs)
	{
		Objects.requireNonNull(proofs, "Proofs is null");
		Numbers.isZero(proofs.size(), "Proofs is empty");
		
		this.proofs = proofs;
	}
	
	@Override
	public Iterator<MerkleProof> iterator() 
	{
	    return new Iterator<MerkleProof>() 
	    {
	        private final Iterator<MerkleProof> delegate = proofs.iterator();
	        
	        @Override
	        public boolean hasNext() 
	        {
	            return delegate.hasNext();
	        }
	        
	        @Override
	        public MerkleProof next() 
	        {
	            return delegate.next();
	        }
	        
	        @Override
	        public void remove() 
	        {
	            throw new UnsupportedOperationException("Cannot modify MerkleAudit");
	        }
	    };
	}

	public boolean isEmpty()
	{
		return this.proofs.isEmpty();
	}

	public int size()
	{
		return this.proofs.size();
	}
}
