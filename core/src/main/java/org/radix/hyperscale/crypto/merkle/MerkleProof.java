package org.radix.hyperscale.crypto.merkle;

import org.apache.commons.lang3.ArrayUtils;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.crypto.Hashable;

public class MerkleProof implements Hashable
{
	public static MerkleProof from(byte[] bytes)
	{
		return new MerkleProof(bytes, 0);
	}
	
	public static MerkleProof from(byte[] bytes, int offset)
	{
		return new MerkleProof(bytes, offset);
	}
	
	public static MerkleProof from(Hash hash, Branch direction)
	{
		return new MerkleProof(hash, direction);
	}

	public static MerkleProof from(String string)
	{
		Branch direction = Branch.ROOT;
		if (string.charAt(0) == 'r')
			direction = Branch.RIGHT;
		else if (string.charAt(0) == 'l')
			direction = Branch.LEFT;
		Hash hash = Hash.from(string, 2);
		return from(hash, direction);
	}
	
	public static final int BYTES = Hash.BYTES + Byte.BYTES; 
	
	public enum Branch 
	{
        LEFT,
        RIGHT,
        ROOT
    }

    private final Hash hash;
    private final Branch direction;

    private volatile byte[] cachedBytes = null;
    
	private MerkleProof(byte[] bytes, int offset)
	{
		this.direction = Branch.values()[bytes[offset]];
		this.hash = Hash.from(bytes, offset+1);
	}

	private MerkleProof(Hash hash, Branch direction) 
    {
        this.hash = hash;
        this.direction = direction;
    }

    @Override
    public Hash getHash() 
    {
        return this.hash;
    }

    public Branch getDirection() 
    {
        return this.direction;
    }
    
    @Override
	public int hashCode() 
    {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.hash.hashCode();
		result = prime * result +  this.direction.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj) 
	{
		if (this == obj)
			return true;
	
		if (obj == null)
			return false;
		
		if (getClass() != obj.getClass())
			return false;
		
		MerkleProof other = (MerkleProof) obj;
		if (this.direction != other.direction)
			return false;
		
		if (this.hash.equals(other.hash) == false)
			return false;

		return true;
	}

	@Override
    public String toString() 
    {
        String hash = this.hash.toString();
        String direction;
        switch(this.direction)
        {
        case LEFT:
        	direction = "l";
        	break;
        case RIGHT:
        	direction = "r";
        	break;
        default:
        	direction = "x";
        	break;
    	}
        
        return direction+":"+hash;
    }
    
	public synchronized byte[] toByteArray()
	{
		if (this.cachedBytes == null)
			this.cachedBytes = ArrayUtils.addFirst(this.hash.toByteArray(), (byte) this.direction.ordinal());

		return this.cachedBytes;
	}
}
