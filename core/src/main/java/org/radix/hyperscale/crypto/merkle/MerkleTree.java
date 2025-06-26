package org.radix.hyperscale.crypto.merkle;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.eclipse.collections.api.factory.Maps;
import org.eclipse.collections.api.map.MutableMap;
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.impl.factory.primitive.LongObjectMaps;
import org.radix.hyperscale.crypto.Hash;

public class MerkleTree
{
    private MerkleNode root;
    private final MutableMap<Hash, MerkleNode> leaves;
    private transient MutableLongObjectMap<MerkleProof> proofs;

    public MerkleTree() 
    {
        this.leaves = Maps.mutable.<Hash, MerkleNode>empty().asSynchronized();
    }

    public MerkleTree(int expectedSize) 
    {
        this.leaves = Maps.mutable.<Hash, MerkleNode>ofInitialCapacity(expectedSize).asSynchronized();
    }

    public List<MerkleNode> getLeaves() 
    {
    	synchronized(this.leaves)
    	{
    		return this.leaves.toList();
    	}
    }
    
    public MerkleNode getRoot() 
    {
        return this.root;
    }

    public MerkleNode appendLeaf(MerkleNode node) 
    {
        this.leaves.put(node.getHash(), node);
        return node;
    }

    public void appendLeaves(MerkleNode[] nodes) 
    {
        for (MerkleNode node : nodes)
            appendLeaf(node);
    }

    public MerkleNode appendLeaf(Hash hash) 
    {
        return appendLeaf(new MerkleNode(hash));
    }

    public List<MerkleNode> appendLeaves(Hash[] hashes) 
    {
        List<MerkleNode> nodes = new ArrayList<>(hashes.length);
        for (Hash hash : hashes)
            nodes.add(appendLeaf(hash));

        return nodes;
    }

    public List<MerkleNode> appendLeaves(Collection<Hash> hashes) 
    {
        List<MerkleNode> nodes = new ArrayList<>(hashes.size());
        for (Hash hash : hashes)
            nodes.add(appendLeaf(hash));

        return nodes;
    }

    public Hash addTree(MerkleTree tree) 
    {
        if (this.leaves.size() <= 0) 
        	throw new IllegalStateException("Cannot add to a tree with no leaves!");
        
        tree.leaves.forEach((h, l) -> appendLeaf(l));
        return this.buildTree();
    }
    
	public boolean isEmpty()
	{
        return this.leaves.isEmpty(); 
	}

    public Hash buildTree() 
    {
        if (this.leaves.size() <= 0) 
        	throw new IllegalStateException("Cannot build a tree with no leaves!");
        
        buildTree(getLeaves());
        return this.root.getHash();
    }

    private void buildTree(List<MerkleNode> nodes) 
    {
        if (nodes.isEmpty())
            throw new IllegalStateException("Node list not expected to be empty!");
        
        int currentSize = nodes.size();
        while (currentSize > 1) 
        {
            int nextSize = 0;
            for (int i = 0; i < currentSize; i += 2) 
            {
                MerkleNode left = nodes.get(i);
                MerkleNode right = (i + 1 < currentSize) ? nodes.get(i + 1) : null;
                MerkleNode parent = new MerkleNode(left, right);
                nodes.set(nextSize++, parent);
            }
            
            currentSize = nextSize;
        }
        
        this.root = nodes.get(0);
    }
    
    public MerkleAudit auditProof(Hash leafHash) 
    {
    	synchronized(this)
    	{
    		if (this.proofs == null)
    			this.proofs = LongObjectMaps.mutable.<MerkleProof>ofInitialCapacity(this.leaves.size()*2).asSynchronized();
    	}
    	
        final List<MerkleProof> auditTrail = new ArrayList<>();
        final MerkleNode leafNode = findLeaf(leafHash);
        if (leafNode != null) 
        {
            if (leafNode.getParent() == null) 
            	throw new IllegalStateException("Expected leaf to have a parent!");
            
            final MerkleNode parent = leafNode.getParent();
            buildAuditTrail(auditTrail, parent, leafNode);
        }

        return new MerkleAudit(auditTrail);
    }

    public static boolean verifyAudit(Hash rootHash, Hash leafHash, MerkleAudit auditTrail) 
    {
        if (auditTrail.isEmpty()) 
        	throw new IllegalStateException("Audit trail cannot be empty!");

        Hash testHash = leafHash;

        for (MerkleProof auditHash : auditTrail) 
            testHash = auditHash.getDirection().equals(MerkleProof.Branch.RIGHT) ? Hash.hash(testHash, auditHash.getHash()) : Hash.hash(auditHash.getHash(), testHash);

        return testHash.equals(rootHash);
    }

    private MerkleNode findLeaf(Hash hash) 
    {
        return this.leaves.get(hash);
    }

    private void buildAuditTrail(List<MerkleProof> auditTrail, MerkleNode parent, MerkleNode child) 
    {
        if (parent != null) 
        {
            if (child.getParent() != parent)
                throw new IllegalStateException("Parent of child is not expected parent!");

            final MerkleNode nextChild = parent.getLeftNode().equals(child) ? parent.getRightNode() : parent.getLeftNode();
            final MerkleProof.Branch direction = parent.getLeftNode().equals(child) ? MerkleProof.Branch.RIGHT : MerkleProof.Branch.LEFT;

            if (nextChild != null)
            {
            	long locator = (nextChild.getHash().asLong() << 1l) + direction.ordinal();
            	MerkleProof proof = this.proofs.get(locator);
            	if (proof == null)
            	{
                	proof = MerkleProof.from(nextChild.getHash(), direction);
                	this.proofs.put(locator, proof);
            	}
            	
            	auditTrail.add(proof);
            }
            
            buildAuditTrail(auditTrail, parent.getParent(), child.getParent());
        }
    }
}