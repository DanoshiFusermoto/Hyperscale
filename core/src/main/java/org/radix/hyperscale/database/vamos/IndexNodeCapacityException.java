package org.radix.hyperscale.database.vamos;

import java.io.IOException;

class IndexNodeCapacityException extends IOException
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 6017034481625074055L;
	
	private final IndexNode indexNode;
	
	IndexNodeCapacityException(IndexNode indexNode)
	{
		super("Index node "+indexNode.getID()+" is at capacity of "+indexNode.capacity());
		
		this.indexNode = indexNode;
	}
	
	IndexNode getIndexNode()
	{
		return this.indexNode;
	}
}
