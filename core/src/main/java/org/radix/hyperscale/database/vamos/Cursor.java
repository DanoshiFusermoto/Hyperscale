package org.radix.hyperscale.database.vamos;

import java.io.IOException;
import java.util.Objects;

import org.radix.hyperscale.database.vamos.IndexItem.Type;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.OperationStatus;

public class Cursor 
{
	private final Database database;
	private final DatabaseEntry key;
	private final InternalKey internalKey;
	
	private transient volatile ExtensionNode extensionNode;
	private transient volatile ExtensionItem currentExtensionItem;
	private transient volatile long currentPosition;
	
	public Cursor(final Database database, final DatabaseEntry key)
	{
		Objects.requireNonNull(database, "Database is null");
		Objects.requireNonNull(key, "Key is null");
		
		if (database.getConfig().getAllowDuplicates() == false)
			throw new IllegalStateException("Database "+database.getName()+" does not support cursors");
		
		this.database = database;
		this.key = new DatabaseEntry(key.getData());
		this.internalKey = InternalKey.from(database, this.key);
		this.currentPosition = -1;
	}
	
	public void close() 
	{
		
	}

	public OperationStatus getFirst(DatabaseEntry value) throws IOException
	{
		IndexItem indexItem = this.database.getEnvironment().getIndex().readIndexItem(this.database, this.internalKey);
		if(indexItem == null)
			return OperationStatus.NOTFOUND;
		
		if (indexItem.getType() != Type.EXTENSION)
			throw new IllegalStateException("Index item is not "+Type.EXTENSION+" for "+value+" in database "+this.database.getName());
		
		ExtensionNode extensionNode = this.database.getEnvironment().getLog().readExtensionNode(indexItem);
		this.extensionNode = extensionNode;
		this.currentPosition = extensionNode.getFirstPosition();
		
		ExtensionItem extensionItem = this.database.getEnvironment().getLog().readExtensionItem(indexItem.getKey(), this.currentPosition);
		this.currentExtensionItem = extensionItem;

		value.setData(this.database.getEnvironment().getLog().readLogData(extensionItem.getLogPosition()));
		
		return OperationStatus.SUCCESS;
	}
	
	public OperationStatus getPrev(DatabaseEntry value) throws IOException
	{
		if (this.currentExtensionItem.getPrevious() == -1)
			return OperationStatus.NOTFOUND;
		
		ExtensionItem extensionItem = this.database.getEnvironment().getLog().readExtensionItem(this.currentExtensionItem.getKey(), this.currentExtensionItem.getPrevious());
		this.currentExtensionItem = extensionItem;
		
		value.setData(this.database.getEnvironment().getLog().readLogData(extensionItem.getLogPosition()));
		
		return OperationStatus.SUCCESS;
	}

	public OperationStatus getNext(DatabaseEntry value) throws IOException
	{
		if (this.currentExtensionItem.getNext() == -1)
			return OperationStatus.NOTFOUND;
		
		ExtensionItem extensionItem = this.database.getEnvironment().getLog().readExtensionItem(this.currentExtensionItem.getKey(), this.currentExtensionItem.getNext());
		this.currentExtensionItem = extensionItem;
		
		value.setData(this.database.getEnvironment().getLog().readLogData(extensionItem.getLogPosition()));
		
		return OperationStatus.SUCCESS;
	}
	
	public OperationStatus getLast(DatabaseEntry value) throws IOException
	{
		IndexItem indexItem = this.database.getEnvironment().getIndex().readIndexItem(this.database, this.internalKey);
		if(indexItem == null)
			return OperationStatus.NOTFOUND;
		
		if (indexItem.getType() != Type.EXTENSION)
			throw new IllegalStateException("Index item is not "+Type.EXTENSION+" for "+value+" in database "+this.database.getName());
		
		ExtensionNode extensionNode = this.database.getEnvironment().getLog().readExtensionNode(indexItem);
		this.extensionNode = extensionNode;
		this.currentPosition = extensionNode.getLastPosition();
		
		ExtensionItem extensionItem = this.database.getEnvironment().getLog().readExtensionItem(indexItem.getKey(), this.currentPosition);
		this.currentExtensionItem = extensionItem;

		value.setData(this.database.getEnvironment().getLog().readLogData(extensionItem.getLogPosition()));
		
		return OperationStatus.SUCCESS;
	}
}
