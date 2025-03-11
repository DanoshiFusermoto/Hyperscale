package org.radix.hyperscale.database.vamos;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.eclipse.collections.api.factory.Maps;
import org.eclipse.collections.api.map.primitive.MutableObjectLongMap;
import org.eclipse.collections.impl.factory.primitive.ObjectLongMaps;
import org.radix.hyperscale.database.DatabaseException;
import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;
import org.radix.hyperscale.utils.Longs;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

class Index 
{
	/* Thread local ByteBuffer for performing I/O on IndexNodes. Set to the max IndexNode size of 64kb*/ 
	private static final ThreadLocal<ByteBuffer> indexNodeByteBuffer = ThreadLocal.withInitial(() -> ByteBuffer.allocate(65535));

	private static final Logger vamosLog = Logging.getLogger("vamos");

	final static int INDEX_HEADER_LENGTH = 0;
	final static long POSITION_UNKNOWN = -1;
	final static long POSITION_NOT_EXISTS = Long.MIN_VALUE;

	private final Environment environment;

	private class IndexFile
	{
	    private final RandomAccessFile file;
	    private final AtomicLong lastPosition;
	    
	    IndexFile(RandomAccessFile file)
	    {
	    	this.file = file;
	    	this.lastPosition = new AtomicLong(-1);
	    }

		protected RandomAccessFile get() 
		{
			return this.file;
		}

		protected long getLastPosition() 
		{
			return this.lastPosition.get();
		}

		protected void setLastPosition(long position) 
		{
			this.lastPosition.set(position);
		}
	}
	
    private final IndexFile indexFiles[];
    private final Object indexMutex = new Object();
    
    private final AtomicLong indexItemReads;
    private final AtomicLong indexItemWrites;
    private final AtomicLong indexNodeReads;
    private final AtomicLong indexNodeWrites;
    private final AtomicLong indexNodeModifications;
    private final AtomicLong consecutiveIndexNodeReads;
    private final AtomicLong consecutiveIndexNodeWrites;
    private final AtomicLong profiledIndexNodeReads;
    private final AtomicLong profiledIndexNodeWrites;

    private final long opened;
    
    private class IndexWorker implements Runnable
    {
        private final List<IndexItem> indexItemsToQueue;
        private final Map<InternalKey, IndexItem> indexItems;
        private final Multimap<IndexNodeID, IndexItem> indexItemsToNodes;
        private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        
    	private volatile boolean terminated = false;

        IndexWorker()
        {
        	this.indexItems = Maps.mutable.ofInitialCapacity(1024);
        	this.indexItemsToNodes = ArrayListMultimap.create();
        	this.indexItemsToQueue = new ArrayList<>(1024);
        }

		@Override
		public void run()
		{
	        final List<IndexItem> indexItemsToWrite = new ArrayList<>(1024);
			while(this.terminated == false)
			{
                LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
                
				try
				{
					this.lock.readLock().lock();
					try
					{
						if (this.indexItems.isEmpty())
							continue;
						
						indexItemsToWrite.clear();
						
						if (this.indexItemsToQueue.isEmpty() == false)
						{
							for (int i = 0 ; i < this.indexItemsToQueue.size() ; i++)
							{
								final IndexItem indexItem = this.indexItemsToQueue.get(i);
								final IndexNodeID indexNodeID = Index.this.environment.toIndexNodeID(indexItem.getKey());
								final Collection<IndexItem> indexItems = this.indexItemsToNodes.get(indexNodeID);
								indexItems.add(indexItem);
								
						    	if (Index.this.environment.getConfig().isIndexNodeDeferredWriteAvailable() == false || 
						        	indexItems.size() >= Index.this.environment.getConfig().getIndexNodeDeferredWriteHint())
						    		indexItemsToWrite.addAll(indexItems);
							}

							this.indexItemsToQueue.clear();
						}
					}
					finally
					{
						this.lock.readLock().unlock();
					}
					
					final Transaction transaction = new Transaction(Index.this.environment, 4);
					final Map<IndexNodeID, IndexNode> indexNodes = new HashMap<>();
					IndexNode lockedIndexNode = null;
					IndexNodeID lockedIndexNodeID = null;
					try
					{
						for (int i = 0 ; i < indexItemsToWrite.size() ; i++)
						{
							final IndexItem indexItem = indexItemsToWrite.get(i);
							final IndexNodeID indexNodeID = Index.this.environment.toIndexNodeID(indexItem.getKey());
							if (lockedIndexNodeID != indexNodeID)
							{
								if (lockedIndexNodeID != null)
								{
									Index.this.writeIndexNode(lockedIndexNode);
						        	Index.this.environment.getLockManager().unlock(lockedIndexNodeID, transaction);
						        	lockedIndexNodeID = null;
						        	lockedIndexNode = null;
								}
								
								try 
								{
									Index.this.environment.getLockManager().lock(indexNodeID, transaction, Index.this.environment.getConfig().getLockTimeout(), TimeUnit.SECONDS);
								} 
								catch (InterruptedException ex) 
								{
									Thread.currentThread().interrupt();
									throw new DatabaseException("Lock interruption on index node ID "+indexNodeID+" when writing "+indexItemsToWrite.size()+" index items");
								}
								
								lockedIndexNodeID = indexNodeID;
								lockedIndexNode = indexNodes.get(indexNodeID);
								if (lockedIndexNode == null)
								{
									lockedIndexNode = readIndexNode(indexNodeID);
									indexNodes.put(indexNodeID, lockedIndexNode);
								}
							}

							if (indexItem.getType() == null) // DELETE
								lockedIndexNode.delete(indexItem.getKey());
							else
								lockedIndexNode.put(indexItem);
						}
						
						// Write the final index node and unlock
						if (lockedIndexNodeID != null)
						{
							Index.this.writeIndexNode(lockedIndexNode);
				        	Index.this.environment.getLockManager().unlock(lockedIndexNodeID, transaction);
						}

						Index.this.indexItemWrites.addAndGet(indexItemsToWrite.size());
					}
					catch (final Throwable t)
					{
						if (lockedIndexNodeID != null)
				        	Index.this.environment.getLockManager().unlock(lockedIndexNodeID, transaction);
					}

					this.lock.writeLock().lock();
					try
					{
						for (int i = 0 ; i < indexItemsToWrite.size() ; i++)
						{
							final IndexItem indexItem = indexItemsToWrite.get(i);
							this.indexItems.remove(indexItem.getKey(), indexItem);
							this.indexItemsToNodes.remove(Index.this.environment.toIndexNodeID(indexItem.getKey()), indexItem);
						}
					}
					finally
					{
						this.lock.writeLock().unlock();
					}
				}
				catch (final Throwable t)
				{
					vamosLog.fatal(t);
					this.terminated = true;
				}
			}
		}
		
		private IndexItem getIndexItem(final InternalKey internalKey)
		{
        	this.lock.readLock().lock();
        	try
        	{
        		return this.indexItems.get(internalKey);
        	}
        	finally
        	{
        		this.lock.readLock().unlock();
        	}
		}

		private void queueIndexItems(final Collection<IndexItem> indexItems)
        {
			if (this.terminated)
				throw new IllegalStateException("Log worker is terminated");
			
			final EnvironmentCache cache = Index.this.environment.getConfig().hasIndexItemCache() ? Index.this.environment.getCache() : null;
        	this.lock.writeLock().lock();
        	try
        	{
				for (IndexItem indexItem : indexItems)
				{
					this.indexItems.put(indexItem.getKey(), indexItem);
					
					if (cache != null)
						cache.putIndexItem(indexItem.getKey(), indexItem);
					
					if (vamosLog.hasLevel(Logging.DEBUG))
						vamosLog.debug("Queued index item for writing "+indexItem);
				}
        		this.indexItemsToQueue.addAll(indexItems);
        	}
        	finally
        	{
        		this.lock.writeLock().unlock();
        	}
        }
		
    }
    private final IndexWorker indexWorker = new IndexWorker();
    
    Index(final Environment environment) throws IOException 
	{
		this.environment = Objects.requireNonNull(environment, "Environment is null");
		
		this.indexItemReads = new AtomicLong(0);
		this.indexItemWrites = new AtomicLong(0);
		this.indexNodeReads = new AtomicLong(0);
		this.indexNodeWrites = new AtomicLong(0);
		this.indexNodeModifications = new AtomicLong(0);
		this.consecutiveIndexNodeReads = new AtomicLong(0);
		this.consecutiveIndexNodeWrites = new AtomicLong(0);
		this.profiledIndexNodeReads = new AtomicLong(0);
		this.profiledIndexNodeWrites = new AtomicLong(0);
		
		this.indexFiles = new IndexFile[environment.getConfig().getIndexNodeCount() / environment.getConfig().getIndexNodesPerFile()];
		
		int i = 0;
		boolean performIndexInit = true;
		do
		{
			File indexFile = new File(this.environment.getDirectory(), "vamos-"+i+".idx");
			
			boolean indexFileExists = indexFile.exists();
			if (indexFileExists == true)
				performIndexInit = false;
			
			if (performIndexInit == false && indexFileExists == false)
				throw new DatabaseException("Index file "+indexFile+" does not exist");

			this.indexFiles[i] = new IndexFile(new RandomAccessFile(indexFile, "rw"));
			
			i++;
		}
		while(i < this.indexFiles.length);

		if (performIndexInit) 
		{
			IndexFile indexFile = this.indexFiles[0];
	
			long indexNodeOffsetPosition = this.environment.getConfig().getIndexNodeCount() * Long.BYTES;
			ByteArrayOutputStream indexNodePositionHeaderBaos = new ByteArrayOutputStream();
			for (long n = 0 ; n < this.environment.getConfig().getIndexNodeCount() ; n++)
			{
				indexNodePositionHeaderBaos.write(Longs.toByteArray(indexNodeOffsetPosition + (n * this.environment.getConfig().getIndexNodeSize()))); //Longs.toByteArray(-1));
				if (indexNodePositionHeaderBaos.size() == (8192 * Long.BYTES))
				{
					indexFile.get().write(indexNodePositionHeaderBaos.toByteArray());
					indexNodePositionHeaderBaos.reset();
				}
			}
			
			if (indexNodePositionHeaderBaos.size() > 0)
				indexFile.get().write(indexNodePositionHeaderBaos.toByteArray());
			
			System.out.println("Preparing Vamos index ...");
			byte[] paddedIndexNodeBytes = new byte[this.environment.getConfig().getIndexNodeSize()];
			
			for (int n = 0 ; n < this.environment.getConfig().getIndexNodeCount() ; n++)
			{
				IndexNodeID indexNodeID = IndexNodeID.from(n);
				IndexNode indexNode = new IndexNode(indexNodeID, this.environment.getConfig().getIndexNodeSize());
				byte[] indexNodeBytes = indexNode.toByteArray();
				System.arraycopy(indexNodeBytes, 0, paddedIndexNodeBytes, 0, indexNodeBytes.length);

				indexFile = this.indexFiles[n / environment.getConfig().getIndexNodesPerFile()];
				indexFile.get().write(paddedIndexNodeBytes);
				
				if (n > 0 && n % (this.environment.getConfig().getIndexNodeCount() / 10) == 0)
					System.out.print(" . ");
			}
			System.out.println();
		}
		
	    this.opened = System.currentTimeMillis();
	    
		Thread indexWorkerThread = new Thread(this.indexWorker);
		indexWorkerThread.setDaemon(true);
		indexWorkerThread.setName("Index Worker");
		indexWorkerThread.setPriority(7);
		indexWorkerThread.start();
	}
    
    IndexItem getIndexItem(final Transaction transaction, final InternalKey internalKey) throws IOException
    {
    	IndexItem indexItem = this.indexWorker.getIndexItem(internalKey);
    	if (indexItem != null)
    		return indexItem;
    	
		indexItem = this.environment.getCache().getIndexItem(internalKey);
    	if (indexItem != null)
    		return indexItem;
    	
    	final IndexNodeID indexNodeID = this.environment.toIndexNodeID(internalKey);
		try 
		{
			this.environment.getLockManager().lock(indexNodeID, transaction, this.environment.getConfig().getLockTimeout(), TimeUnit.SECONDS);
			
	    	final IndexNode indexNode = readIndexNode(indexNodeID);
	    	indexItem = indexNode.getOrDefault(internalKey, IndexItem.VACANT);

	    	this.environment.getCache().putIndexItem(internalKey, indexItem);
		} 
		catch (InterruptedException ex) 
		{
			Thread.currentThread().interrupt();
			throw new DatabaseException("Lock interruption on index node ID "+indexNodeID+" for transaction "+transaction+" on database "+this);
		}
        finally
        {
        	this.environment.getLockManager().unlock(indexNodeID, transaction);
    		this.indexItemReads.incrementAndGet();
        }

		return indexItem;
    }
    
    IndexItem readIndexItem(final Database database, final InternalKey internalKey) throws IOException
    {
    	IndexItem indexItem = this.indexWorker.getIndexItem(internalKey);
    	if (indexItem != null && indexItem.equals(IndexItem.VACANT) == false)
    		return indexItem;
    	
		indexItem = this.environment.getCache().getIndexItem(internalKey);
    	if (indexItem != null && indexItem.equals(IndexItem.VACANT) == false)
    		return indexItem;

    	final IndexNodeID indexNodeID = this.environment.toIndexNodeID(internalKey);
		final Transaction transaction = new Transaction(this.environment);

		final IndexNode indexNode;
		try 
		{
			this.environment.getLockManager().lock(indexNodeID, transaction, this.environment.getConfig().getLockTimeout(), TimeUnit.SECONDS);
		} 
		catch (InterruptedException ex) 
		{
			Thread.currentThread().interrupt();
			throw new DatabaseException("Lock interruption on index node ID "+indexNodeID+" for transaction "+transaction+" on database "+database);
		}

		try
		{
			indexNode = readIndexNode(indexNodeID);
			if (indexNode == null)
				return null;
				
			return indexNode.get(internalKey);
    	}
    	finally
        {
    		this.environment.getLockManager().unlock(indexNodeID, transaction);
    		this.indexItemReads.incrementAndGet();
        }
    }
    
    void writeIndexItems(final Collection<IndexItem> indexItems)
    {
    	this.indexWorker.queueIndexItems(indexItems);
    }

    private Collection<IndexNode> readIndexNodes(final Collection<IndexNodeID> indexNodeIDs) throws IOException
    {
    	final MutableObjectLongMap<IndexNodeID> indexNodePositions = ObjectLongMaps.mutable.ofInitialCapacity(indexNodeIDs.size());
    	synchronized(this.indexMutex)
    	{
        	for(final IndexNodeID indexNodeID : indexNodeIDs)
        	{
        		final IndexNode indexNode = this.environment.getCache().getIndexNode(indexNodeID);
            	if (indexNode != null)
            		indexNodePositions.getIfAbsentPut(indexNodeID, Long.MAX_VALUE);
            	else
            		indexNodePositions.getIfAbsentPut(indexNodeID, getIndexNodePosition(indexNodeID, false));
        	}
    	}
    	
    	final ArrayList<IndexNode> indexNodes = new ArrayList<IndexNode>(indexNodePositions.size());
   		for (final IndexNodeID indexNodeID : indexNodePositions.keySet())
   		{
   			if (indexNodePositions.getOrThrow(indexNodeID) == Long.MAX_VALUE)
            	indexNodes.add(this.environment.getCache().getIndexNode(indexNodeID));
   			else   				
   				indexNodes.add(readIndexNode(indexNodeID, indexNodePositions.getOrThrow(indexNodeID)));
   		}
    	
    	return indexNodes;
    }
    
    IndexNode readIndexNode(final IndexNodeID indexNodeID) throws IOException
    {
    	IndexNode indexNode = this.environment.getCache().getIndexNode(indexNodeID);
    	if (indexNode == null)
    	{
    		final long indexNodePosition = getIndexNodePosition(indexNodeID, false);
			if (indexNodePosition < 0)
			{
				indexNode = new IndexNode(indexNodeID, this.environment.getConfig().getIndexNodeSize());
				
				if (vamosLog.hasLevel(Logging.DEBUG))
					vamosLog.debug("Created index node "+indexNode.getID()+":"+indexNode.toString()+" containing "+indexNode.size()+" items");
			}
			else
				indexNode = readIndexNode(indexNodeID, indexNodePosition);
			
			this.environment.getCache().putIndexNode(indexNode);
    	}
		
		return indexNode;
    }

    private IndexNode readIndexNode(final IndexNodeID indexNodeID, final long position) throws IOException
    {
    	final ByteBuffer byteBuffer = indexNodeByteBuffer.get();
    	byteBuffer.clear();
    	
    	final IndexFile indexFile = mapPositionToIndexFile(position);
    	final long remappedPosition = remapPositionByIndexFile(position);
		synchronized(indexFile)
    	{
			if (indexFile.getLastPosition() != remappedPosition)
				indexFile.get().seek(remappedPosition);
			else
				this.consecutiveIndexNodeReads.incrementAndGet();
				
			indexFile.get().read(byteBuffer.array(), 0, this.environment.getConfig().getIndexNodeSize());
			indexFile.setLastPosition(remappedPosition + this.environment.getConfig().getIndexNodeSize());
    	}
		
		this.indexNodeReads.incrementAndGet();

		final IndexNode indexNode = new IndexNode(byteBuffer);
		if (vamosLog.hasLevel(Logging.DEBUG))
			vamosLog.debug("Read index node "+indexNode);

		logInfo();

		return indexNode;
    }
    
    private long getIndexNodePosition(final IndexNodeID indexNodeID, final boolean allocate) throws IOException
    {
		long indexNodeOffsetPosition = this.environment.getConfig().getIndexNodeCount() * Long.BYTES;
    	long indexNodePosition = indexNodeOffsetPosition + ((long)indexNodeID.value() * this.environment.getConfig().getIndexNodeSize());
		return indexNodePosition;
    }
    
    void writeIndexNode(final IndexNode indexNode) throws IOException
    {
    	if (this.environment.getConfig().isIndexNodeDeferredWriteAvailable() == false || 
    		indexNode.modifications() >= this.environment.getConfig().getIndexNodeDeferredWriteHint() ||
    		System.currentTimeMillis() - indexNode.modifiedAt() >= this.environment.getConfig().getCheckpointInterval(TimeUnit.MILLISECONDS) / 2)
    		writeIndexNodeInternal(indexNode);
    	else
   			this.indexNodeModifications.addAndGet(indexNode.modifications());
    }

    private void writeIndexNodeInternal(final IndexNode indexNode) throws IOException
    {
		long indexNodePosition = getIndexNodePosition(indexNode.getID(), true);
    	final ByteBuffer buffer = indexNodeByteBuffer.get();
   		buffer.clear();
   		synchronized(indexNode)
   		{
   			indexNode.write(buffer);
   			writeIndexNodeInternal(indexNode, indexNodePosition, buffer);
   			this.indexNodeModifications.addAndGet(indexNode.modifications());
   			indexNode.flushed();
   		}
    }

    private void writeIndexNodes(final Collection<IndexNode> indexNodes) throws IOException
    {
    	if (this.environment.getConfig().isIndexNodeDeferredWriteAvailable() == false) 
    		writeIndexNodesInternal(indexNodes);
    	else
    	{
    		for (IndexNode indexNode : indexNodes)
    			writeIndexNode(indexNode);
    	}
    }
    	
    private void writeIndexNodesInternal(final Collection<IndexNode> indexNodes) throws IOException
    {
    	final List<Entry<Long, IndexNode>> indexNodePositions = new ArrayList<Entry<Long, IndexNode>>(indexNodes.size());
		synchronized(this.indexMutex)
		{
			for(final IndexNode indexNode : indexNodes)
				indexNodePositions.add(new AbstractMap.SimpleEntry<Long, IndexNode>(getIndexNodePosition(indexNode.getID(), true), indexNode));
		}
		
		indexNodePositions.sort((o1, o2) -> {
			if (o1.getKey() < o2.getKey())
				return -1;
			if (o1.getKey() > o2.getKey())
				return 1;
			return 0;
		});

    	final ByteBuffer buffer = indexNodeByteBuffer.get();
   		for (final Entry<Long, IndexNode> indexNode : indexNodePositions)
   		{
   			buffer.clear();
   	   		synchronized(indexNode.getValue())
   	   		{
   	   			indexNode.getValue().write(buffer);
   	   			writeIndexNodeInternal(indexNode.getValue(), indexNode.getKey(), buffer);
   	   			this.indexNodeModifications.addAndGet(indexNode.getValue().modifications());
   	   			indexNode.getValue().flushed();
   	   		}
   		}
    }

    private void writeIndexNodeInternal(final IndexNode indexNode, final long position, final ByteBuffer serialized) throws IOException
    {
    	final IndexFile indexFile = mapPositionToIndexFile(position);
    	final long remappedPosition = remapPositionByIndexFile(position);

		synchronized(indexFile)
    	{
			if (indexFile.getLastPosition() != remappedPosition)
				indexFile.get().seek(remappedPosition);
			else
				this.consecutiveIndexNodeWrites.incrementAndGet();
				
			indexFile.get().write(serialized.array(), 0, serialized.position());
			indexFile.setLastPosition(remappedPosition + serialized.position());
    	}
		
		this.indexNodeWrites.incrementAndGet();
		
		if (vamosLog.hasLevel(Logging.DEBUG))
			vamosLog.debug("Wrote index node "+indexNode);
		
		logInfo();
    }
    
    private long lastLog = -1;
    private void logInfo()
    {
		long age = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - this.opened);
		if (age == 0 || age <= this.lastLog)
			return;
		
		if (TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - this.opened) % 10 == 0)
		{
			vamosLog.info("Index:");
			vamosLog.info(String.format("  Item Reads %d %d/s", this.indexItemReads.get(), (this.indexItemReads.get() / age)));
			vamosLog.info(String.format("  Item Writes %d %d/s", this.indexItemWrites.get(), (this.indexItemWrites.get() / age)));
			vamosLog.info(String.format("  Node Reads %d %d/s %d", this.indexNodeReads.get(), (this.indexNodeReads.get() / age), this.consecutiveIndexNodeReads.get()));
			vamosLog.info(String.format("  Node Writes %d %d/s %d %d", this.indexNodeWrites.get(), (this.indexNodeWrites.get() / age), this.consecutiveIndexNodeWrites.get(), this.indexNodeModifications.get()));
			
			this.lastLog = age;
		}
    }
    
    private IndexFile mapPositionToIndexFile(final long position)
    {
    	final long indexNodeOffsetPosition = this.environment.getConfig().getIndexNodeCount() * Long.BYTES;
    	final long adjustedPosition = position - indexNodeOffsetPosition;
    	if (adjustedPosition < 0)
    		throw new IllegalArgumentException("Position  is negative after adjusting for index node offset");
    	
    	int i = (int) (adjustedPosition / this.environment.getConfig().getIndexNodeSize());
    	i = i / this.environment.getConfig().getIndexNodesPerFile();
    	return this.indexFiles[i];
    }
    
    private long remapPositionByIndexFile(final long position)
    {
		long indexNodeOffsetPosition = this.environment.getConfig().getIndexNodeCount() * Long.BYTES;
    	final long adjustedPosition = position - indexNodeOffsetPosition;
    	if (adjustedPosition < 0)
    		throw new IllegalArgumentException("Position is negative after adjusting for index node offset");
    	
    	long i = adjustedPosition / this.environment.getConfig().getIndexNodeSize();
    	i = i / this.environment.getConfig().getIndexNodesPerFile();
    	
    	if (i == 0)
    	{
    		final long remappedPosition = indexNodeOffsetPosition + adjustedPosition;
    		return remappedPosition;
    	}
    	else
    	{
    		final long remappedPosition = adjustedPosition - (this.environment.getConfig().getIndexNodesPerFile() * i * this.environment.getConfig().getIndexNodeSize());
    		return remappedPosition;
    	}
    }

}
