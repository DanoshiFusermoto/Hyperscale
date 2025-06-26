package org.radix.hyperscale.database.vamos;

import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.mutable.MutableLong;
import org.radix.hyperscale.collections.LRUCacheMap;
import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;

/**
 * A cache for index nodes and data that is shared between all databases
 * 
 * @author Dan
 *
 */
class EnvironmentCache 
{
	private static final Logger vamosLog = Logging.getLogger("vamos");

	private final Environment environment;
	
    private final AtomicLong indexItemCacheHits;
    private final AtomicLong indexItemCacheMisses;
    private final AtomicLong indexNodeCacheHits;
    private final AtomicLong indexNodeCacheMisses;
    private final AtomicLong logCacheHits;
    private final AtomicLong logCacheMisses;

    private final LRUCacheMap<IndexNodeID, IndexNode> indexNodeCache;
    private final LRUCacheMap<InternalKey, IndexItem> indexItemCache;
    private final LRUCacheMap<Long, LogOperation> logOperationCache;
    
    EnvironmentCache(final Environment environment)
    {
		this.environment = Objects.requireNonNull(environment, "Environment is null");
	
	    if (this.environment.getConfig().hasIndexNodeCache())
	    	this.indexNodeCache = new LRUCacheMap<IndexNodeID, IndexNode>(this.environment.getConfig().getIndexNodeCacheMaxEntries());
	    else
	    	this.indexNodeCache = null;
	    
	    if (this.environment.getConfig().hasIndexItemCache())
	    	this.indexItemCache = new LRUCacheMap<InternalKey, IndexItem>(this.environment.getConfig().getIndexItemCacheMaxEntries());
	    else
	    	this.indexItemCache = null;
	    
	    if (this.environment.getConfig().hasLogCache())
	    	this.logOperationCache = new LRUCacheMap<Long, LogOperation>(this.environment.getConfig().getLogCacheMaxEntries());
	    else
	    	this.logOperationCache = null;
	    
	    this.indexItemCacheHits = new AtomicLong(0);
	    this.indexItemCacheMisses = new AtomicLong(0);
	    this.indexNodeCacheHits = new AtomicLong(0);
	    this.indexNodeCacheMisses = new AtomicLong(0);
	    this.logCacheHits = new AtomicLong(0);
	    this.logCacheMisses = new AtomicLong(0);
    }
    
    void clearIndexNodes()
    {
    	if (this.indexNodeCache == null)
    		return;
    	
   		this.indexNodeCache.clear();
    }
    
    IndexNode getIndexNode(IndexNodeID ID)
    {
    	if (this.indexNodeCache == null)
    		return null;
    	
   		return this.indexNodeCache.get(ID);
    }

    void putIndexNode(IndexNode indexNode)
    {
    	if (this.indexNodeCache == null)
    		return;

   		this.indexNodeCache.put(indexNode.getID(), indexNode);
    }
    
	void removeIndexNode(IndexNodeID ID) 
	{
    	if (this.indexNodeCache == null)
    		return;

   		this.indexNodeCache.remove(ID);
	}

    void clearIndexItems()
    {
    	if (this.indexItemCache == null)
    		return;
    	
   		this.indexItemCache.clear();
    }
    
    IndexItem getIndexItem(InternalKey key)
    {
    	if (this.indexItemCache == null)
    		return null;

    	final IndexItem indexItem = this.indexItemCache.get(key);
    	if (indexItem == null)
    		this.indexItemCacheMisses.incrementAndGet();
    	else
    		this.indexItemCacheHits.incrementAndGet();
    	
    	logInfo();
    		
    	return indexItem;
    }

    void putIndexItem(InternalKey internalKey, IndexItem indexItem)
    {
    	if (this.indexItemCache == null)
    		return;

   		this.indexItemCache.put(internalKey, indexItem);
    }

    void clearLogOperations()
    {
    	if (this.logOperationCache == null)
    		return;
    	
   		this.logOperationCache.clear();
    }

    LogOperation getLogOperation(long position)
    {
    	if (this.logOperationCache == null)
    		return null;

   		final LogOperation logOperation = this.logOperationCache.get(position);
   		if (logOperation == null)
   			this.logCacheMisses.incrementAndGet();
   		else
   			this.logCacheHits.incrementAndGet();
   		
    	logInfo();

    	return logOperation;
    }

    void putLogOperation(LogOperation logOperation)
    {
    	if (this.logOperationCache == null)
    		return;
    	
    	if (logOperation.isExtension())
    		return;

   		this.logOperationCache.put(logOperation.getLogPosition(), logOperation);
    }
    
    private long opened = System.currentTimeMillis();
    private long lastLog = -1;
    private AtomicBoolean logging = new AtomicBoolean(false);
    private void logInfo()
    {
    	if (vamosLog.hasLevel(Logging.INFO) == false)
    		return;
    	
		long age = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - this.opened);
		if (age == 0 || age <= this.lastLog)
			return;
		
		if (TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - this.opened) % 10 == 0)
		{
			if (logging.compareAndSet(false, true) == true)
			{
				final MutableLong logCacheBytes = new MutableLong();
				this.logOperationCache.forEach((l,o) -> logCacheBytes.add(o.length()));
				
				vamosLog.info("Cache:");
				vamosLog.info(String.format("     Index Items %d %d", this.indexItemCacheHits.get(), this.indexItemCacheMisses.get()));
				vamosLog.info(String.format("     Index Nodes %d %d", this.indexNodeCacheHits.get(), this.indexNodeCacheMisses.get()));
				vamosLog.info(String.format("  Log Operations %d %d", this.logCacheHits.get(), this.logCacheMisses.get()));
				vamosLog.info(String.format("  Log Cache Size %d %d", this.logOperationCache.size(), logCacheBytes.longValue()));
				
				this.lastLog = age;
				this.logging.set(false);
			}
		}
    }

}
