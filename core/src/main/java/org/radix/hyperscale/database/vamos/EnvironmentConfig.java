package org.radix.hyperscale.database.vamos;

import java.util.concurrent.TimeUnit;

public class EnvironmentConfig implements Cloneable 
{
    /**
     * An instance created using the default constructor is initialized with default settings.
     */
    public static final EnvironmentConfig DEFAULT = new EnvironmentConfig();

    private int lockTimeout = 120;
    
    private int indexNodesPerFile = 1<<18;

    private int indexNodeSize = 4096;
    private int indexNodeCount = 1<<18;
    private int indexNodeDeferredWriteHint = 4;
    private boolean indexNodeDeferredWriteAvailable = false;
    
    private long checkpointIntervalSeconds = 1200;
    private long checkpointAfterBytesWritten = 1l<<32l;

    private boolean hasLogCache = true;
    private int logCacheMaxEntries = 1<<16;
    private int logCacheMaxItemSize = 1<<12;
    private int logCompressionThreshold = 1024;
    
    private boolean hasIndexNodeCache = true;
    private int indexNodeCacheMaxEntries = 1<<16;

    private boolean hasIndexItemCache = true;
    private int indexItemCacheMaxEntries = 1<<16;

    private boolean hasExtensionItemCache = true;
    private int extensionItemCacheMaxEntries = 1<<10;

    private boolean hasExtensionNodeCache = true;
    private int extensionNodeCacheMaxEntries = 1<<10;

    public EnvironmentConfig() 
    {
    	// Disable the IndexItem cache if it is the same size as the IndexNode cache
    	if (this.indexNodeCacheMaxEntries == this.indexItemCacheMaxEntries)
    		this.hasIndexItemCache = false;
    	
    	updateIndexNodeDeferredWrite();    
    }
    
    private void updateIndexNodeDeferredWrite()
    {
    	// Deferred writes are possible?
    	if (this.indexNodeCount <= this.indexNodeCacheMaxEntries)
    		this.indexNodeDeferredWriteAvailable = true;
    	else
    		this.indexNodeDeferredWriteAvailable = false;
    }
    
    public int getLockTimeout() 
    {
        return this.lockTimeout;
    }

    public EnvironmentConfig setLockTimeout(int lockTimeout) 
    {
        this.lockTimeout = lockTimeout;
        return this;
    }

    public boolean hasIndexNodeCache()
    {
    	return this.hasIndexNodeCache;
    }

    public boolean hasIndexNodeCache(boolean enabled)
    {
    	this.hasIndexNodeCache = enabled;
    	return enabled;
    }

    public int getIndexNodeCacheMaxEntries() 
    {
        return this.indexNodeCacheMaxEntries;
    }
    
    public EnvironmentConfig setIndexNodeCacheMaxEntries(int indexNodeCacheMaxEntries) 
    {
        this.indexNodeCacheMaxEntries = indexNodeCacheMaxEntries;
    	updateIndexNodeDeferredWrite();    
        return this;
    }

    public boolean hasIndexItemCache()
    {
    	return this.hasIndexItemCache;
    }

    public boolean hasIndexItemCache(boolean enabled)
    {
    	this.hasIndexItemCache = enabled;
    	return enabled;
    }

    public int getIndexItemCacheMaxEntries() 
    {
        return this.indexItemCacheMaxEntries;
    }
    
    public EnvironmentConfig setIndexItemCacheMaxEntries(int indexItemCacheMaxEntries) 
    {
        this.indexItemCacheMaxEntries = indexItemCacheMaxEntries;
        return this;
    }

    public int getExtensionItemCacheMaxEntries() 
    {
        return this.extensionItemCacheMaxEntries;
    }

    public EnvironmentConfig setExtensionItemCacheMaxEntries(int extensionItemCacheMaxEntries) 
    {
        this.extensionItemCacheMaxEntries = extensionItemCacheMaxEntries;
        return this;
    }

    public int getExtensionNodeCacheMaxEntries() 
    {
        return this.extensionNodeCacheMaxEntries;
    }

    public EnvironmentConfig setExtensionNodeCacheMaxEntries(int extensionNodeCacheMaxEntries) 
    {
        this.extensionNodeCacheMaxEntries = extensionNodeCacheMaxEntries;
        return this;
    }
    
    public boolean hasLogCache()
    {
    	return this.hasLogCache;
    }

    public boolean hasLogCache(boolean enabled)
    {
    	this.hasLogCache = enabled;
    	return enabled;
    }

    public int getLogCacheMaxEntries() 
    {
        return this.logCacheMaxEntries;
    }

    public EnvironmentConfig setLogCacheMaxEntries(int logCacheMaxEntries) 
    {
        this.logCacheMaxEntries = logCacheMaxEntries;
        return this;
    }

    public int getLogCacheMaxItemSize() 
    {
        return this.logCacheMaxItemSize;
    }

    public EnvironmentConfig setLogCacheMaxItemSize(int logCacheMaxItemSize) 
    {
        this.logCacheMaxItemSize = logCacheMaxItemSize;
        return this;
    }
    
    public EnvironmentConfig setCheckpointAfterBytesWritten(long checkpointAfterBytesWritten) 
    {
        this.checkpointAfterBytesWritten = checkpointAfterBytesWritten;
        return this;
    }

    public long getCheckpointAfterBytesWritten() 
    {
        return this.checkpointAfterBytesWritten;
    }
    
    public EnvironmentConfig setCheckpointInterval(long checkpointInterval, TimeUnit timeUnit) 
    {
        this.checkpointIntervalSeconds = timeUnit.toSeconds(checkpointInterval);
        return this;
    }

    public long getCheckpointInterval(TimeUnit timeUnit) 
    {
        return timeUnit.convert(this.checkpointIntervalSeconds, TimeUnit.SECONDS);
    }

    public int getIndexNodeSize() 
    {
        return this.indexNodeSize;
    }

    public EnvironmentConfig setIndexNodeSize(int indexNodeSize) 
    {
        this.indexNodeSize = indexNodeSize;
        return this;
    }

    public int getIndexNodeCapacity() 
    {
        return this.indexNodeSize / IndexItem.BYTES;
    }

    public int getIndexNodeCount() 
    {
        return this.indexNodeCount;
    }

    public EnvironmentConfig setIndexNodeCount(int indexNodeCount) 
    {
        this.indexNodeCount = indexNodeCount;
    	updateIndexNodeDeferredWrite();    
        return this;
    }
    
    public int getIndexNodesPerFile() 
    {
        return this.indexNodesPerFile;
    }
    
    public boolean isIndexNodeDeferredWriteAvailable()
    {
    	return this.indexNodeDeferredWriteAvailable;
    }

    public int getIndexNodeDeferredWriteHint()
    {
    	return this.indexNodeDeferredWriteHint;
    }

    public EnvironmentConfig setIndexNodesPerFile(int indexNodesPerFile) 
    {
    	if (indexNodesPerFile < 1<<16)
    		throw new IllegalArgumentException("Index nodes per file can not be less than "+(1<<16));    	
        this.indexNodesPerFile = indexNodesPerFile;
        return this;
    }

	public int getLogCompressionThreshold()
	{
		return this.logCompressionThreshold;
	}
	
	public EnvironmentConfig setLogCompressionThreshold(int bytesThreshold)
	{
		this.logCompressionThreshold = bytesThreshold;
		return this;
	}
}
