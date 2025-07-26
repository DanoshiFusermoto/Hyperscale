package org.radix.hyperscale;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.util.Map;

import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;
import com.sun.management.GarbageCollectionNotificationInfo;

import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;

public class GCMonitor
{
	private static final Logger memoryLog = Logging.getLogger("memory");
	static 
	{
		memoryLog.setLevels(Logging.ALL);
		memoryLog.setStdOut(false);
	}
    
    private static final long 	GC_DURATION_THRESHOLD_MS = 250;
    private static final double MEMORY_RELEASED_THRESHOLD_PERCENT = 25.0;
    private static final double AVAILABLE_HEAP_WARNING_PERCENT = 25.0;
    private static final double AVAILABLE_HEAP_CRITICAL_PERCENT = 10.0;
    
    private static volatile GCMonitor instance;
    private static final Object lock = new Object();
    public static GCMonitor getInstance() 
    {
        if (instance == null) 
        {
            synchronized (lock) 
            {
                if (instance == null)
                    instance = new GCMonitor();
            }
        }
        
        return instance;
    }
    
    private volatile long totalGCEvents = 0;
    private volatile long totalGCTime = 0;
    
    private GCMonitor()
    {
        for (final GarbageCollectorMXBean gcBean : ManagementFactory.getGarbageCollectorMXBeans()) 
        {
            final NotificationEmitter emitter = (NotificationEmitter) gcBean;
            final NotificationListener listener = (notification, handback) -> {
                if (notification.getType().equals(GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION)) 
                {
                    final GarbageCollectionNotificationInfo info = GarbageCollectionNotificationInfo.from((CompositeData) notification.getUserData());
                    analyzeGCEvent(info);
                }
            };
            
            emitter.addNotificationListener(listener, null, null);
        }

        memoryLog.info("GC monitoring started");
    }
    
    private void analyzeGCEvent(GarbageCollectionNotificationInfo info) 
    {
        final long duration = info.getGcInfo().getDuration();
        final String gcName = info.getGcName();
        final String gcAction = info.getGcAction();
        
        this.totalGCEvents++;
        this.totalGCTime += duration;
        
        // Get memory usage before and after GC
        MemoryUsage heapBefore = info.getGcInfo().getMemoryUsageBeforeGc().get("Heap");
        MemoryUsage heapAfter = info.getGcInfo().getMemoryUsageAfterGc().get("Heap");
        
        // Handle case where heap memory usage might be reported differently
        if (heapBefore == null || heapAfter == null) 
        {
            // Try to get from all memory pools
            heapBefore = getTotalHeapUsage(info.getGcInfo().getMemoryUsageBeforeGc());
            heapAfter = getTotalHeapUsage(info.getGcInfo().getMemoryUsageAfterGc());
        }
        
        if (heapBefore == null || heapAfter == null) 
        {
            // Fallback: only check duration
            if (duration >= GC_DURATION_THRESHOLD_MS)
                logGCEvent(Logging.INFO, gcName, gcAction, duration, 0, 0, 0);

            return;
        }
        
        // Calculate metrics
        final long totalHeap = heapAfter.getMax();
        final long memoryReleased = heapBefore.getUsed() - heapAfter.getUsed();
        final double memoryReleasedPercent = (double) memoryReleased / totalHeap * 100.0;
        
        final long availableAfterGC = totalHeap - heapAfter.getUsed();
        final double availablePercent = (double) availableAfterGC / totalHeap * 100.0;
        
        // Check conditions and trigger output
        boolean shouldLog = false;
        int alertLevel = Logging.INFO;
        
        // Condition 1: Duration > 250ms
        if (duration >= GC_DURATION_THRESHOLD_MS) 
        {
            shouldLog = true;
            alertLevel = Logging.WARN;
        }
        
        // Condition 2: Memory released > 25% of total heap
        if (memoryReleasedPercent > MEMORY_RELEASED_THRESHOLD_PERCENT) 
        {
            shouldLog = true;
            if (alertLevel != Logging.WARN)
                alertLevel = Logging.INFO;
        }
        
        // Condition 3: Available heap post-GC < 25%
        if (availablePercent < AVAILABLE_HEAP_WARNING_PERCENT) 
        {
            shouldLog = true;
            alertLevel = Logging.WARN;
        }
        
        // Condition 4: Available heap post-GC < 10% (CRITICAL)
        if (availablePercent < AVAILABLE_HEAP_CRITICAL_PERCENT) 
        {
            shouldLog = true;
            alertLevel = Logging.FATAL;
        }
        
        if (shouldLog)
            logGCEvent(alertLevel, gcName, gcAction, duration, memoryReleasedPercent, availablePercent, totalHeap);
    }
    
    private MemoryUsage getTotalHeapUsage(Map<String, MemoryUsage> memoryUsageMap) 
    {
        long used = 0;
        long committed = 0;
        long max = 0;
        boolean foundHeapPools = false;
        
        for (java.util.Map.Entry<String, MemoryUsage> entry : memoryUsageMap.entrySet()) 
        {
            String poolName = entry.getKey().toLowerCase();
            // Look for typical heap pool names
            if (poolName.contains("eden") || poolName.contains("survivor") || 
                poolName.contains("old") || poolName.contains("tenured") ||
                poolName.contains("young") || poolName.contains("heap")) {
                
                MemoryUsage usage = entry.getValue();
                used += usage.getUsed();
                committed += usage.getCommitted();
                if (usage.getMax() > 0)
                    max += usage.getMax();

                foundHeapPools = true;
            }
        }
        
        return foundHeapPools ? new MemoryUsage(0, used, committed, max > 0 ? max : committed) : null;
    }
    
    private void logGCEvent(int level, String gcName, String gcAction, long duration, double memoryReleasedPercent, double availablePercent, long totalHeap) 
    {
        final StringBuilder message = new StringBuilder();
        message.append(String.format("GC Event %d - %s %s | Duration: %d ms / %d ms", this.totalGCEvents, gcName, gcAction, duration, this.totalGCTime));
        
        if (totalHeap > 0) 
        {
            message.append(String.format(" | Memory Released: %.1f%% | Available Heap Post-GC: %.1f%% (%.1f MB / %.1f MB)", 
                         		memoryReleasedPercent,
                         		availablePercent,
                         		(totalHeap * availablePercent / 100.0) / (1024 * 1024),
                         		totalHeap / (1024.0 * 1024.0)));
        }
        
        // Add specific condition details
        final StringBuilder conditions = new StringBuilder();
        if (duration >= GC_DURATION_THRESHOLD_MS)
            conditions.append(String.format(" [Long GC pause > %d ms]", GC_DURATION_THRESHOLD_MS));

        if (memoryReleasedPercent > MEMORY_RELEASED_THRESHOLD_PERCENT)
            conditions.append(String.format(" [Large memory release > %.1f%%]", MEMORY_RELEASED_THRESHOLD_PERCENT));

        if (availablePercent < AVAILABLE_HEAP_CRITICAL_PERCENT)
            conditions.append(String.format(" [CRITICAL: High heap usage < %.1f%% available]", AVAILABLE_HEAP_CRITICAL_PERCENT));
        else if (availablePercent < AVAILABLE_HEAP_WARNING_PERCENT)
            conditions.append(String.format(" [High heap usage < %.1f%% available]", AVAILABLE_HEAP_WARNING_PERCENT));
        
        final String fullMessage = message.toString() + conditions.toString();
        
        // Log at appropriate level
        switch (level) {
            case Logging.FATAL:
            	memoryLog.fatal("GC CRITICAL: "+fullMessage);
                break;
            case Logging.WARN:
            	memoryLog.warn("GC Warning: "+fullMessage);
                break;
            case Logging.INFO:
            default:
            	memoryLog.info("GC Info: "+fullMessage);
                break;
        }
    }
}
