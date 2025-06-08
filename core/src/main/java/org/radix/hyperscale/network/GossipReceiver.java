package org.radix.hyperscale.network;

import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.radix.hyperscale.Context;
import org.radix.hyperscale.common.Primitive;
import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;

public abstract class GossipReceiver<T extends Primitive>
{
	private static final Logger gossipLog = Logging.getLogger("gossip");

	private final Context context;
	private volatile long iterations; 
	private volatile long duration;
	
	private final long reportInterval;
	private final TimeUnit reportTimeUnit;
	private volatile long lastReportAt;
	
	public GossipReceiver(final Context context)
	{
		Objects.requireNonNull(context, "Context is null");
		this.context = context;
		
		this.reportInterval = 60;
		this.reportTimeUnit = TimeUnit.SECONDS;
		this.lastReportAt = System.currentTimeMillis();
	}
	
	public final void receive(final Class<? extends Primitive> type, final Collection<T> items, final AbstractConnection connection) throws Throwable
	{
		final long start = System.nanoTime();
		try
		{
			process(type, items, connection);
		}
		finally
		{
			this.iterations++;
			this.duration += (System.nanoTime() - start);
			
			if (System.currentTimeMillis() - this.lastReportAt >= TimeUnit.MILLISECONDS.convert(this.reportInterval, this.reportTimeUnit))
			{
				final double averageDurationNano = this.duration / this.iterations;
				final double averageDurationMS = averageDurationNano / 1_000_000;
				gossipLog.info(this.context.getName()+": Receive processor for "+type.getSimpleName()+" "+this.iterations+" iterations duration="+TimeUnit.NANOSECONDS.toMillis(this.duration)+"ms average="+averageDurationMS+"ms");
				
				this.lastReportAt = System.currentTimeMillis();
			}
		}
	}
	
	public abstract void process(final Class<? extends Primitive> type, final Collection<T> items, final AbstractConnection connection) throws Throwable;
}
