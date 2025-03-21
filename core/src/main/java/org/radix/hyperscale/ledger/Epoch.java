package org.radix.hyperscale.ledger;

import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.utils.Numbers;

import com.google.common.primitives.Longs;

public final class Epoch
{
	/*
	 * Basic memory light Epoch cache.  Frequent identical Epochs are generated. 
	 * Re-using these created instances where possible, even if minimal, reduces load on the garbage collector "significantly".  
	 * 
	 * TODO improvements?
	 */
	private static final int EPOCH_CACHE_SIZE = 1<<10;
	private static final Epoch[] cache = new Epoch[EPOCH_CACHE_SIZE];
	private static final Epoch compute(final long clock)
	{
		final int index = (int) (clock % EPOCH_CACHE_SIZE);
		synchronized(cache)
		{
			Epoch epoch = cache[index];
			if (epoch == null)
			{
				epoch = new Epoch(clock);
				cache[index] = epoch;
			}
			else
			{
				if (epoch.clock == clock)
					return epoch;
				else
				{
					epoch = new Epoch(clock);
					cache[index] = epoch;
				}
			}
			
			return epoch;
		}
	}
	
	public static final Epoch from(final Hash proposal)
	{
		final byte[] hashBytes = proposal.toByteArray();
		if (hashBytes[0] != 0 || hashBytes[1] != 0)
			throw new IllegalArgumentException("Proposal has is invalid");
		
		final long height = Longs.fromByteArray(proposal.toByteArray());
		return from(height / Ledger.definitions().proposalsPerEpoch());
	}

	public static final Epoch from(final BlockHeader header)
	{
		return from(header.getHeight() / Ledger.definitions().proposalsPerEpoch());
	}

	public static final Epoch from(final long clock)
	{
		return compute(clock);
	}

	private final long clock;
	
	private Epoch(final long clock)
	{
		Numbers.isNegative(clock, "Epock clock is negative");
		this.clock = clock;
	}

	public long getClock()
	{
		return this.clock;
	}
	
	public Epoch increment()
	{
		return new Epoch(this.clock + 1l);
	}

	public Epoch increment(int ticks)
	{
		return new Epoch(this.clock + ticks);
	}

	public Epoch decrement()
	{
		return new Epoch(this.clock - 1l);
	}

	public Epoch decrement(int ticks)
	{
		return new Epoch(this.clock - ticks);
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (this.clock ^ (this.clock >>> 32));
		return result;
	}

	@Override
	public boolean equals(final Object object)
	{
		if (this == object)
			return true;
		
		if (object == null)
			return false;
		
		if (object instanceof Epoch epoch)
		{
			if (this.clock != epoch.clock)
				return false;
			
			return true;
		}
		
		return false;
	}

	@Override
	public String toString()
	{
		return "Epoch [clock=" + this.clock + "]";
	}
}
