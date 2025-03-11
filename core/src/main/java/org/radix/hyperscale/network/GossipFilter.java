package org.radix.hyperscale.network;

import java.util.Objects;
import java.util.Set;

import org.radix.hyperscale.Context;
import org.radix.hyperscale.common.Primitive;
import org.radix.hyperscale.ledger.ShardGroupID;

public abstract class GossipFilter<T extends Primitive>
{
	private final Context context;
	
	protected GossipFilter(Context context)
	{
		this.context = Objects.requireNonNull(context, "Context is null");
	}
	
	Context getContext()
	{
		return this.context;
	}
	
	public abstract Set<ShardGroupID> filter(T object);
}
