package org.radix.hyperscale.ledger.messages;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;

import org.radix.hyperscale.Constants;
import org.radix.hyperscale.Context;
import org.radix.hyperscale.common.Primitive;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.network.AbstractConnection;
import org.radix.hyperscale.network.MessageProcessor;
import org.radix.hyperscale.network.messages.InventoryMessage;

public abstract class SyncAcquiredMessageProcessor implements MessageProcessor<SyncAcquiredMessage>
{
	private final Context context;
	
	protected SyncAcquiredMessageProcessor(final Context context)
	{
		Objects.requireNonNull(context, "Context is null");
		
		this.context = context;
	}
	
	protected int broadcastSyncInventory(final Set<Hash> syncInventory, final Class<? extends Primitive> type, final int threshold, final AbstractConnection connection) throws IOException
	{
		if (syncInventory.size() < threshold)
			return 0;
		
		final int numItems = syncInventory.size();
		if (numItems == 0)
			return 0;

		int startItem = 0;
		while(startItem < numItems)
		{
			int endItem = startItem + Math.min(Constants.MAX_BROADCAST_INVENTORY_ITEMS, numItems - startItem);
			InventoryMessage inventoryMessage = new InventoryMessage(syncInventory, startItem, endItem, type);
			this.context.getNetwork().getMessaging().send(inventoryMessage, connection);
			startItem = endItem;
		}
		syncInventory.clear();
		return numItems;
	}
}
