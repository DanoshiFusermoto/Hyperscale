package org.radix.hyperscale.network;

import java.util.Objects;

import org.radix.hyperscale.network.messages.Message;

final class GossipEvent implements Comparable<GossipEvent>
{
	private final Message message;
	private final AbstractConnection connection;
	
	GossipEvent(final Message message, final AbstractConnection connection)
	{
		this.connection = Objects.requireNonNull(connection, "Primitive is null");
		this.message = Objects.requireNonNull(message, "Message is null");
	}
	
	@SuppressWarnings("unchecked")
	public <T extends Message> T getMessage()
	{
		return (T) this.message;
	}

	public AbstractConnection getConnection()
	{
		return this.connection;
	}

	@Override
	public int compareTo(final GossipEvent other)
	{
    	// Inspect urgency
    	boolean m1Urgent = this.getMessage().isUrgent();
    	boolean m2Urgent = other.getMessage().isUrgent();

        if (m1Urgent == true && m2Urgent == false)
            return -1;
        if (m1Urgent == false && m2Urgent == true)
            return 1;
        
        // Now inspect message priority
    	int m1Priority = this.getMessage().getPriority();
    	int m2Priority = other.getMessage().getPriority();
    	
    	if (m1Priority > m2Priority)
    		return -1;
    	
    	if (m1Priority < m2Priority)
    		return 1;
        
        // For same urgency and priority, older messages get higher priority
    	return Long.compare(this.getMessage().getTimestamp(), other.getMessage().getTimestamp());
	}
}
