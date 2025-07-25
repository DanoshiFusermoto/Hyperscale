package org.radix.hyperscale.network;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.AbstractMap;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.radix.hyperscale.Constants;
import org.radix.hyperscale.Context;
import org.radix.hyperscale.common.Direction;
import org.radix.hyperscale.crypto.bls12381.BLSPublicKey;
import org.radix.hyperscale.crypto.bls12381.BLSSignature;
import org.radix.hyperscale.crypto.ed25519.EDPublicKey;
import org.radix.hyperscale.exceptions.StartupException;
import org.radix.hyperscale.exceptions.TerminationException;
import org.radix.hyperscale.ledger.messages.SyncAcquiredMessage;
import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;
import org.radix.hyperscale.network.exceptions.SocketNotConnectedException;
import org.radix.hyperscale.network.messages.HandshakeMessage;
import org.radix.hyperscale.network.messages.Message;
import org.radix.hyperscale.network.messages.NodeMessage;
import org.radix.hyperscale.node.Node;
import org.radix.hyperscale.time.Time;
import org.radix.hyperscale.utils.MathUtils;

import io.netty.util.internal.ThreadLocalRandom;

public class Messaging
{
	private static final Logger messagingLog = Logging.getLogger("messaging");
	
	private final Map<Class<? extends Message>, Map<Class<?>, MessageProcessor>> listeners = Collections.synchronizedMap(new HashMap<Class<? extends Message>, Map<Class<?>, MessageProcessor>>());

	private final Context context;
	
	private AtomicLong sentTotal = new AtomicLong(0l);
	private AtomicLong receivedTotal = new AtomicLong(0l);
	private final Map<Class<?>, AtomicLong> received;
	private final Map<Class<?>, AtomicLong> sent;
	
	private final ScheduledExecutorService messageProcessor;

	public Messaging(final Context context)
	{ 
		this.context = Objects.requireNonNull(context);
		
		this.received = Collections.synchronizedMap(new HashMap<Class<?>, AtomicLong>());
		this.sent = Collections.synchronizedMap(new HashMap<Class<?>, AtomicLong>());

		// Using a dedicated message executor with a fixed thread pool in the range 2 - 2 + log2(cores)
		this.messageProcessor = Executors.newScheduledThreadPool(Math.max(2, 2 + MathUtils.log2(Runtime.getRuntime().availableProcessors())), new ThreadFactory() {
	        private final AtomicLong counter = new AtomicLong();
	        
	        @Override
	        public Thread newThread(final Runnable r) 
	        {
	            final Thread t = new Thread(r, Messaging.this.context.getName()+" Message Processor-"+this.counter.incrementAndGet());
	            t.setDaemon(false);
	            t.setPriority(Thread.NORM_PRIORITY + 1);
	            return t;
	        }
	    });
	}

	public void start() throws StartupException
	{
	}

	public void stop() throws TerminationException
	{
	}

	public void register(final Class<? extends Message> type, final Class<?> owner, final MessageProcessor listener)
	{
		Objects.requireNonNull(type, "Type class for registration is null");
		Objects.requireNonNull(owner, "Owner class for registration is null");
		Objects.requireNonNull(listener, "Listener for registration is null");
		
		synchronized(this.listeners)
		{
			if (this.listeners.containsKey(type) == false)
				this.listeners.put(type, Collections.synchronizedMap(new HashMap<Class<?>, MessageProcessor>()));

			if (this.listeners.get(type).containsKey(owner) == false)
				listeners.get(type).put(owner, listener);
		}
	}

	public void deregister(final MessageProcessor<? extends Message> listener)
	{
		Objects.requireNonNull(listener, "Listener for deregistration is null");

		synchronized(this.listeners)
		{
			for (Class<? extends Message> type : this.listeners.keySet())
			{
				Iterator<MessageProcessor> listenerIterator = this.listeners.get(type).values().iterator();
				while (listenerIterator.hasNext())
				{
					if (listenerIterator.next() == listener)
					{
						listenerIterator.remove();
						break;
					}
				}
			}
		}
	}

	public void deregisterAll(final Class<?> owner)
	{
		Objects.requireNonNull(owner, "Owner for blanket deregistration is null");

		synchronized(this.listeners)
		{
			for (Class<? extends Message> type : this.listeners.keySet())
			{
				Iterator<Class<?>> listenerOwnerIterator = this.listeners.get(type).keySet().iterator();
				while (listenerOwnerIterator.hasNext())
				{
					if (listenerOwnerIterator.next() == owner)
					{
						listenerOwnerIterator.remove();
						break;
					}
				}
			}
		}
	}

	public void send(final Message message, final AbstractConnection connection) throws IOException
	{
		Objects.requireNonNull(message, "Message is null");
		Objects.requireNonNull(connection, "Connection is null");

		if (messagingLog.hasLevel(Logging.DEBUG))
			messagingLog.debug(this.context.getName()+": Sending "+message+" to "+connection);

		if (connection.getState().equals(ConnectionState.DISCONNECTED) || connection.getState().equals(ConnectionState.DISCONNECTING))
			throw new SocketNotConnectedException(connection+" is "+connection.getState());

		message.setDirection(Direction.OUTBOUND);
			
		connection.send(message);
		
		this.sentTotal.incrementAndGet();
		this.sent.computeIfAbsent(message.getClass(), c -> new AtomicLong(0)).incrementAndGet();

		this.context.getMetaData().increment("messaging.outbound");
		this.context.getTimeSeries("messages").increment("outbound", 1, System.currentTimeMillis(), TimeUnit.MILLISECONDS);
	}
	
	// MESSAGE CALLBACKS //
	void onReceived(final Message message, final AbstractConnection connection) throws IOException
	{
		Objects.requireNonNull(message, "Message is null");
		Objects.requireNonNull(connection, "Connection is null");
		
		if (messagingLog.hasLevel(Logging.DEBUG))
			messagingLog.debug(Messaging.this.context.getName()+": Received "+message+" from "+connection);

		if (Time.getSystemTime() - message.witnessedAt() > TimeUnit.SECONDS.toMillis(this.context.getConfiguration().get("messaging.processing_latency_warn", Constants.DEFAULT_MESSAGE_PLW_SECONDS)))
			messagingLog.warn(this.context.getName()+": Inbound "+message+" with PLW of "+(Time.getSystemTime()-message.witnessedAt())+"ms from "+connection);

		if (Time.getSystemTime() - message.getTimestamp() > TimeUnit.SECONDS.toMillis(this.context.getConfiguration().get("messaging.time_to_live", Constants.DEFAULT_MESSAGE_TTL_SECONDS)))
		{
			messagingLog.warn(this.context.getName()+": Inbound "+message+" with expired TTL of "+(Time.getSystemTime()-message.getTimestamp())+"ms from "+connection);
			return;
		}
		
		if (Time.getSystemTime() - message.getTimestamp() > TimeUnit.SECONDS.toMillis(this.context.getConfiguration().get("messaging.transmit_latency_warn", Constants.DEFAULT_MESSAGE_TLW_SECONDS)))
			messagingLog.warn(this.context.getName()+": Inbound "+message+" with TLW of "+(Time.getSystemTime()-message.getTimestamp())+"ms from "+connection);

		// MUST send a HandshakeMessage first to establish handshake //
		// TODO what if its an OUTBOUND connection and the end point is not who we expect?
		if (connection.isHandshaked() == false)
		{
			if(message instanceof HandshakeMessage handshakeMessage)
			{
				Node node = handshakeMessage.getNode();
				if (node == null)
					throw new IOException("Didn't send node object in handshake");
	
				EDPublicKey ephemeralRemotePublicKey = handshakeMessage.getEphemeralKey();
				if (ephemeralRemotePublicKey == null)
					throw new IOException("Didn't send ephemeral public key in handshake");
	
				BLSSignature ephemeralBindingSignature = handshakeMessage.getBinding();
				if (ephemeralBindingSignature == null)
					throw new IOException("Didn't send BLS signature binding ephemeral public key to identity in handshake");
	
				if (node.getIdentity().<BLSPublicKey>getKey().verify(ephemeralRemotePublicKey.toByteArray(), ephemeralBindingSignature) == false)
					throw new IOException("BLS signature binding ephemeral public key to identity failed");
				
				connection.setNode(node);
				if (this.context.getNetwork().accept(connection) == false)
					return;
				
				connection.handshake(ephemeralRemotePublicKey);
			}
			else
				throw new IOException("Expected HandshakeMessage but received "+message);
		}

		if (message instanceof NodeMessage nodeMessage)
		{
			final Node node = nodeMessage.getNode();
			connection.setNode(node);
		}
		
		// Handle this here directly to ensure that any processors that might reference 
		// the node object know the sync status is updated
		if (message instanceof SyncAcquiredMessage syncAcquiredMessage)
		{
			connection.getNode().setHead(syncAcquiredMessage.getHead());
			connection.getNode().setSynced(true);
			
			if (messagingLog.hasLevel(Logging.DEBUG))
				messagingLog.debug(Messaging.this.context.getName()+": Received SyncAcquiredMessage with block header "+syncAcquiredMessage.getHead()+" for "+connection);
		}
		
		this.receivedTotal.incrementAndGet();
		this.received.computeIfAbsent(message.getClass(), c -> new AtomicLong(0)).incrementAndGet();
		
		connection.onReceived(message);
		
		// SIMULATED LATENCY //
		final int simulatedNetworkLatency = Messaging.this.context.getConfiguration().get("network.latency", 0);
		final int simulatedNetworkLatencyJitter;
		if (simulatedNetworkLatency > 0)
			simulatedNetworkLatencyJitter = ThreadLocalRandom.current().nextInt(Messaging.this.context.getConfiguration().get("network.latency.jitter", 0)) - (Messaging.this.context.getConfiguration().get("network.latency.jitter", 0)/2);
		else
			simulatedNetworkLatencyJitter = 0;

		// MESSAGING PROCESSING //
		final TransportParameters transportParameters = message.getClass().getAnnotation(TransportParameters.class);
		final Map<Class<?>, MessageProcessor> listeners = this.listeners.get(message.getClass());
		if (listeners != null)
		{
			synchronized (listeners)
			{
				for (final MessageProcessor listener : listeners.values())
				{
					final Runnable executor = new Runnable() 
					{
						@Override
						public void run()
						{
							try
							{
								listener.process(message, connection);
							}
							catch (Exception ex)
							{
								messagingLog.error(message+" from "+connection.getID()+" "+connection.getURI()+" failed", ex);
							}
						}
					};
					
					if (simulatedNetworkLatency + simulatedNetworkLatencyJitter == 0 && (transportParameters == null || transportParameters.async() == false))
						this.messageProcessor.submit(executor);
					else
						this.messageProcessor.schedule(executor, simulatedNetworkLatency + simulatedNetworkLatencyJitter, TimeUnit.MILLISECONDS);
				}
			}
		}
		
		this.context.getMetaData().increment("messaging.inbound");
		this.context.getTimeSeries("messages").increment("inbound", 1, System.currentTimeMillis(), TimeUnit.MILLISECONDS);
	}

	void onSent(final Message message, final AbstractConnection connection)
	{
		if (messagingLog.hasLevel(Logging.DEBUG))
			messagingLog.debug(Messaging.this.context.getName()+": Sent "+message+" to "+connection);

		if (Time.getSystemTime() - message.witnessedAt() > TimeUnit.SECONDS.toMillis(this.context.getConfiguration().get("messaging.processing_latency_warn", Constants.DEFAULT_MESSAGE_PLW_SECONDS)))
			messagingLog.warn(this.context.getName()+": Outbound "+message+" with PLW of "+(Time.getSystemTime()-message.witnessedAt())+"ms to "+connection);

		if (Time.getSystemTime() - message.getTimestamp() > TimeUnit.SECONDS.toMillis(this.context.getConfiguration().get("messaging.time_to_live", Constants.DEFAULT_MESSAGE_TTL_SECONDS)))
			messagingLog.warn(this.context.getName()+": Outbound "+message+" with expired TTL of "+(Time.getSystemTime()-message.getTimestamp())+"ms to "+connection);
		
		if (Time.getSystemTime() - message.getTimestamp() > TimeUnit.SECONDS.toMillis(this.context.getConfiguration().get("messaging.transmit_latency_warn", Constants.DEFAULT_MESSAGE_TLW_SECONDS)))
			messagingLog.warn(this.context.getName()+": Outbound "+message+" with TLW of "+(Time.getSystemTime()-message.getTimestamp())+"ms to "+connection);
		
		connection.onSent(message);
	}

	public long getTotalSent()
	{
		return this.sentTotal.get();
	}

	public long getTotalReceived()
	{
		return this.receivedTotal.get();
	}
	
	public Collection<Entry<Class<?>, Long>> getReceivedByType()
	{
		synchronized(this.received)
		{
			return this.received.entrySet().stream().map(e -> new AbstractMap.SimpleEntry<Class<?>, Long>(e.getKey(), e.getValue().get())).collect(Collectors.toList());
		}
	}

	public Collection<Entry<Class<?>, Long>> getSentByType()
	{
		synchronized(this.sent)
		{
			return this.sent.entrySet().stream().map(e -> new AbstractMap.SimpleEntry<Class<?>, Long>(e.getKey(), e.getValue().get())).collect(Collectors.toList());
		}
	}
}
