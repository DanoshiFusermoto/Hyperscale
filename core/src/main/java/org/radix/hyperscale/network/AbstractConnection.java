package org.radix.hyperscale.network;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.radix.hyperscale.Constants;
import org.radix.hyperscale.Context;
import org.radix.hyperscale.common.Agent;
import org.radix.hyperscale.common.Direction;
import org.radix.hyperscale.crypto.bls12381.BLSSignature;
import org.radix.hyperscale.crypto.ed25519.EDKeyPair;
import org.radix.hyperscale.crypto.ed25519.EDPublicKey;
import org.radix.hyperscale.exceptions.QueueFullException;
import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;
import org.radix.hyperscale.network.events.BannedEvent;
import org.radix.hyperscale.network.events.ConnectedEvent;
import org.radix.hyperscale.network.events.ConnectingEvent;
import org.radix.hyperscale.network.events.DisconnectedEvent;
import org.radix.hyperscale.network.exceptions.BanException;
import org.radix.hyperscale.network.exceptions.SocketNotConnectedException;
import org.radix.hyperscale.network.messages.HandshakeMessage;
import org.radix.hyperscale.network.messages.Message;
import org.radix.hyperscale.node.Node;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.Serializable;
import org.radix.hyperscale.serialization.SerializerId2;
import org.radix.hyperscale.serialization.DsonOutput.Output;
import org.radix.hyperscale.time.Time;
import org.radix.hyperscale.utils.MathUtils;
import org.radix.hyperscale.utils.Numbers;
import org.radix.hyperscale.utils.TimeSeriesStatistics;
import org.radix.hyperscale.utils.URIs;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("network.connection")
public abstract class AbstractConnection extends Serializable implements Comparable<AbstractConnection>
{	
	private static final Logger messagingLog = Logging.getLogger("messaging");
	private static final Logger networkLog = Logging.getLogger("network");

	private static final int DEFAULT_BANTIME_SECONDS = 60 * 60;
	
	private static boolean bufferWarning = false;
	
	private Thread inboundThread = null;
	private TCPInboundProcessor inboundProcessor = null;
	private class TCPInboundProcessor implements Runnable
	{
		@Override
		public void run ()
		{
			Message message = null;

			final DataInputStream dataInputStream;
			final BufferedInputStream bufferedInputStream;
			try
			{
				bufferedInputStream = new BufferedInputStream(AbstractConnection.this.inputStream, 1<<14);
				dataInputStream = new DataInputStream(bufferedInputStream);

				if (AbstractConnection.this.direction.equals(Direction.INBOUND))
					doHandshake();
	
				while (AbstractConnection.this.socket.isConnected() && 
					   AbstractConnection.this.getState().equals(ConnectionState.DISCONNECTING) == false && 
					   AbstractConnection.this.getState().equals(ConnectionState.DISCONNECTED) == false)
				{
					try
					{
						message = Message.inbound(dataInputStream, AbstractConnection.this);

						AbstractConnection.this.totalIngress.addAndGet(message.getSize());
						AbstractConnection.this.context.getMetaData().increment("network.transferred.inbound", message.getSize());

						AbstractConnection.this.timeseries.increment("inbound", message.getSize(), System.currentTimeMillis(), TimeUnit.MILLISECONDS);
						AbstractConnection.this.context.getTimeSeries("bandwidth").increment("inbound", message.getSize(), System.currentTimeMillis(), TimeUnit.MILLISECONDS);
					}
					catch(EOFException eofex)
					{
						disconnect(null, null);
						return;
					}
					catch(IOException ioex)
					{
						disconnect(ioex.getMessage(), ioex);
						return;
					}
					catch(BanException bex)
					{
						ban(bex.getMessage(), 60, TimeUnit.SECONDS);  // TODO increase this!
						return;
					}
					catch (Exception ex)
					{
						disconnect("Exception in message parsing", ex);
						return;
					}
					
					try
					{
						AbstractConnection.this.context.getNetwork().getMessaging().received(message, AbstractConnection.this);
					}
					catch(Exception ex)
					{
						messagingLog.error(AbstractConnection.this.context.getName()+": Message processing error for "+message+" on "+AbstractConnection.this, ex);
						disconnect("Exception in message processing", ex);
						return;
					}
				}
				
				if (networkLog.hasLevel(Logging.DEBUG))
					networkLog.debug("TCPProcessor thread "+AbstractConnection.this.inboundThread.getName()+" is quitting");
			}
			catch (Throwable t)
			{
				networkLog.error("TCPProcessor thread "+AbstractConnection.this.inboundThread.getName()+" threw uncaught", t);
			}
		}
	}

	private Thread outboundThread = null;
	private TCPOutboundProcessor outboundProcessor = null;
	private class TCPOutboundProcessor implements Runnable
	{
		private long lastFlush = 0;
		private boolean requiresFlush = true;
		private final DataOutputStream dataOutputStream;
		private final BlockingQueue<Message> outboundQueue;
		
		TCPOutboundProcessor()
		{
			BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(AbstractConnection.this.outputStream, 1<<14);
			this.dataOutputStream = new DataOutputStream(bufferedOutputStream);
			this.outboundQueue = new ArrayBlockingQueue<Message>(AbstractConnection.this.context.getConfiguration().get("messaging.outbound.queue_max", 1<<12));
		}
		
		@Override
		public void run()
		{
			try
			{
				while (AbstractConnection.this.socket.isConnected() && 
					   AbstractConnection.this.getState().equals(ConnectionState.DISCONNECTING) == false && 
					   AbstractConnection.this.getState().equals(ConnectionState.DISCONNECTED) == false)
				{
					// Reset strikes
					if (AbstractConnection.this.strikes > 0 && System.currentTimeMillis() > AbstractConnection.this.strikeResetAt)
					{
						AbstractConnection.this.strikes--;
						if (AbstractConnection.this.strikes > 0)
							AbstractConnection.this.strikeResetAt = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(1);
						else
							AbstractConnection.this.strikeResetAt = Long.MAX_VALUE;
					}
					
					// Get the batch of messages to send in priority order
					final List<Message> prioritized = getPrioritizedForDispatch(Constants.QUEUE_POLL_TIMEOUT, TimeUnit.MILLISECONDS);
					
					// Check if a flush is required before processing
					flush();
					
					if (prioritized.isEmpty() == false)
					{
						for(final Message message : prioritized)
						{
							try
							{
								Message.outbound(message, this.dataOutputStream, AbstractConnection.this);
								this.requiresFlush = true;
								
								AbstractConnection.this.totalEgress.addAndGet(message.getSize());
								AbstractConnection.this.context.getMetaData().increment("network.transferred.outbound", message.getSize());
								
								AbstractConnection.this.timeseries.increment("outbound", message.getSize(), System.currentTimeMillis(), TimeUnit.MILLISECONDS);
								AbstractConnection.this.context.getTimeSeries("bandwidth").increment("outbound", message.getSize(), System.currentTimeMillis(), TimeUnit.MILLISECONDS);
							}
							catch (Exception ex)
							{
								disconnect("Exception in message sending", ex);
								return;
							}
							
							flush();
						}
					}
				}
				
				if (networkLog.hasLevel(Logging.DEBUG))
					networkLog.debug("TCPProcessor thread "+AbstractConnection.this.outboundThread.getName()+" is quitting");
			}
			catch (Throwable t)
			{
				networkLog.error("TCPProcessor thread "+AbstractConnection.this.outboundThread.getName()+" threw uncaught "+t);
			}
		}
		
		private void flush() throws IOException
		{
			if (this.requiresFlush == true && System.currentTimeMillis() > this.lastFlush + Constants.QUEUE_POLL_TIMEOUT)
			{
				this.dataOutputStream.flush();
				this.requiresFlush = false;
				this.lastFlush = System.currentTimeMillis();
			}
		}
		
		private List<Message> getPrioritizedForDispatch(final long timeout, final TimeUnit timeUnit)
		{
			Message message;
			try 
			{
				message = this.outboundQueue.poll(timeout, timeUnit);
			} 
			catch (InterruptedException ex) 
			{
				Thread.currentThread().interrupt();
				messagingLog.warn(AbstractConnection.this.context.getName()+": Message outbound processing was interrupted for "+AbstractConnection.this, ex);
				return Collections.emptyList();
			}
			
			if (message == null)
				return Collections.emptyList();
			
			final int numDispatchItems = Math.max(this.outboundQueue.size()+1, Constants.MAX_REQUEST_INVENTORY_ITEMS_TOTAL);
			final List<Message> messages = new ArrayList<>(numDispatchItems);
			messages.add(message);
			this.outboundQueue.drainTo(messages, numDispatchItems-1);
			
			Collections.sort(messages);
			
			return messages;
		}
	}

	// TODO temporary, substituting equals for some cases where Node/URI equals() is insufficient
	private final int id = ThreadLocalRandom.current().nextInt();

	private final Context context;
	private final Socket socket;
	private volatile InputStream inputStream;
	private volatile OutputStream outputStream;
	
	private volatile URI 	uri;
	private volatile Node 	node;
	
	private final Direction direction;
	private volatile boolean stale = false;
	private final Semaphore	handshake = new Semaphore(2);
	private volatile ConnectionState state = ConnectionState.NONE;

	private final EDKeyPair ephemeralLocalKeyPair;
	private final BLSSignature ephemeralKeyBinding;
	private volatile EDPublicKey ephemeralRemotePublicKey;

	private long connectedAt;
	private long connectingAt;
	private long disconnectedAt;
	private long shuffleAt;

	private long bannedUntil;
	private String banReason = null;	

	private volatile int strikes;
	private volatile long strikeResetAt = Long.MAX_VALUE;

	private final AtomicInteger latency = new AtomicInteger(1000);
	private final AtomicInteger timeout = new AtomicInteger(1000);
	
	private volatile int maximumRequestQuota = Constants.MAX_REQUEST_INVENTORY_ITEMS_TOTAL;
	private final AtomicInteger pendingRequests = new AtomicInteger();
	private final AtomicInteger pendingRequested = new AtomicInteger();
	private final AtomicInteger pendingWeight = new AtomicInteger();
	private final AtomicInteger totalRequests = new AtomicInteger();
	private final AtomicInteger totalRequested = new AtomicInteger();
	private final AtomicInteger totalEgress = new AtomicInteger();
	private final AtomicInteger totalIngress = new AtomicInteger();
	private final TimeSeriesStatistics timeseries;
	
	AbstractConnection(final Context context, final URI uri) throws IOException 
	{
		Objects.requireNonNull(uri, "URI is null");
		Objects.requireNonNull(uri.getHost(), "URI doesn't have a host");
		Numbers.isZero(uri.getPort(), "URI doesn't have a port");

		this.context = Objects.requireNonNull(context, "Context is null");

		this.uri = uri;
		this.direction = Direction.OUTBOUND;

		this.ephemeralLocalKeyPair = context.getNode().getEphemeralKeyPair();
		this.ephemeralKeyBinding = context.getNode().getEphemeralBinding();
		
		this.timeseries = new TimeSeriesStatistics(30, TimeUnit.SECONDS);

		try
		{
			onConnecting();
			
			this.socket = new Socket();
			this.socket.setSoTimeout(60000);
			this.socket.setTcpNoDelay(true);
			this.socket.setKeepAlive(true);
			this.socket.setReceiveBufferSize(this.context.getConfiguration().get("network.tcp.buffer", Constants.DEFAULT_TCP_BUFFER));
			this.socket.setSendBufferSize(this.context.getConfiguration().get("network.tcp.buffer", Constants.DEFAULT_TCP_BUFFER));
			this.socket.connect(new InetSocketAddress(uri.getHost(), uri.getPort()), (int) TimeUnit.SECONDS.toMillis(this.context.getConfiguration().get("network.peer.connect.timeout", 10)));

			networkLog.info(this.context.getName()+": OUTBOUND connection opened to "+this.id+" "+uri);
   			networkLog.info(this.context.getName()+":    TCP server socket "+this.socket.getLocalSocketAddress()+" RCV_BUF: "+this.socket.getReceiveBufferSize()+" SND_BUF: "+this.socket.getSendBufferSize());
   			
   			checkBuffersAndWarn();

   			listen();
		}
		catch (Exception ex) 
		{
			disconnect(ex.getMessage(), ex);
			throw new IOException("OUTBOUND connection to "+this.id+" "+uri+" failed", ex);
		}
	}

	AbstractConnection(final Context context, final Socket socket) throws IOException 
	{
		Objects.requireNonNull(socket, "Socket is null");

		this.context = Objects.requireNonNull(context, "Context is null");

		this.direction = Direction.INBOUND;
		this.uri = Agent.getURI(socket.getRemoteSocketAddress());

		this.ephemeralLocalKeyPair = context.getNode().getEphemeralKeyPair();
		this.ephemeralKeyBinding = context.getNode().getEphemeralBinding();
		
		this.timeseries = new TimeSeriesStatistics(30, TimeUnit.SECONDS);

		networkLog.info(this.context.getName()+": INBOUND connection from "+this.id+" "+getURI().getHost());

		try
		{
			onConnecting();
			
			this.socket = socket;
			this.socket.setSoTimeout(60000);
			this.socket.setTcpNoDelay(true);
			this.socket.setKeepAlive(true);
			this.socket.setReceiveBufferSize(this.context.getConfiguration().get("network.tcp.buffer", Constants.DEFAULT_TCP_BUFFER));
			this.socket.setSendBufferSize(this.context.getConfiguration().get("network.tcp.buffer", Constants.DEFAULT_TCP_BUFFER));

			networkLog.info(this.context.getName()+":    TCP server socket "+this.socket.getLocalSocketAddress()+" RCV_BUF: "+this.socket.getReceiveBufferSize()+" SND_BUF: "+this.socket.getSendBufferSize());
   			checkBuffersAndWarn();

			listen();
		} 
		catch (Exception ex) 
		{
			disconnect(ex.getMessage());
			throw new IOException("INBOUND connection from "+this.id+" "+getURI().getHost()+" failed", ex);
		}
	}
	
	private final void checkBuffersAndWarn() throws SocketException
	{
		if (AbstractConnection.bufferWarning == false)
		{
			if (this.socket.getReceiveBufferSize() < this.context.getConfiguration().get("network.tcp.buffer", Constants.DEFAULT_TCP_BUFFER) || 
				this.socket.getSendBufferSize() < this.context.getConfiguration().get("network.tcp.buffer", Constants.DEFAULT_TCP_BUFFER))
			{
				System.err.println("WARNING: Connection send / receive buffers not set according to 'network.tcp.buffer' of "+this.context.getConfiguration().get("network.tcp.buffer", Constants.DEFAULT_TCP_BUFFER)+" bytes");
				System.err.println("           SND_BUF = "+this.socket.getSendBufferSize()+" RCV_BUF = "+this.socket.getReceiveBufferSize());
				System.err.println("         Performance of connectivity may be sub-par and may need OS level re-configuration to allow larger buffers");
			
				AbstractConnection.bufferWarning = true;
			}
		}
	}
	
	private final void listen() throws IOException
	{
		networkLog.info(this.context.getName()+": TCP client socket "+this.socket.getLocalSocketAddress()+" TIMEOUT: "+this.socket.getSoTimeout()+" NO_DELAY: "+this.socket.getTcpNoDelay()+" SND_BUF: "+this.socket.getSendBufferSize()+" RCV_BUF: "+this.socket.getReceiveBufferSize());
		
		this.inputStream = this.socket.getInputStream();
		this.outputStream = this.socket.getOutputStream();
		
		this.outboundProcessor = new TCPOutboundProcessor();
		this.outboundThread = new Thread(this.outboundProcessor);
		this.outboundThread.setDaemon(true);
		this.outboundThread.setName(this.context.getName()+" Peer-"+this.socket.getInetAddress()+":"+this.socket.getLocalPort()+"-TCP-OUT");
		this.outboundThread.setPriority(7);
		this.outboundThread.start();

		this.inboundProcessor = new TCPInboundProcessor();
		this.inboundThread = new Thread(this.inboundProcessor);
		this.inboundThread.setDaemon(true);
		this.inboundThread.setName(this.context.getName()+" Peer-"+this.socket.getInetAddress()+":"+this.socket.getLocalPort()+"-TCP-IN");
		this.inboundThread.setPriority(7);
		this.inboundThread.start();
	}

	public int getID()
	{
		return this.id;
	}
	
	public URI getURI()
	{
		return this.uri;
	}

	@JsonProperty("node")
	@DsonOutput(Output.API)
	public Node getNode()
	{
		return this.node;
	}

	public void setNode(final Node node)
	{
		this.node = node;
		this.uri = Agent.getURI(this.uri.getHost(), node);
	}
	
	@JsonProperty("host")
	@DsonOutput(Output.API)
	public URI getHost()
	{
		return URIs.toHostAndPort(this.uri);
	}

	@JsonProperty("direction")
	@DsonOutput(Output.API)
	public final Direction getDirection()
	{
		return this.direction;
	}
	
	@JsonProperty("protocol")
	@DsonOutput(Output.API)
	public abstract Protocol getProtocol();
	
	@Override
	// TODO violates expected equals contract
	public final boolean equals(Object object)
	{
		if (object == null) 
			return false;
		
		if (object == this) 
			return true;

		if (object instanceof AbstractConnection abstractConnection)
		{
			if (getID() != abstractConnection.getID())
				return false;
			
			return true;
		}

		return false;
	}

	@Override
	// TODO violates expected hashcode contract
	public final int hashCode()
	{
		return this.id;
	}
	
	// CONNECTIVITY //
	private void onConnecting()
	{
		 setState(ConnectionState.CONNECTING);
		 setConnectingAt(Time.getSystemTime());
		 this.context.getEvents().post(new ConnectingEvent(this));
	}
	 
	private void onConnected()
	{
		setState(ConnectionState.CONNECTED);
		setConnectedAt(Time.getSystemTime());
		this.context.getEvents().post(new ConnectedEvent(this));
	}

	public final boolean isHandshaked()
	{
		return this.handshake.availablePermits() == 0;
	}
	
	final void doHandshake() throws IOException
	{
		if (this.handshake.tryAcquire() == false)
			throw new IllegalStateException("Handshake already performed!");

		send(new HandshakeMessage(this.context.getNode(), this.ephemeralLocalKeyPair.getPublicKey(), this.ephemeralKeyBinding));
		networkLog.info(this.context.getName()+": Sent handshake to "+this);
	}

	public final void handshake(final EDPublicKey ephemeralRemotePublicKey) throws IOException
	{
		Objects.requireNonNull(ephemeralRemotePublicKey, "Ephemeral remote public key is null");
		
		if (this.handshake.tryAcquire() == false)
			throw new IllegalStateException("Handshake already performed!");
		
		this.ephemeralRemotePublicKey = ephemeralRemotePublicKey;
		networkLog.info(this.context.getName()+": Ephemeral keys set as "+this.ephemeralLocalKeyPair.getPublicKey()+" <> "+this.ephemeralRemotePublicKey+" for "+this);
		
		if (this.direction.equals(Direction.OUTBOUND))
			doHandshake();

		onConnected();
	}
	
	public final void ban(final String reason) throws IOException
	{
		ban(reason, DEFAULT_BANTIME_SECONDS, TimeUnit.SECONDS);
	}

	public final void ban(final String reason, final long duration, final TimeUnit unit) throws IOException
	{
		networkLog.info(this.context.getName()+": "+toString()+" - Banned for "+unit.toSeconds(duration)+" seconds due to "+reason);

		setBanReason(reason);
		setBannedUntil(Time.getSystemTime()+unit.toMillis(duration));
	
		disconnect(reason);

		this.context.getEvents().post(new BannedEvent(this));
	}
	
	public final void strikeOrDisconnect(final String reason) throws IOException
	{
		this.strikes++;
		if (this.strikes == Constants.MAX_STRIKES_FOR_DISCONNECT)
			disconnect(reason);
		else
		{
			networkLog.error(this.context.getName()+": "+toString()+" - Received a strike "+this.strikes+"/"+Constants.MAX_STRIKES_FOR_DISCONNECT+" - "+reason);
			this.strikeResetAt = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(1);
		}
	}
	
	public final void disconnect(final String reason) throws IOException
	{
		disconnect(reason, null);
	}

	public final void disconnect(final String reason, final Throwable throwable) throws IOException
	{
		if (getState().equals(ConnectionState.DISCONNECTING) || getState().equals(ConnectionState.DISCONNECTED))
			return;

		try
		{
			setState(ConnectionState.DISCONNECTING);
			
			if (reason != null)
			{
				// EOFExceptions on disconnect should be ignored
				if (throwable != null && EOFException.class.isAssignableFrom(throwable.getClass()) == false)
				{
					if (networkLog.hasLevel(Logging.DEBUG))
						networkLog.error(this.context.getName()+": "+toString()+" - Disconnected - "+reason, throwable);
					else if (reason.equalsIgnoreCase(throwable.getMessage()) == false)
						networkLog.error(this.context.getName()+": "+toString()+" - Disconnected - "+reason+" - "+throwable.getMessage());
					else
						networkLog.error(this.context.getName()+": "+toString()+" - Disconnected - "+reason);
				}
				else
					networkLog.error(this.context.getName()+": "+toString()+" - Disconnected - "+reason);
			}
			else
			{
				// EOFExceptions on disconnect should be ignored
				if (throwable != null && EOFException.class.isAssignableFrom(throwable.getClass()) == false)
				{
					if (networkLog.hasLevel(Logging.DEBUG))
						networkLog.error(this.context.getName()+": "+toString()+" - Disconnected - ", throwable);
					else if (throwable.getMessage() != null)
						networkLog.error(this.context.getName()+": "+toString()+" - Disconnected - "+throwable.getMessage());
					else
						networkLog.error(this.context.getName()+": "+toString()+" - Disconnected - "+throwable.getClass());
				}
				else
					networkLog.info(this.context.getName()+": "+toString()+" - Disconnected");
			}
		}
		catch(Exception e)
		{
			networkLog.error("Exception in disconnect of "+this.context.getName()+": "+toString(), e);
		}
		finally
		{
			if (this.socket != null && this.socket.isClosed() == false)
				this.socket.close();

			onDisconnected(throwable);
		}
	}

	private void onDisconnected(final Throwable throwable)
	{
		setState(ConnectionState.DISCONNECTED);
		setDisconnectedAt(Time.getSystemTime());
		this.context.getEvents().post(new DisconnectedEvent(this, throwable));
	}
	
	public abstract boolean requiresSignatures();

	public final EDKeyPair getEphemeralLocalKeyPair()
	{
		return this.ephemeralLocalKeyPair;
	}
	
	final BLSSignature getEphemeralKeyBinding()
	{
		return this.ephemeralKeyBinding;
	}

	public final EDPublicKey getEphemeralRemotePublicKey()
	{
		return this.ephemeralRemotePublicKey;
	}
	
	void receive(final Message message)
	{
		Objects.requireNonNull(message, "Message is null");
	}

	void send(final Message message) throws IOException
	{
		Objects.requireNonNull(message, "Message is null");

		if (this.socket.isConnected() == false)
			throw new SocketNotConnectedException("Socket not connected "+this);
		
		try
		{
			if (this.outboundProcessor.outboundQueue.offer(message) == false)
			{
				messagingLog.warn(message+": Outbound queue is full:waiting "+this);
				
				if (this.outboundProcessor.outboundQueue.offer(message, 1, TimeUnit.SECONDS) == false)
					throw new QueueFullException(message+": Outbound queue is full");
			}
		} 
		catch (InterruptedException ex) 
		{
			messagingLog.error(message+": Sending to "+this+" failed", ex);
			
			// Not going to handle it here.
			Thread.currentThread().interrupt();
			throw new IOException("While sending message", ex);
		} 
		catch (Exception ex) 
		{
			messagingLog.error(message+": Sending to "+this+" failed", ex);
			throw new IOException(ex);
		}
	}

	// STATE //
	public final ConnectionState getState()
	{
		return this.state;
	}

	public final void setState(final ConnectionState state)
	{
		this.state = state;
	}
	
	public final boolean isStale()
	{
		return this.stale;
	}

	public final void setStale()
	{
		this.stale = true;
	}

	public final void resetStale()
	{
		this.stale = false;
	}

	@JsonProperty("statistics")
	@DsonOutput(Output.API)
	public final Map<String, Object> getStatistics()
	{
		final Map<String, Object> statistics = new HashMap<String, Object>(8);
		statistics.put("timeout", this.timeout.get());
		statistics.put("requests", Map.of("total", this.totalRequests, "pending", this.pendingRequests, "recent", (int) this.timeseries.average("requests", System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(10), TimeUnit.MILLISECONDS)));
		statistics.put("requested", Map.of("total", this.totalRequested, "pending", this.pendingRequested, "recent", (int) this.timeseries.average("requested", System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(10), TimeUnit.MILLISECONDS)));
		statistics.put("transferred", Map.of("egress", this.totalEgress, "ingress", this.totalIngress));
		statistics.put("throughput", Map.of("egress", (int) this.timeseries.average("outbound", System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(30), TimeUnit.MILLISECONDS), 
											"ingress", (int) this.timeseries.average("inbound", System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(30), TimeUnit.MILLISECONDS)));
		return statistics;
	}

	@JsonProperty("strikes")
	@DsonOutput(Output.API)
	private int getStrikes()
	{
		return this.strikes;
	}
	
	@JsonProperty("latency")
	@DsonOutput(Output.API)
	public final int getLatency()
	{
		return this.latency.intValue();
	}
	
	public final void updateLatency(final long latency)
	{
		Numbers.isNegative(latency, "Latency is negative");
		this.latency.updateAndGet(v -> (int) MathUtils.EWMA(v, latency, 0.1));
	}

	// REQUESTS //
	public final int pendingRequests()
	{
		return this.pendingRequests.intValue();
	}

	public final int incrementPendingRequests()
	{
		return this.pendingRequests.incrementAndGet();
	}

	public final int decrementPendingRequests()
	{
		int result = this.pendingRequests.decrementAndGet();
		if (result < 0)
		{
			this.pendingRequests.set(0);
			throw new IllegalStateException("Pending requests is negative "+result);
		}
		return result;
	}

	public final int pendingRequested()
	{
		return this.pendingRequested.intValue();
	}

	public final int incrementPendingRequested()
	{
		return incrementPendingRequested(1);
	}

	public final int incrementPendingRequested(int requested)
	{
		return this.pendingRequested.addAndGet(requested);
	}

	public final int decrementPendingRequested()
	{
		return decrementPendingRequested(1);
	}

	public final int decrementPendingRequested(int requested)
	{
		int result = this.pendingRequested.addAndGet(-requested);
		if (result < 0)
		{
			this.pendingRequested.set(0);
			throw new IllegalStateException("Pending requested is negative "+result);
		}
		return result;
	}

	public final int pendingWeight()
	{
		return this.pendingWeight.intValue();
	}

	public final int incrementPendingWeight()
	{
		return incrementPendingWeight(1);
	}

	public final int incrementPendingWeight(int weight)
	{
		return this.pendingWeight.addAndGet(weight);
	}

	public final int decrementPendingWeight()
	{
		return decrementPendingWeight(1);
	}

	public final int decrementPendingWeight(int weight)
	{
		int result = this.pendingWeight.addAndGet(-weight);
		if (result < 0)
		{
			this.pendingWeight.set(0);
			throw new IllegalStateException("Pending weight is negative "+result);
		}
		return result;
	}

	public final long totalRequests()
	{
		return this.totalRequests.intValue();
	}

	public final long totalRequested()
	{
		return this.totalRequested.intValue();
	}
	
	public int availableRequestQuota()
	{
		return this.maximumRequestQuota - (pendingRequests() + pendingWeight());
	}
	
	public int allocatedRequestQuota()
	{
		return pendingRequests() + pendingWeight();
	}

	public long getNextTimeout(int requestWeight, TimeUnit timeUnit)
	{
		final long baselineTimeout = Constants.MIN_GOSSIP_REQUEST_TIMEOUT_MILLISECONDS;
		final long sqrtBaselineTimeout = (long) Math.sqrt(baselineTimeout);
		final long timeoutWeightAllowance = sqrtBaselineTimeout * pendingWeight(); // Additional allowance for outstanding requests weight
		final long timeoutRequestAllowance = Math.max(sqrtBaselineTimeout, getLatency()) * pendingRequests(); // Additional allowance for outstanding requests
		final long gossipRequestTimeout = baselineTimeout + timeoutRequestAllowance + timeoutWeightAllowance + (sqrtBaselineTimeout * requestWeight); // Sum with additional allowance for request size
		this.timeout.updateAndGet(v -> (int) MathUtils.EWMA(v, gossipRequestTimeout, 0.95));
		return timeUnit.convert(gossipRequestTimeout, TimeUnit.MILLISECONDS);
	}

	public final int numRequests(int duration, TimeUnit unit)
	{
		return (int) this.timeseries.sum("requests", System.currentTimeMillis() - unit.toMillis(duration), System.currentTimeMillis(), TimeUnit.MILLISECONDS);
	}

	public final int numRequested(int duration, TimeUnit unit)
	{
		return (int) this.timeseries.sum("requested", System.currentTimeMillis() - unit.toMillis(duration), System.currentTimeMillis(), TimeUnit.MILLISECONDS);
	}

	public final void updateRequests(int increment)
	{
		this.totalRequests.addAndGet(increment);
		this.timeseries.increment("requests", increment, System.currentTimeMillis(), TimeUnit.MILLISECONDS);
	}

	public final void updateRequested(int increment)
	{
		this.totalRequested.addAndGet(increment);
		this.timeseries.increment("requested", increment, System.currentTimeMillis(), TimeUnit.MILLISECONDS);
	}

	// TIMESTAMPS
	@JsonProperty("connected_at")
	@DsonOutput(Output.API)
	public long getConnectedAt()
	{
		return this.connectedAt;
	}

	void setConnectedAt(final long timestamp)
	{
		this.connectedAt = timestamp;
		
		final long shuffleRandom = TimeUnit.MILLISECONDS.convert(ThreadLocalRandom.current().nextInt(Constants.DEFAULT_CONNECTION_STICKY_DURATION_SECONDS), TimeUnit.SECONDS); 
		this.shuffleAt = this.connectedAt + TimeUnit.MILLISECONDS.convert(Constants.DEFAULT_CONNECTION_STICKY_DURATION_SECONDS, TimeUnit.SECONDS) + shuffleRandom;
	}
	
	public long getConnectingAt()
	{
		return this.connectingAt;
	}

	void setConnectingAt(final long timestamp)
	{
		this.connectingAt = timestamp;
	}

	public long getDisconnectedAt()
	{
		return this.disconnectedAt;
	}

	void setDisconnectedAt(final long timestamp)
	{
		this.disconnectedAt = timestamp;
	}
	
	@JsonProperty("shuffle_at")
	@DsonOutput(Output.API)
	public long getShuffleAt()
	{
		return this.shuffleAt;
	}

	// BANS //
	public String getBanReason()
	{
		return this.banReason;
	}

	void setBanReason(final String banReason)
	{
		this.banReason = banReason;
	}
	
	public long getBannedUntil()
	{
		return this.bannedUntil;
	}
	
	void setBannedUntil(final long bannedUntil)
	{
		this.bannedUntil = bannedUntil;
	}

	public String toString()
	{
		return this.id+" "+getProtocol()+" "+(getNode() != null && getNode().isSynced() ? "synced" : "unsynced")+" "+getURI().getHost()+":"+getURI().getPort()+" "+getState()+" "+getDirection()+" "+getLatency()+"ms "+(getNode() == null ? "" : getNode());
	}
	
	@Override
	public int compareTo(final AbstractConnection o) 
	{
		return getID() - o.getID();
	}
}
