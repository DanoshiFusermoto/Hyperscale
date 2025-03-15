package org.radix.hyperscale;

import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.java_websocket.WebSocket;
import org.java_websocket.WebSocketImpl;
import org.java_websocket.drafts.Draft;
import org.java_websocket.drafts.Draft_6455;
import org.java_websocket.extensions.permessage_deflate.PerMessageDeflateExtension;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.radix.hyperscale.events.EventListener;
import org.radix.hyperscale.ledger.CommitDecision;
import org.radix.hyperscale.ledger.PendingAtom;
import org.radix.hyperscale.ledger.events.AtomAcceptedTimeoutEvent;
import org.radix.hyperscale.ledger.events.AtomCommitEvent;
import org.radix.hyperscale.ledger.events.AtomCommitTimeoutEvent;
import org.radix.hyperscale.ledger.events.AtomExceptionEvent;
import org.radix.hyperscale.ledger.events.AtomExecutionTimeoutEvent;
import org.radix.hyperscale.ledger.events.AtomUnpreparedTimeoutEvent;
import org.radix.hyperscale.ledger.primitives.Atom;
import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;
import org.radix.hyperscale.serialization.Serialization;
import org.radix.hyperscale.serialization.SerializationException;
import org.radix.hyperscale.serialization.DsonOutput.Output;
import org.radix.hyperscale.utils.MathUtils;

import com.google.common.eventbus.Subscribe;

/** 
 * Simple Websocket service that clients can connect to and receive a stream of JSON formatted events regarding atoms.
 * 
 * Needs lots of additional functionality to be a real Websocket service.  Currently used to assist with testing, hence simplicity.
 * 
 * @author Dan
 *
 */
public final class WebSocketService extends WebSocketServer
{
	private static final Logger websocketLog = Logging.getLogger("websocket");

	private final Context context;
	
	// TODO hack to keep a copy of the connections list rather than calling getConnections() repeatedly on the super class
	//		which returns a new ArrayList every time.  Furthermore if there are no collections, it creates a new ArrayList 
	//      rather than Collections.emptyList() if there are no connections!
	private final AtomicInteger connections = new AtomicInteger(0);
	
	private static final Draft perMessageDeflateDraft = new Draft_6455(new PerMessageDeflateExtension());
	
	public WebSocketService(final Context context)
	{
		super(new InetSocketAddress(context.getConfiguration().get("websocket.address", "0.0.0.0"), context.getConfiguration().get("websocket.port", WebSocketImpl.DEFAULT_PORT)),
			  context.getConfiguration().get("websocket.decoders", 1 + Math.max(0, MathUtils.log2(Runtime.getRuntime().availableProcessors() / context.getConfiguration().getCommandLine("contexts", 1))))); 
		
		this.context = Objects.requireNonNull(context, "Context is null");
		
		this.setConnectionLostTimeout(120);
	}
	
	@Override
	public void onStart()
	{
		this.context.getEvents().register(this.asyncAtomListener);
	}

	@Override
	public void stop(int timeout) throws InterruptedException
	{
		this.context.getEvents().unregister(this.asyncAtomListener);
		super.stop(timeout);
	}
	
	@Override
	public void onOpen(WebSocket conn, ClientHandshake handshake)
	{
		this.connections.incrementAndGet();
	}

	@Override
	public void onClose(WebSocket conn, int code, String reason, boolean remote)
	{
		int remainingConnections = this.connections.decrementAndGet();
		if (remainingConnections < 0)
			websocketLog.error(this.context.getName()+": Cached connections count is negative!");
	}

	@Override
	public void onMessage(WebSocket conn, String message)
	{
		JSONObject messageJSON = new JSONObject(message);
		String type = messageJSON.getString("type");
		if (type.equalsIgnoreCase("submit"))
		{
			JSONObject atomJSON = null;
			Atom atom = null;
			try
			{
				atomJSON = messageJSON.getJSONObject("atom");
				atom = Serialization.getInstance().fromJsonObject(atomJSON, Atom.class);
			}
			catch (Exception ex)
			{
				JSONObject eventJSON = new JSONObject();
				eventJSON.put("type", "exception");
				eventJSON.put("error", "Atom was malformed with exception "+ex.toString());
				conn.send(eventJSON.toString());
				websocketLog.error(this.context.getName()+": Submission of atom "+atom.getHash()+" failed from "+conn.getRemoteSocketAddress(), ex);
				return;
			}
			
			try
			{
				if (this.context.getLedger().submit(atom) == false)
					new RejectedExecutionException("Submission of atom "+atom.getHash()+" failed");
			}
			catch (Exception ex)
			{
				JSONObject eventJSON = new JSONObject();
				eventJSON.put("type", "exception");
				eventJSON.put("atom", atom.getHash());
				JSONArray manifest = new JSONArray();
				for (Object instruction : atom.getManifest())
					manifest.put(instruction);
				eventJSON.put("manifest", manifest);
				eventJSON.put("error", ex.toString());
				conn.send(eventJSON.toString());
				websocketLog.error(this.context.getName()+": Submission of atom "+atom.getHash()+" failed from "+conn.getRemoteSocketAddress(), ex);
			}
		}
	}

	@Override
	public void onError(WebSocket conn, Exception ex)
	{
	}

	// ASYNC ATOM LISTENER //
	private EventListener asyncAtomListener = new EventListener()
	{
		@Subscribe
		public void on(AtomCommitEvent event) throws JSONException 
		{
			if (WebSocketService.this.connections.get() == 0)
				return;
			
			JSONObject eventJSON = new JSONObject();
			
			if (event.getDecision().equals(CommitDecision.ACCEPT))
				eventJSON.put("type", "committed");
			else if (event.getDecision().equals(CommitDecision.REJECT))
			{
				eventJSON.put("type", "rejected");
				if (event.getPendingAtom().thrown() != null)
					eventJSON.put("reason", event.getPendingAtom().thrown().getMessage());
				else
					eventJSON.put("reason", "unknown");
			}
			else
				throw new UnsupportedOperationException("State decision of type "+event.getDecision()+" is not supported");
			
			eventJSON.put("atom", Serialization.getInstance().toJsonObject(event.getPendingAtom().getAtom(), Output.API));
			eventJSON.put("certificate", Serialization.getInstance().toJsonObject(event.getPendingAtom().getCertificate(), Output.API));
			WebSocketService.this.broadcast(eventJSON.toString());
		}

		@Subscribe
		public void on(AtomExceptionEvent event)
		{
			if (WebSocketService.this.connections.get() == 0)
				return;

			JSONObject eventJSON = new JSONObject();
			eventJSON.put("type", "exception");
			eventJSON.put("atom", Serialization.getInstance().toJsonObject(event.getPendingAtom().getAtom(), Output.API));
			eventJSON.put("error", event.getException().toString());
			WebSocketService.this.broadcast(eventJSON.toString());
		}
		
		@Subscribe
		public void on(AtomUnpreparedTimeoutEvent event) throws SerializationException
		{
			timedout(event.getPendingAtom(), "prepare");
		}

		@Subscribe
		public void on(AtomAcceptedTimeoutEvent event) throws SerializationException 
		{
			timedout(event.getPendingAtom(), "accept");
		}

		@Subscribe
		public void on(AtomExecutionTimeoutEvent event) throws SerializationException
		{
			timedout(event.getPendingAtom(), "execution");
		}

		@Subscribe
		public void on(AtomCommitTimeoutEvent event)
		{
			timedout(event.getPendingAtom(), "commit");
		}
		
		private void timedout(PendingAtom pendingAtom, String timeout)
		{
			if (WebSocketService.this.connections.get() == 0)
				return;

			JSONObject eventJSON = new JSONObject();
			eventJSON.put("type", "timeout");
			eventJSON.put("timeout", timeout);
			eventJSON.put("atom", Serialization.getInstance().toJsonObject(pendingAtom.getAtom(), Output.API));

			WebSocketService.this.broadcast(eventJSON.toString());
		}
	};
}
