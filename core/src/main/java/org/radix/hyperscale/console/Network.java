package org.radix.hyperscale.console;

import java.io.PrintStream;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.lang.System;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.json.JSONObject;
import org.radix.hyperscale.Constants;
import org.radix.hyperscale.Context;
import org.radix.hyperscale.common.Agent;
import org.radix.hyperscale.common.Direction;
import org.radix.hyperscale.crypto.Identity;
import org.radix.hyperscale.ledger.ShardGroupID;
import org.radix.hyperscale.ledger.ShardMapper;
import org.radix.hyperscale.network.AbstractConnection;
import org.radix.hyperscale.network.ConnectionState;
import org.radix.hyperscale.network.Protocol;
import org.radix.hyperscale.network.StandardConnectionFilter;
import org.radix.hyperscale.network.discovery.NotLocalPeersFilter;
import org.radix.hyperscale.network.discovery.OutboundDiscoveryFilter;
import org.radix.hyperscale.network.peers.Peer;
import org.radix.hyperscale.serialization.Serialization;
import org.radix.hyperscale.serialization.DsonOutput.Output;
import org.radix.hyperscale.time.Time;

public class Network extends Function
{
	private static final Options options = new Options().addOption(Option.builder("disconnect").desc("Disconnect a peer").optionalArg(true).build())
		    											.addOption(Option.builder("ban").desc("Bans a peer host IP or identity").numberOfArgs(3).build())
													    .addOption("connect", true, "Forcibly connects to an endpoint")
														.addOption(Option.builder("stats").desc("Outputs network statistics options are gossip and messages").optionalArg(true).build())
														.addOption("host", true, "Shows detailed information about a connection")
														.addOption("peer", true, "Shows detailed information about a known peer")
														.addOption("known", false, "Lists all known peers (can be very large)") // TODO pagination for this?
														.addOption("best", false, "Lists all known peers by XOR ranking (can be very large)"); // TODO pagination for this?


	public Network()
	{
		super("network", options);
	}

	@Override
	public void execute(Context context, String[] arguments, PrintStream printStream) throws Exception
	{
		CommandLine commandLine = Function.parser.parse(options, arguments);

		if (commandLine.hasOption("ban"))
		{
			final Peer knownPeer;
			final AbstractConnection connection;
			final String query = commandLine.getOptionValues("ban")[0];
			if (query.contains(".") || query.contains(":"))
			{
				final URI URI = Agent.getURI(query);
				knownPeer = context.getNetwork().getPeerHandler().get(URI);
				connection = context.getNetwork().get(URI, Protocol.TCP, ConnectionState.SELECT_CONNECTING_CONNECTED);
			}
			else
			{
				final Identity identity = Identity.from(query);
				knownPeer = context.getNetwork().getPeerHandler().get(identity);
				connection = context.getNetwork().get(identity, Protocol.TCP, ConnectionState.SELECT_CONNECTING_CONNECTED);
			}

			if (knownPeer != null)
			{
				int banDurationSeconds = Integer.parseInt(commandLine.getOptionValues("ban")[1]);
				String banReason = commandLine.getOptionValues("ban")[2];
				if (connection != null)
					connection.ban(banReason, banDurationSeconds, TimeUnit.SECONDS);
				else
				{
					knownPeer.setBanned(banReason, Time.getSystemTime() + TimeUnit.SECONDS.toMillis(banDurationSeconds));
					context.getNetwork().getPeerHandler().update(knownPeer);
				}
			}
			else
				printStream.println("Peer not found for "+query);
		}
		else if (commandLine.hasOption("disconnect"))
		{
			if (commandLine.getOptionValue("disconnect") == null)
			{
				for (AbstractConnection connectedPeer : context.getNetwork().get(StandardConnectionFilter.build(context).setStates(ConnectionState.SELECT_CONNECTING_CONNECTED), false))
				{
					connectedPeer.disconnect("Forced disconnect");
					printStream.println("Disconnecting "+connectedPeer+" @ "+connectedPeer.getNode().getHead());
				}
			}
			else
			{
				printStream.println("Disconnecting individual peers not yet supported");
			}
		}
		else if (commandLine.hasOption("connect"))
		{
			URI URI = Agent.getURI(commandLine.getOptionValue("connect"));
			context.getNetwork().connect(URI, Direction.OUTBOUND, Protocol.TCP);
		}
		else if (commandLine.hasOption("best"))
		{
			ShardGroupID syncShardGroupID = ShardMapper.toShardGroup(context.getNode().getIdentity(), context.getLedger().numShardGroups());
			printStream.println("-- Filtered Sync "+syncShardGroupID+" ---");
			OutboundDiscoveryFilter outboundSyncDiscoveryFilter = new OutboundDiscoveryFilter(context, syncShardGroupID);
			Collection<Peer> bestPeers = context.getNetwork().getPeerHandler().get(outboundSyncDiscoveryFilter, Constants.DEFAULT_TCP_CONNECTIONS_OUT_SYNC);
			for (Peer bestPeer : bestPeers)
				printStream.println(bestPeer.distance(context.getNode().getIdentity())+" "+bestPeer.toString());

			for (int sg = 0 ; sg < context.getLedger().numShardGroups() ; sg++)
			{
				ShardGroupID shardGroupID = ShardGroupID.from(sg);
				if (shardGroupID.equals(syncShardGroupID))
					continue;

				printStream.println("-- Filtered Shard "+sg+" ---");
				OutboundDiscoveryFilter outboundShardDiscoveryFilter = new OutboundDiscoveryFilter(context, shardGroupID);
				bestPeers = context.getNetwork().getPeerHandler().get(outboundShardDiscoveryFilter, Constants.DEFAULT_TCP_CONNECTIONS_OUT_SYNC);
				for (Peer bestPeer : bestPeers)
					printStream.println(bestPeer.distance(context.getNode().getIdentity())+" "+bestPeer.toString());
			}
		}
		else if (commandLine.hasOption("known"))
		{
			// TODO paginate this
			Collection<Peer> knownPeers = context.getNetwork().getPeerHandler().get(new NotLocalPeersFilter(context.getNode()).setBanned(true).setInvalid(true), 0, Short.MAX_VALUE);
			int numShardGroups = context.getLedger().numShardGroups();
			for (Peer knownPeer : knownPeers)
				printStream.println("S-"+(ShardMapper.toShardGroup(knownPeer.getIdentity(), numShardGroups))+" <- "+knownPeer.toString());
			
			printStream.println("Total known: "+knownPeers.size());
		}
		else if (commandLine.hasOption("host"))
		{
			final URI URI = Agent.getURI(commandLine.getOptionValue("host"));
			final Collection<AbstractConnection> connections = context.getNetwork().get(StandardConnectionFilter.build(context).setURI(URI));
			if (connections.isEmpty() == false)
			{
				for (final AbstractConnection connection : connections)
					printStream.println(connection.toString());
			}
			else
				printStream.println("Connection/s not found for "+URI);
		}
		else if (commandLine.hasOption("peer"))
		{
			final String query = commandLine.getOptionValue("peer");
			final Peer knownPeer;
			if (query.contains(".") || query.contains(":"))
			{
				final URI URI = Agent.getURI(commandLine.getOptionValue("peer"));
				knownPeer = context.getNetwork().getPeerHandler().get(URI);
			}
			else
			{
				final Identity identity = Identity.from(query);
				knownPeer = context.getNetwork().getPeerHandler().get(identity);
			}

			if (knownPeer != null)
			{
				JSONObject knownPeerJSON = Serialization.getInstance().toJsonObject(knownPeer, Output.PERSIST);
				printStream.println(knownPeerJSON.toString(4));
			}
			else
				printStream.println("Peer not found for "+query);
		}
		else if (commandLine.hasOption("stats"))
		{
			String options[] = commandLine.getArgs();
			if (options == null || options.length == 0 || options[0].equalsIgnoreCase("messages"))
			{
				printStream.println("Bandwidth:");
				printStream.println("In bytes: "+context.getMetaData().get("network.transferred.inbound", 0l)+" bytes "+(context.getTimeSeries("bandwidth").average("inbound", System.currentTimeMillis()-TimeUnit.SECONDS.toMillis(30), TimeUnit.MILLISECONDS))+" bytes/s");
				printStream.println("Out bytes: "+context.getMetaData().get("network.transferred.outbound", 0l)+" bytes "+(context.getTimeSeries("bandwidth").average("outbound", System.currentTimeMillis()-TimeUnit.SECONDS.toMillis(30), TimeUnit.MILLISECONDS))+" bytes/s");
				printStream.println();
	
				printStream.println("Messages:");
				printStream.println();
	
				printStream.println("Received: "+context.getMetaData().get("messaging.inbound", 0l)+" / "+(context.getMetaData().get("messaging.inbound.latency", 0l) / context.getMetaData().get("messaging.inbound", 1l))+" ms");
				for (Entry<Class<?>, Long> received : context.getNetwork().getMessaging().getReceivedByType())
					printStream.println(received.getKey()+": "+received.getValue());
				printStream.println();
	
				printStream.println("Sent: "+context.getMetaData().get("messaging.outbound", 0l)+" / "+(context.getMetaData().get("messaging.outbound.latency", 0l) / context.getMetaData().get("messaging.outbound", 1l))+" ms");
				for (Entry<Class<?>, Long> sent : context.getNetwork().getMessaging().getSentByType())
					printStream.println(sent.getKey()+": "+sent.getValue());
				printStream.println();
			}

			if (options == null || options.length == 0 || options[0].equalsIgnoreCase("gossip"))
			{
				printStream.println("Gossip Queue:");
				printStream.println("               Events: "+context.getNetwork().getGossipHandler().getEventQueueSize());
				printStream.println("            Broadcast: "+context.getNetwork().getGossipHandler().getBroadcastQueueSize());
				printStream.println("            Requested: "+context.getNetwork().getGossipHandler().getRequestedQueueSize());
				printStream.println("        Request Tasks: "+context.getNetwork().getGossipHandler().getRequestTasksQueueSize());
				printStream.println();
	
				printStream.println("Gossip Requests "+context.getMetaData().get("gossip.requests.items", 0l));
				printStream.println("                 Atom: "+context.getMetaData().get("gossip.requests.atom", 0l));
				printStream.println("      Atom Vote Block: "+context.getMetaData().get("gossip.requests.atomvoteblock", 0l));
				printStream.println("         Block Header: "+context.getMetaData().get("gossip.requests.blockheader", 0l));
				printStream.println("           Block Vote: "+context.getMetaData().get("gossip.requests.blockvote", 0l));
				printStream.println("     State Vote Block: "+context.getMetaData().get("gossip.requests.statevoteblock", 0l));
				printStream.println("          State Input: "+context.getMetaData().get("gossip.requests.stateinput", 0l));
				printStream.println("    State Certificate: "+context.getMetaData().get("gossip.requests.statecertificate", 0l));
				printStream.println();
				
				printStream.println("Gossip Inventories "+context.getMetaData().get("gossip.inventories.items", 0l));
				printStream.println("                 Atom: "+context.getMetaData().get("gossip.inventories.atom", 0l));
				printStream.println("      Atom Vote Block: "+context.getMetaData().get("gossip.inventories.atomvoteblock", 0l));
				printStream.println("         Block Header: "+context.getMetaData().get("gossip.inventories.blockheader", 0l));
				printStream.println("           Block Vote: "+context.getMetaData().get("gossip.inventories.blockvote", 0l));
				printStream.println("     State Vote Block: "+context.getMetaData().get("gossip.inventories.statevoteblock", 0l));
				printStream.println("          State Input: "+context.getMetaData().get("gossip.inventories.stateinput", 0l));
				printStream.println("    State Certificate: "+context.getMetaData().get("gossip.inventories.statecertificate", 0l));
				printStream.println("      Int. cache hits: "+context.getMetaData().get("gossip.cache.hits", 0l));
				printStream.println("    Int. cache misses: "+context.getMetaData().get("gossip.cache.misses", 0l));
				printStream.println("      Ext. cache hits: "+context.getMetaData().get("store.has.cache.hits", 0l));
				printStream.println("    Ext. cache misses: "+context.getMetaData().get("store.has.cache.misses", 0l));
				printStream.println();
	
				printStream.println("Gossip Required "+context.getMetaData().get("gossip.required.items", 0l));
				printStream.println("                 Atom: "+context.getMetaData().get("gossip.required.atom", 0l));
				printStream.println("      Atom Vote Block: "+context.getMetaData().get("gossip.required.atomvoteblock", 0l));
				printStream.println("         Block Header: "+context.getMetaData().get("gossip.required.blockheader", 0l));
				printStream.println("           Block Vote: "+context.getMetaData().get("gossip.required.blockvote", 0l));
				printStream.println("     State Vote Block: "+context.getMetaData().get("gossip.required.statevoteblock", 0l));
				printStream.println("          State Input: "+context.getMetaData().get("gossip.required.stateinput", 0l));
				printStream.println("    State Certificate: "+context.getMetaData().get("gossip.required.statecertificate", 0l));
				printStream.println();
	
				printStream.println("Gossip Latency");
				printStream.println("    0-1000ms: "+context.getMetaData().get("gossip.received.items", 0l)+" / "+(context.getMetaData().get("gossip.received.items.interval", 0l) / context.getMetaData().get("gossip.received.items", 1l))+" ms");
				printStream.println("     1000ms+: "+context.getMetaData().get("gossip.received.items.latent", 0l)+" / "+(context.getMetaData().get("gossip.received.items.latent.interval", 0l) / context.getMetaData().get("gossip.received.items.latent", 1l))+" ms");
				printStream.println();
	
				printStream.println("Gossip Utilization");
				printStream.println("      Requests: "+context.getMetaData().get("gossip.requests.total", 0l)+" / "+context.getMetaData().get("gossip.requests.items", 0l)+" / "+(context.getMetaData().get("gossip.requests.items", 0l) / context.getMetaData().get("gossip.requests.total", 1l)));
				printStream.println("    Broadcasts: "+context.getMetaData().get("gossip.broadcasts.total", 0l)+" / "+context.getMetaData().get("gossip.broadcasts.items", 0l)+" / "+(context.getMetaData().get("gossip.broadcasts.items", 0l) / context.getMetaData().get("gossip.broadcasts.total", 1l)));
				printStream.println();
			}
		}
		else
		{
			final ShardGroupID shardGroupID = ShardMapper.toShardGroup(context.getNode().getIdentity(), context.getLedger().numShardGroups());
			final List<AbstractConnection> connections = context.getNetwork().get(StandardConnectionFilter.build(context).setStates(ConnectionState.SELECT_CONNECTED));
			connections.sort((o1, o2) -> {
				ShardGroupID sg1 = ShardMapper.toShardGroup(o1.getNode().getIdentity(), context.getLedger().numShardGroups());
				ShardGroupID sg2 = ShardMapper.toShardGroup(o2.getNode().getIdentity(), context.getLedger().numShardGroups());
				
				return sg1.compareTo(sg2);
			});

			int syncConnections = 0;
			printStream.println("Sync:");
			for (AbstractConnection connection : connections)
			{
				if (ShardMapper.toShardGroup(connection.getNode().getIdentity(), context.getLedger().numShardGroups()).equals(shardGroupID) == true)
				{
					syncConnections++;
					printStream.println(connection.toString());
				}
			}
			printStream.println("Total Sync Connections: "+syncConnections);
	
			int shardConnections = 0;
			printStream.println("Shard:");
			for (AbstractConnection connection : connections)
			{
				if (ShardMapper.toShardGroup(connection.getNode().getIdentity(), context.getLedger().numShardGroups()).equals(shardGroupID) == false)
				{
					shardConnections++;
					printStream.println(ShardMapper.toShardGroup(connection.getNode().getIdentity(), context.getLedger().numShardGroups())+" "+connection.toString());
				}
			}
			printStream.println("Total Shard Connections: "+shardConnections);
		}
	}
}