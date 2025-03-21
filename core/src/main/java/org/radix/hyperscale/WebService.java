package org.radix.hyperscale;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.time.DurationFormatUtils;
import org.eclipse.jetty.http.MimeTypes;
import org.json.JSONArray;
import org.json.JSONObject;
import org.radix.hyperscale.common.Agent;
import org.radix.hyperscale.common.Match;
import org.radix.hyperscale.common.Order;
import org.radix.hyperscale.common.Primitive;
import org.radix.hyperscale.crypto.CryptoException;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.crypto.Identity;
import org.radix.hyperscale.database.SystemMetaData;
import org.radix.hyperscale.exceptions.StartupException;
import org.radix.hyperscale.exceptions.TerminationException;
import org.radix.hyperscale.ledger.AssociationSearchQuery;
import org.radix.hyperscale.ledger.AssociationSearchResponse;
import org.radix.hyperscale.ledger.Block;
import org.radix.hyperscale.ledger.BlockHeader;
import org.radix.hyperscale.ledger.Epoch;
import org.radix.hyperscale.ledger.Isolation;
import org.radix.hyperscale.ledger.Ledger;
import org.radix.hyperscale.ledger.PrimitiveSearchQuery;
import org.radix.hyperscale.ledger.PrimitiveSearchResponse;
import org.radix.hyperscale.ledger.ShardGroupID;
import org.radix.hyperscale.ledger.ShardMapper;
import org.radix.hyperscale.ledger.StateAddress;
import org.radix.hyperscale.ledger.SubstateCommit;
import org.radix.hyperscale.ledger.SubstateSearchQuery;
import org.radix.hyperscale.ledger.SubstateSearchResponse;
import org.radix.hyperscale.ledger.BlockHeader.InventoryType;
import org.radix.hyperscale.ledger.primitives.Atom;
import org.radix.hyperscale.ledger.primitives.AtomCertificate;
import org.radix.hyperscale.ledger.primitives.Blob;
import org.radix.hyperscale.ledger.sme.PolyglotPackage;
import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;
import org.radix.hyperscale.network.AbstractConnection;
import org.radix.hyperscale.network.ConnectionState;
import org.radix.hyperscale.network.StandardConnectionFilter;
import org.radix.hyperscale.network.discovery.MultiPeerFilter;
import org.radix.hyperscale.network.discovery.NotLocalPeersFilter;
import org.radix.hyperscale.network.discovery.StandardDiscoveryFilter;
import org.radix.hyperscale.network.peers.Peer;
import org.radix.hyperscale.node.Node;
import org.radix.hyperscale.serialization.Serialization;
import org.radix.hyperscale.serialization.DsonOutput.Output;
import org.radix.hyperscale.serialization.mapper.JacksonCodecConstants;
import org.radix.hyperscale.utils.Bytes;
import org.radix.hyperscale.utils.TimeSeriesStatistics;
import org.radix.hyperscale.utils.UInt256;

import spark.Request;
import spark.Response;

// TODO upgrade allowing multiple WebService instances for each context.  Needs multiple WebService endpoints on different ports. 
public class WebService implements Service
{
	private static final Logger apiLog = Logging.getLogger("api");
	
	public static final int DEFAULT_PORT = 8080;
	public static final String DEFAULT_API_PATH = "/api";
	public static final String DEFAULT_SCHEME = "http://";

	private static WebService instance;
	
	public static synchronized WebService create()
	{
		if (instance != null)
			throw new RuntimeException("WebService instance already created");
		
		instance = new WebService();
		return instance;
	}

	public static synchronized WebService getInstance()
	{
		if (instance == null)
			throw new RuntimeException("WebService instance not created");
		
		return instance;
	}
	
//	private final Context context;
	
	private WebService()
	{
	}
	
	@Override
	public void start() throws StartupException
	{
		try
		{
			
			Set<String> knownTypes = MimeTypes.getKnownMimeTypes();
			apiLog.info("MIME types known for hosting:");
			for (String knownType : knownTypes)
				apiLog.info(knownType);

			spark.Spark.port(Configuration.getDefault().get("api.port", DEFAULT_PORT));
			
			spark.Spark.options("/*", (request, response) -> 
			{
	            String accessControlRequestHeaders = request.headers("Access-Control-Request-Headers");
	            if (accessControlRequestHeaders != null) 
	            	response.header("Access-Control-Allow-Headers", accessControlRequestHeaders);

	            String accessControlRequestMethod = request.headers("Access-Control-Request-Method");
	            if (accessControlRequestMethod != null)
	            	response.header("Access-Control-Allow-Methods", accessControlRequestMethod);

	            return "OK";
	        });

			spark.Spark.before((request, response) -> {
				response.header("Access-Control-Allow-Origin", "*");
				response.header("Access-Control-Allow-Credentials", "true");
			});
			
			// CONTEXT
			{
				contextNode();
				
				contextUniverse();
			}
			
			// LEDGER
			{
				ledgerHead();
				
				ledgerStatistics();
	
				ledgerLatest();

				ledgerSearch();
				
				ledgerSubmit();
			}
			
			// NETWORK
			{
				networkBootstrap();
				
				networkStatistics();
				
				networkConnections();
				
				networkKnown();
	
				networkSearch();
			}
			
			// STATISTICS
			statisticsTimeSeries();
			
			statisticsAll();

			// MUST BE LAST!
			webContent();
		}
		catch (Throwable t)
		{
			throw new StartupException(t);
		}
	}

	@Override
	public void stop() throws TerminationException
	{
		spark.Spark.stop();
	}
	
	private Context getContext(Request req)
	{
		String value = req.queryParams("context");
		if (value != null)
		{
			Context context = Context.get(value);
			if (context == null)
				throw new IllegalArgumentException("Context "+value+" not found");
			
			return context;
		}

		value = req.queryParams("shardgroup");
		if (value != null)
		{
			ShardGroupID requiredShardGroup = ShardGroupID.from(Integer.parseInt(value));
			for (Context context : Context.getAll())
			{
				ShardGroupID contextShardGroupID = ShardMapper.toShardGroup(context.getNode().getIdentity(), context.getLedger().numShardGroups());
				if (requiredShardGroup.equals(contextShardGroupID))
					return context;
			}
			
			throw new IllegalArgumentException("Context with shard group "+requiredShardGroup+" not found");
		}
		
		return Context.get();
	}

	// CONTEXT
	private void contextNode()
	{
		spark.Spark.get(Configuration.getDefault().get("api.url", DEFAULT_API_PATH)+"/node", (req, res) -> 
		{
			JSONObject responseJSON = new JSONObject();
			
			try
			{
				Context context = getContext(req);
				Node node = new Node(context.getNode());
				responseJSON.put("node", Serialization.getInstance().toJsonObject(node, Output.API));
				
				// Add the current shard for the node
				int numShardGroups = context.getLedger().numShardGroups();
				ShardGroupID shardGroupID = ShardMapper.toShardGroup(node.getIdentity(), numShardGroups);
				responseJSON.getJSONObject("node").put("shard_group", shardGroupID.intValue());
				
				// Add node uptime
				JSONObject uptimeJSON = new JSONObject();
				uptimeJSON.put("duration", DurationFormatUtils.formatDuration(context.getUptime(), "H:mm:ss", true));
				uptimeJSON.put("millis", context.getUptime());
				responseJSON.put("uptime", uptimeJSON);

				// Remove the head inventory 
				responseJSON.getJSONObject("node").getJSONObject("head").remove("inventory");
				status(responseJSON, 200);
			}
			catch(Throwable t)
			{
				status(responseJSON, 500, t.toString());
				apiLog.error("Failed to fetch Universe object - "+t.getMessage());
			}

			return responseJSON.toString(4);
		});
	}

	private void contextUniverse()
	{
		spark.Spark.get(Configuration.getDefault().get("api.url", DEFAULT_API_PATH)+"/universe", (req, res) -> 
		{
			JSONObject responseJSON = new JSONObject();
			
			try
			{
				responseJSON.put("universe", Serialization.getInstance().toJsonObject(Universe.get(), Output.ALL));
				status(responseJSON, 200);
			}
			catch(Throwable t)
			{
				status(responseJSON, 500, t.toString());
				apiLog.error("Failed to fetch Universe object - "+t.getMessage());
			}

			return responseJSON.toString(4);
		});
	}

	// LEDGER
	private void ledgerHead()
	{
		spark.Spark.get(Configuration.getDefault().get("api.url", DEFAULT_API_PATH)+"/ledger/head", (req, res) -> 
		{
			JSONObject responseJSON = new JSONObject();
			
			try
			{
				Context context = getContext(req);
				BlockHeader head = context.getLedger().getHead();
				JSONObject 	headJSON = Serialization.getInstance().toJsonObject(head, Output.ALL);
				if (req.queryParamOrDefault("inventory", "false").equalsIgnoreCase("false") )
					headJSON.remove("inventory");
				responseJSON.put("head", headJSON);
				status(responseJSON, 200);
			}
			catch(Throwable t)
			{
				status(responseJSON, 500, t.toString());
				apiLog.error("Failed to fetch current ledger statistics", t);
			}

			return responseJSON.toString(4);
		});
	}
	
	private void ledgerLatest()
	{
		spark.Spark.get(Configuration.getDefault().get("api.url", DEFAULT_API_PATH)+"/ledger/latest", (req, res) -> 
		{
			JSONObject responseJSON = new JSONObject();
			
			try
			{
				Context context = getContext(req);
				BlockHeader head = context.getLedger().getHead();
				JSONObject 	headJSON = Serialization.getInstance().toJsonObject(head, Output.ALL);
				if (req.queryParamOrDefault("inventory", "false").equalsIgnoreCase("false") )
					headJSON.remove("inventory");
				responseJSON.put("head", headJSON);

				List<SubstateCommit> substateCommits = new ArrayList<SubstateCommit>();
				for (Hash atomCertificateHash : head.getInventory(InventoryType.COMMITTED))
				{
					AtomCertificate atomCertificate = context.getLedger().get(atomCertificateHash, AtomCertificate.class);
					Future<SubstateSearchResponse> atomSubstate = context.getLedger().get(new SubstateSearchQuery(StateAddress.from(Atom.class, atomCertificate.getAtom()), true));
					SubstateSearchResponse substateSearchResponse = atomSubstate.get(1, TimeUnit.SECONDS);
					if (substateSearchResponse.getResult() == null)
						continue;
					
					substateCommits.add(substateSearchResponse.getResult());
					if (substateCommits.size() == 10)
						break;
				}
				
				JSONArray latestJSON = new JSONArray();
				if (substateCommits.isEmpty() == false)
				{
					for(SubstateCommit substateCommit : substateCommits)
						latestJSON.put(Serialization.getInstance().toJsonObject(substateCommit, Output.API));
				}
				responseJSON.put("latest", latestJSON);
				status(responseJSON, 200);
			}
			catch(Throwable t)
			{
				status(responseJSON, 500, t.toString());
				apiLog.error("Failed to fetch latest transactions", t);
			}

			return responseJSON.toString(4);
		});
	}

	private void ledgerStatistics()
	{
		spark.Spark.get(Configuration.getDefault().get("api.url", DEFAULT_API_PATH)+"/ledger/statistics", (req, res) -> 
		{
			JSONObject responseJSON = new JSONObject();
			
			try
			{
				final Context context = getContext(req);
				final long timestamp = System.currentTimeMillis()-TimeUnit.SECONDS.toMillis(1);
				final Epoch epoch = context.getLedger().getEpoch();
				final BlockHeader head = context.getLedger().getHead();
				final JSONObject  headJSON = Serialization.getInstance().toJsonObject(head, Output.ALL);
				headJSON.remove("inventory");
				responseJSON.put("head", headJSON);
				
				final JSONObject ageJSON = new JSONObject();
				ageJSON.put("duration", DurationFormatUtils.formatDuration(head.getHeight() * Ledger.definitions().roundInterval(), "H:mm:ss", true));
				ageJSON.put("millis", head.getHeight() * Ledger.definitions().roundInterval());
				responseJSON.put("age", ageJSON);

				final JSONObject statistics = new JSONObject();
				statistics.put("shard_groups", context.getLedger().numShardGroups(epoch));
				
				final JSONObject processed = new JSONObject();
				{
					final JSONObject transactions = new JSONObject();
					transactions.put("pending", context.getLedger().getAtomHandler().size());
					transactions.put("failed", context.getMetaData().get("ledger.processed.atoms.timedout.accept", 0l)+
											   context.getMetaData().get("ledger.processed.atoms.timedout.execution", 0l)+
											   context.getMetaData().get("ledger.processed.atoms.timedout.commit", 0l));
					transactions.put("local", context.getMetaData().get("ledger.processed.atoms.local", 0l));
					transactions.put("total", context.getMetaData().get("ledger.processed.atoms.total", 0l));
					processed.put("transactions", transactions);
					
					final JSONObject executions = new JSONObject();
					executions.put("local", context.getMetaData().get("ledger.processed.executions.local", 0l));
					executions.put("total", context.getMetaData().get("ledger.processed.executions.total", 0l));
					processed.put("executions", executions);
				}
				statistics.put("processed", processed);

				final JSONObject throughput = new JSONObject();
				{
					final JSONObject transactions = new JSONObject();
					transactions.put("local", context.getTimeSeries("throughput").average("local", timestamp-TimeUnit.SECONDS.toMillis(30), timestamp, TimeUnit.MILLISECONDS));
					transactions.put("total", context.getTimeSeries("throughput").average("total", timestamp-TimeUnit.SECONDS.toMillis(30), timestamp, TimeUnit.MILLISECONDS));
					transactions.put("shards_average", Math.max(context.getMetaData().get("ledger.throughput.shards.touched", 1.0), 1.0));
					throughput.put("transactions", transactions);

					final JSONObject executions = new JSONObject();
					final JSONObject executionsLocal = new JSONObject();
					final JSONObject executionsTotal = new JSONObject();
					final TimeSeriesStatistics executionsTimeSeries = context.getTimeSeries("executions");
					final Collection<String> executionTimeSeriesFields = executionsTimeSeries.getFields();
					for (final String field : executionTimeSeriesFields)
					{
						Number fieldAverage = executionsTimeSeries.average(field, timestamp-TimeUnit.SECONDS.toMillis(30), timestamp, TimeUnit.MILLISECONDS);
						if (field.startsWith("local:"))
							executionsLocal.put(field.substring("local:".length()), fieldAverage.doubleValue());
						else if (field.startsWith("total:"))
							executionsTotal.put(field.substring("total:".length()), fieldAverage.doubleValue());
						else
							throw new IllegalArgumentException("Field "+field+" is unsupported in execution timeseries");
					}
					executions.put("local", executionsLocal);
					executions.put("total", executionsTotal);
					throughput.put("executions", executions);

					JSONObject finality = new JSONObject();
					finality.put("consensus", context.getMetaData().get("ledger.throughput.latency.accepted", 0l));
					finality.put("client", context.getMetaData().get("ledger.throughput.latency.witnessed", 0l));
					throughput.put("finality", finality);
					
					JSONObject data = new JSONObject();
					data.put("local", context.getMetaData().get("ledger.blocks.bytes", 0l));
					data.put("total", context.getLedger().numShardGroups(epoch) * context.getMetaData().get("ledger.blocks.bytes", 0l));
					throughput.put("data", data);
				}
				statistics.put("throughput", throughput);
				
				final JSONObject proposals = new JSONObject();
				{
					proposals.put("size_average", context.getMetaData().get("ledger.blocks.bytes", 0l)/(head.getHeight()+1));
					
					final JSONObject intervals = new JSONObject();
					intervals.put("phase", context.getMetaData().get("ledger.interval.phase", 0l));
					intervals.put("round", context.getMetaData().get("ledger.interval.round", 0l));
					intervals.put("commit", context.getMetaData().get("ledger.interval.commit", 0l));
					proposals.put("intervals", intervals);
				}
				statistics.put("proposals", proposals);
				
				responseJSON.put("statistics", statistics);
				
				status(responseJSON, 200);
			}
			catch(Throwable t)
			{
				status(responseJSON, 500, t.toString());
				apiLog.error("Failed to fetch current ledger statistics", t);
			}

			return responseJSON.toString(4);
		});
	}
	
	private void ledgerSearch()
	{
		spark.Spark.get(Configuration.getDefault().get("api.url", DEFAULT_API_PATH)+"/ledger/primitive/:type/:hash", (req, res) -> processPrimitiveSearch(req, res).toString(4));

		spark.Spark.get(Configuration.getDefault().get("api.url", DEFAULT_API_PATH)+"/ledger/state/:context/:scope", (req, res) -> processSubstateSearch(req, res).toString(4));

		// TODO should be deprecated in favour of the POST method
		spark.Spark.get(Configuration.getDefault().get("api.url", DEFAULT_API_PATH)+"/ledger/association", (req, res) -> 
		{
			JSONObject responseJSON = new JSONObject();
			
			try
			{
				Context context = getContext(req);
				Match matchon = Match.valueOf(req.queryParamOrDefault("match", "all").toUpperCase());
				List<Hash> associations = new ArrayList<Hash>();
				for (String secondary : req.queryParamOrDefault("keys", "").split(","))
					associations.add(Hash.valueOf(decodeQueryValue(secondary)));
				
				if (matchon.equals(Match.ALL) && associations.size() > 1)
				{
					Hash concatenated = Hash.valueOf(associations);
					associations.clear();
					associations.add(concatenated);
				}
				
				Order order = Order.valueOf(req.queryParamOrDefault("order", Order.ASCENDING.toString()).toUpperCase());
				int offset = Integer.parseInt(req.queryParamOrDefault("offset", "-1"));
				int limit = Integer.parseUnsignedInt(req.queryParamOrDefault("limit", "50"));
				// TODO filter
				String filterContext = req.queryParamOrDefault("context", "");
				Future<AssociationSearchResponse> associationSearchResponseFuture = context.getLedger().get(new AssociationSearchQuery(associations, matchon, filterContext, order, offset, limit));
				AssociationSearchResponse associationSearchResponse = associationSearchResponseFuture.get(5, TimeUnit.SECONDS);

				if (associationSearchResponse.isEmpty())
					status(responseJSON, 404, "No instances found associated with "+req.queryParamOrDefault("keys", ""));
				else
				{
					JSONArray substatesArray  = new JSONArray();
					for (SubstateCommit result : associationSearchResponse.getResults())
						substatesArray.put(Serialization.getInstance().toJsonObject(result, Output.API));

					responseJSON.put("results", substatesArray);
					status(responseJSON, 200, associationSearchResponse.getQuery().getOffset(), associationSearchResponse.getNextOffset(), limit, associationSearchResponse.isEOR());
				}
			}
			catch(Throwable t)
			{
				status(responseJSON, 500, t.toString());
				apiLog.error("Failed to fetch associations "+req.queryParamOrDefault("keys", ""), t);
			}
			
			return responseJSON.toString(4);
		});

		spark.Spark.post(Configuration.getDefault().get("api.url", DEFAULT_API_PATH)+"/ledger/association", (req, res) -> 
		{
			JSONObject requestJSON = new JSONObject(req.body());
			JSONObject responseJSON = new JSONObject();
			
			try
			{
				Context context = getContext(req);
				Match matchon = Match.valueOf(requestJSON.optString("match", "all").toUpperCase());
				List<Hash> associations = new ArrayList<Hash>();
				for (int a = 0 ; a < requestJSON.getJSONArray("keys").length() ; a++)
				{
					String key = requestJSON.getJSONArray("keys").getString(a).toLowerCase();
					associations.add(Hash.valueOf(decodeQueryValue(key)));
				}
				
				if (matchon.equals(Match.ALL) && associations.size() > 1)
				{
					Hash concatenated = Hash.valueOf(associations);
					associations.clear();
					associations.add(concatenated);
				}
				
				Order order = Order.valueOf(requestJSON.optString("order", Order.ASCENDING.toString()).toUpperCase());
				int offset = requestJSON.optInt("offset", -1);
				int limit = requestJSON.optInt("limit", 100);
				// TODO filter
				String filterContext = req.queryParamOrDefault("filter", "");
				Future<AssociationSearchResponse> associationSearchResponseFuture = context.getLedger().get(new AssociationSearchQuery(associations, matchon, filterContext, order, offset, limit));
				AssociationSearchResponse associationSearchResponse = associationSearchResponseFuture.get(5, TimeUnit.SECONDS);

				if (associationSearchResponse.isEmpty())
					status(responseJSON, 404, "No instances found associated with "+requestJSON.getJSONArray("keys"));
				else
				{
					JSONArray substatesArray  = new JSONArray();
					for (SubstateCommit result : associationSearchResponse.getResults())
						substatesArray.put(Serialization.getInstance().toJsonObject(result, Output.API));

					responseJSON.put("results", substatesArray);
					status(responseJSON, 200, associationSearchResponse.getQuery().getOffset(), associationSearchResponse.getNextOffset(), limit, associationSearchResponse.isEOR());
				}
			}
			catch(Throwable t)
			{
				status(responseJSON, 500, t.toString());
				apiLog.error("Failed to fetch associations "+requestJSON.getJSONArray("keys").toString(), t);
			}
			
			return responseJSON.toString(4);
		});
	}

	private void ledgerSubmit()
	{
		spark.Spark.post(Configuration.getDefault().get("api.url", DEFAULT_API_PATH)+"/ledger/atom/submit", (req, res) -> 
		{
			JSONObject responseJSON = new JSONObject();
			Atom atom = null;
			try
			{
				String bodyString = req.body();
				JSONObject atomJSON = new JSONObject(bodyString);
				atom = Serialization.getInstance().fromJsonObject(atomJSON, Atom.class);
				Context.get().getLedger().submit(atom);
				status(responseJSON, 200);
			}
			catch(Throwable t)
			{
				status(responseJSON, 500, t.toString());
				apiLog.error("Failed to submit atom "+(atom != null ? atom.getHash() : ""), t);
			}

			return responseJSON.toString(4);
		});
	}
	
	private JSONObject processPrimitiveSearch(Request req, Response res)
	{
		JSONObject responseJSON = new JSONObject();
		
		try
		{
			final Hash primitiveHash = Hash.valueOf(decodeQueryValue(req.params("hash")));
			final Class<? extends Primitive> primitiveType = (Class<? extends Primitive>) Serialization.getInstance().getClassForId(req.params("type"));
			
			Future<PrimitiveSearchResponse> primitiveFuture = WebService.this.getContext(req).getLedger().get(new PrimitiveSearchQuery(primitiveHash, primitiveType));
			PrimitiveSearchResponse primitiveResult = primitiveFuture.get(5, TimeUnit.SECONDS);
			if (primitiveResult.getResult() == null)
				status(responseJSON, 404, "No primitive found "+primitiveHash+" of type "+req.params("type"));
			else
			{
				Primitive primitive = primitiveResult.getResult();
				if (Atom.class.isInstance(primitive))
				{
					Atom atom = (Atom) primitive;
					JSONObject atomJSON = Serialization.getInstance().toJsonObject(atom, Output.API);
					responseJSON.put("result", atomJSON);
					status(responseJSON, 200);
				}
				else if (BlockHeader.class.isInstance(primitive))
				{
					BlockHeader header = primitiveResult.getResult();
					JSONObject headerJSON = Serialization.getInstance().toJsonObject(header, Output.API);
					responseJSON.put("result", headerJSON);
					status(responseJSON, 200);
				}
				else if (Block.class.isInstance(primitive))
				{
					Block block = primitiveResult.getResult();
					JSONObject blockJSON = Serialization.getInstance().toJsonObject(block, Output.API);
					responseJSON.put("result", blockJSON);
					status(responseJSON, 200);
				}
				else if (AtomCertificate.class.isInstance(primitive))
				{
					AtomCertificate certificate = primitiveResult.getResult();
					JSONObject certificateJSON = Serialization.getInstance().toJsonObject(certificate, Output.API);
					responseJSON.put("result", certificateJSON);
					status(responseJSON, 200);
				}
				else if (PolyglotPackage.class.isInstance(primitive))
				{
					PolyglotPackage pakage = primitiveResult.getResult();
					JSONObject pakageJSON = Serialization.getInstance().toJsonObject(pakage, Output.API);
					responseJSON.put("result", pakageJSON);
					status(responseJSON, 200);
				}
				else if (Blob.class.isInstance(primitive))
				{
					Blob blob = primitiveResult.getResult();
					JSONObject blobJSON = Serialization.getInstance().toJsonObject(blob, Output.API);
					responseJSON.put("result", blobJSON);
					status(responseJSON, 200);
				}
				else
					status(500, "Primitive type "+primitive.getClass()+" not supported");
			}
		}
		catch(Throwable t)
		{
			status(responseJSON, 500, t.toString());
			apiLog.error("Failed to fetch primitive "+req.params("hash")+" of type "+req.params("type")+" - "+t.getMessage());
		}
		
		return responseJSON;
	}
	
	private JSONObject processSubstateSearch(Request req, Response res)
	{
		JSONObject responseJSON = new JSONObject();
		
		try
		{
			StateAddress stateAddress = StateAddress.from(req.params("context"), Hash.valueOf(decodeQueryValue(req.params("scope"))));
			Future<SubstateSearchResponse> substateSearchFuture = WebService.this.getContext(req).getLedger().get(new SubstateSearchQuery(stateAddress, Isolation.valueOf(req.queryParamOrDefault("isolation", "COMMITTED").toUpperCase())));
			SubstateSearchResponse substateSearchResponse = substateSearchFuture.get(5, TimeUnit.SECONDS);
			if (substateSearchResponse.getResult() == null) 
				status(responseJSON, 404, "Substate "+req.params("context")+":"+req.params("scope"));
			else
			{
				JSONObject substateJSON = Serialization.getInstance().toJsonObject(substateSearchResponse.getResult(), Output.API);
				responseJSON.put("result", substateJSON);
				status(responseJSON, 200);
			}
		}
		catch(Throwable t)
		{
			status(responseJSON, 500, t.toString());
			apiLog.error("Failed to fetch substate "+req.params("context")+":"+req.params("scope")+" - "+t.toString());
		}
		
		return responseJSON;
	}

	// NETWORK
	private void networkBootstrap()
	{
		spark.Spark.get(Configuration.getDefault().get("api.url", DEFAULT_API_PATH)+"/bootstrap", (req, res) -> 
		{
			final JSONObject responseJSON = new JSONObject();
			try
			{
				final Context context = getContext(req);
				final int offset = Integer.parseInt(req.queryParamOrDefault("offset", "0"));
				final int limit = Integer.parseUnsignedInt(req.queryParamOrDefault("limit", "100"));
				
				final List<Peer> peers;
				final InetAddress reqInetAddress = InetAddress.getByName(req.ip());
				if (reqInetAddress.isAnyLocalAddress() || reqInetAddress.isLoopbackAddress() ||
					reqInetAddress.isLinkLocalAddress() || reqInetAddress.isSiteLocalAddress())
				{
					final StandardDiscoveryFilter standardDiscoveryFilter = new StandardDiscoveryFilter(context);
					peers = context.getNetwork().getPeerHandler().get(standardDiscoveryFilter, offset, limit);
				}
				else
				{
					final NotLocalPeersFilter notLocalPeersFilter = new NotLocalPeersFilter(context.getNode());
					final StandardDiscoveryFilter standardDiscoveryFilter = new StandardDiscoveryFilter(context);
					peers = context.getNetwork().getPeerHandler().get(new MultiPeerFilter(notLocalPeersFilter, standardDiscoveryFilter), offset, limit);
				}
				
				if (peers.isEmpty())
				{
					status(responseJSON, 404, "No peers found");
				}
				else
				{
					final JSONArray peersArray  = new JSONArray();
					for (final Peer peer : peers)
						peersArray.put(peer.getURI());
					responseJSON.put("peers", peersArray);
					
					final boolean EOR = peers.size() != limit;
					status(responseJSON, 200, offset, EOR ? -1 : offset + peers.size(), limit, EOR);
				}
			}
			catch(Throwable t)
			{
				status(responseJSON, 500, t.toString());
				apiLog.error("Failed to process GET bootstrap", t);
			}
			
			return responseJSON.toString(4);
		});

		spark.Spark.post(Configuration.getDefault().get("api.url", DEFAULT_API_PATH)+"/bootstrap", (req, res) -> 
		{
			JSONObject responseJSON = new JSONObject();

			try
			{
				final Context context = getContext(req);
				final int offset = Integer.parseInt(req.queryParamOrDefault("offset", "0"));
				final int limit = Integer.parseUnsignedInt(req.queryParamOrDefault("limit", "100"));
				
				final List<Peer> peers;
				final InetAddress reqInetAddress = InetAddress.getByName(req.ip());
				if (reqInetAddress.isAnyLocalAddress() || reqInetAddress.isLoopbackAddress() ||
					reqInetAddress.isLinkLocalAddress() || reqInetAddress.isSiteLocalAddress())
				{
					final StandardDiscoveryFilter standardDiscoveryFilter = new StandardDiscoveryFilter(context);
					peers = context.getNetwork().getPeerHandler().get(standardDiscoveryFilter, offset, limit);
				}
				else
				{
					final NotLocalPeersFilter notLocalPeersFilter = new NotLocalPeersFilter(context.getNode());
					final StandardDiscoveryFilter standardDiscoveryFilter = new StandardDiscoveryFilter(context);
					peers = context.getNetwork().getPeerHandler().get(new MultiPeerFilter(notLocalPeersFilter, standardDiscoveryFilter), offset, limit);
				}
				
				if (peers.isEmpty())
				{
					status(responseJSON, 404, "No peers found");
				}
				else
				{
					final JSONArray peersArray  = new JSONArray();
					peers.forEach(peer -> peersArray.put(peer.getURI()));
					responseJSON.put("peers", peersArray);
					
					final boolean EOR = peers.size() != limit;
					status(responseJSON, 200, offset, EOR ? -1 : offset + peers.size(), limit, EOR);
				}
				
				// Check gateway status of requesting node and update peer store as a gateway if successful
				// TODO move to bootstrap module
				try
		        {
		            final String nodeIP = req.ip();
		            final JSONObject nodeJSON = new JSONObject(req.body());
		            final Node node = Serialization.getInstance().fromJsonObject(nodeJSON, Node.class);
		            final URI nodeURI = Agent.getURI(nodeIP, node);
		            
		            context.getNetwork().getPeerHandler().checkAccessible(nodeURI, node);
		        }
		        catch (Exception ex) 
				{
		            apiLog.warn("Failed to process peer update during bootstrap: ", ex);
		        }
			}
			catch(Throwable t)
			{
				status(responseJSON, 500, t.toString());
				apiLog.error("Failed to process POST bootstrap - ", t);
			}

			return responseJSON.toString(4);
		});
	}
	
	private void networkStatistics()
	{
		spark.Spark.get(Configuration.getDefault().get("api.url", DEFAULT_API_PATH)+"/network/statistics", (req, res) -> 
		{
			JSONObject responseJSON = new JSONObject();
			
			try
			{
				Context context = getContext(req);
				long timestamp = System.currentTimeMillis();

				JSONObject statistics = new JSONObject();
				statistics.put("connections", context.getNetwork().count(ConnectionState.CONNECTED));

				JSONObject bandwidth = new JSONObject();
				{
					JSONObject transferred = new JSONObject();
					transferred.put("inbound", context.getMetaData().get("network.transferred.inbound", 0l));
					transferred.put("outbound", context.getMetaData().get("network.transferred.outbound", 0l));
					bandwidth.put("transferred", transferred);
					
					JSONObject throughput = new JSONObject();
					throughput.put("inbound", context.getTimeSeries("bandwidth").average("inbound", timestamp-TimeUnit.SECONDS.toMillis(30), timestamp, TimeUnit.MILLISECONDS));
					throughput.put("outbound", context.getTimeSeries("bandwidth").average("outbound", timestamp-TimeUnit.SECONDS.toMillis(30), timestamp, TimeUnit.MILLISECONDS));
					bandwidth.put("throughput", throughput);
				}
				statistics.put("bandwidth", bandwidth);

				JSONObject messages = new JSONObject();
				{
					JSONObject transferred = new JSONObject();
					transferred.put("inbound", context.getMetaData().get("messaging.inbound", 0l));
					transferred.put("outbound", context.getMetaData().get("messaging.outbound", 0l));
					messages.put("transferred", transferred);
					
					JSONObject throughput = new JSONObject();
					throughput.put("inbound", context.getTimeSeries("messages").average("inbound", timestamp-TimeUnit.SECONDS.toMillis(30), timestamp, TimeUnit.MILLISECONDS));
					throughput.put("outbound", context.getTimeSeries("messages").average("outbound", timestamp-TimeUnit.SECONDS.toMillis(30), timestamp, TimeUnit.MILLISECONDS));
					messages.put("throughput", throughput);
				}
				statistics.put("messages", messages);
				
				JSONObject gossip = new JSONObject();
				{
					JSONObject queues = new JSONObject();
					queues.put("broadcast", context.getMetaData().get("gossip.broadcast.queue", 0l));
					queues.put("events", context.getMetaData().get("gossip.events.queue", 0l));
					gossip.put("queues", queues);
				}
				statistics.put("gossip", gossip);

				responseJSON.put("statistics", statistics);
				
				status(responseJSON, 200);
			}
			catch(Throwable t)
			{
				status(responseJSON, 500, t.toString());
				apiLog.error("Failed to fetch current network statistics", t);
			}

			return responseJSON.toString(4);
		});
	}

	
	private void networkConnections()
	{
		spark.Spark.get(Configuration.getDefault().get("api.url", DEFAULT_API_PATH)+"/network/connections", (req, res) -> 
		{
			JSONObject responseJSON = new JSONObject();
			
			try
			{
				Context context = getContext(req);
				
				JSONObject connections = new JSONObject();

				final ShardGroupID targetShardGroupID;
				if (req.queryParamOrDefault("shardgroup", "-1").equalsIgnoreCase("-1"))
					targetShardGroupID = null;
				else
					targetShardGroupID = ShardGroupID.from(req.queryParams("shardgroup"));

				if (targetShardGroupID == null)
				{
					final int numShardGroups = context.getLedger().numShardGroups();
					final ShardGroupID localShardGroupID = ShardMapper.toShardGroup(context.getNode().getIdentity(), numShardGroups);

					List<AbstractConnection> syncConnections = context.getNetwork().get(StandardConnectionFilter.build(context).setShardGroupID(localShardGroupID).setStates(ConnectionState.CONNECTED), false);
					JSONArray sync = new JSONArray();
					for (AbstractConnection syncConnection : syncConnections)
						sync.put(Serialization.getInstance().toJsonObject(syncConnection, Output.API));
					
					connections.put("sync", sync);
					
					JSONObject shardgroups = new JSONObject();
					for (int sg = 0 ; sg < numShardGroups ; sg++)
					{
						if (sg == localShardGroupID.intValue())
							continue;
						
						List<AbstractConnection> shardConnections = context.getNetwork().get(StandardConnectionFilter.build(context).setShardGroupID(ShardGroupID.from(sg)).setStates(ConnectionState.CONNECTED), false);
						JSONArray shardgroup = new JSONArray();
						for (AbstractConnection shardConnection : shardConnections)
							shardgroup.put(Serialization.getInstance().toJsonObject(shardConnection, Output.API));
						
						shardgroups.put(Integer.toString(sg), shardgroup);
					}
					connections.put("shardgroups", shardgroups);
				}
				else
				{
					JSONObject shardgroups = new JSONObject();
					{
						JSONArray shardgroup = new JSONArray();
						List<AbstractConnection> shardConnections = context.getNetwork().get(StandardConnectionFilter.build(context).setShardGroupID(targetShardGroupID).setStates(ConnectionState.CONNECTED));
						for (AbstractConnection shardConnection : shardConnections)
							shardgroup.put(Serialization.getInstance().toJsonObject(shardConnection, Output.API));
						shardgroups.put(Integer.toString(targetShardGroupID.intValue()), shardgroup);
					}						
					connections.put("shardgroups", shardgroups);
				}
				
				responseJSON.put("connections", connections);

				status(responseJSON, 200);
			}
			catch(Throwable t)
			{
				status(responseJSON, 500, t.toString());
				apiLog.error("Failed to fetch network connections", t);
			}

			return responseJSON.toString(4);
		});
	}
	
	private void networkKnown()
	{
		spark.Spark.get(Configuration.getDefault().get("api.url", DEFAULT_API_PATH)+"/network/known", (req, res) -> 
		{
			JSONObject responseJSON = new JSONObject();
			
			try
			{
				Context context = getContext(req);
				
				JSONObject knownJSON = new JSONObject();

				final ShardGroupID targetShardGroupID;
				if (req.queryParamOrDefault("shardgroup", "-1").equalsIgnoreCase("-1"))
					targetShardGroupID = null;
				else
					targetShardGroupID = ShardGroupID.from(req.queryParams("shardgroup"));
				
				final Collection<Peer> knownPeers = context.getNetwork().getPeerHandler().get(new NotLocalPeersFilter(context.getNode()).setBanned(true).setInvalid(true), 0, Short.MAX_VALUE);
				final int numShardGroups = context.getLedger().numShardGroups();
				for (final Peer knownPeer : knownPeers)
				{
					final ShardGroupID shardGroupID = ShardMapper.toShardGroup(knownPeer.getIdentity(), numShardGroups);
					if (targetShardGroupID != null && shardGroupID.equals(targetShardGroupID) == false)
						continue;
					
					JSONArray knownByShardGroupJSON = knownJSON.optJSONArray(Integer.toString(shardGroupID.intValue()));
					if (knownByShardGroupJSON == null)
					{
						knownByShardGroupJSON = new JSONArray();
						knownJSON.put(Integer.toString(shardGroupID.intValue()), knownByShardGroupJSON);
					}
					
					knownByShardGroupJSON.put(Serialization.getInstance().toJsonObject(knownPeer, Output.API));
				}
				
				responseJSON.put("known", knownJSON);

				status(responseJSON, 200);
			}
			catch(Throwable t)
			{
				status(responseJSON, 500, t.toString());
				apiLog.error("Failed to fetch network connections", t);
			}

			return responseJSON.toString(4);
		});
	}

	private void networkSearch()
	{
		spark.Spark.get(Configuration.getDefault().get("api.url", DEFAULT_API_PATH)+"/network/search", (req, res) -> 
		{
			JSONObject responseJSON = new JSONObject();
			
			try
			{
				Context context = getContext(req);
				Order order = Order.valueOf(req.queryParamOrDefault("order", Order.ASCENDING.toString()).toUpperCase());
				int offset = Integer.parseInt(req.queryParamOrDefault("offset", "0"));
				int limit = Integer.parseUnsignedInt(req.queryParamOrDefault("limit", "50"));
				
				StandardDiscoveryFilter discoveryFilter = new StandardDiscoveryFilter(context);
				if (req.queryParams("shardgroup") != null)
					discoveryFilter.setShardGroupID(ShardGroupID.from(req.queryParams("shardgroup")));
				if (req.queryParams("identity") != null)
					discoveryFilter.setIdentity(decodeQueryValue(req.queryParams("identity")));
				
				List<Peer> discoveredPeers = context.getNetwork().getPeerHandler().get(discoveryFilter, offset, limit);
				JSONArray peers = new JSONArray();
				for (Peer discoveredPeer : discoveredPeers)
					peers.put(Serialization.getInstance().toJsonObject(discoveredPeer, Output.API));
				responseJSON.put("peers", peers);
				
				int nextOffset = discoveredPeers.size() < limit ? -1 : offset + limit;
				status(responseJSON, 200, offset, nextOffset, limit, nextOffset == -1 ? true : false);
			}
			catch(Throwable t)
			{
				status(responseJSON, 500, t.toString());
				apiLog.error("Failed to fetch known peers", t);
			}
			
			return responseJSON.toString(4);
		});
	}
	
	// OTHER STATISTICS
	private void statisticsAll()
	{
		spark.Spark.get(Configuration.getDefault().get("api.url", DEFAULT_API_PATH)+"/statistics", (req, res) -> 
		{
			JSONObject responseJSON = new JSONObject();
			
			try
			{
				final Context context = getContext(req);
				final JSONObject statistics = new JSONObject();
				final SystemMetaData metaData = context.getMetaData();
				metaData.forEach((s, o) -> {
					if (o instanceof String value)
						statistics.put(s, value);
					else if (o instanceof Number value)
						statistics.put(s, value);
					else if (o instanceof byte[] value)
						statistics.put(s, Base64.getEncoder().encodeToString(value));
				});
				responseJSON.put("statistics", statistics);
				
				status(responseJSON, 200);
			}
			catch(Throwable t)
			{
				status(responseJSON, 500, t.toString());
				apiLog.error("Failed to fetch general statistics", t);
			}

			return responseJSON.toString(4);
		});
	}
	
	private void statisticsTimeSeries()
	{
		spark.Spark.get(Configuration.getDefault().get("api.url", DEFAULT_API_PATH)+"/statistics/timeseries/:name", (req, res) -> 
		{
			JSONObject responseJSON = new JSONObject();
			
			try
			{
				final Context context = getContext(req);
				final String timeSeriesName = req.params(":name");
				final TimeSeriesStatistics timeSeries = context.getTimeSeries(timeSeriesName);
				if (timeSeries == null)
					throw new UnsupportedOperationException("No timeseries statistics for "+req.params(":name"));

				final List<String> timeSeriesFields = timeSeries.getFields();
				final HashMap<String, String> trimmedFieldCache = new HashMap<>(timeSeriesFields.size());

				// Return last 5 minutes in 1 second intervals
				JSONArray statistics = new JSONArray();
				long start = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()) - TimeUnit.MINUTES.toSeconds(5);
				long end = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()) - TimeUnit.SECONDS.toSeconds(1);
				long timestamp = end;
				
				while(timestamp > start)
				{
					JSONObject record;
					
					try
					{
						if (timeSeriesName.equals("throughput"))
						{
							record = new JSONObject().put("timestamp", timestamp);
							for (int i = 0 ; i < timeSeriesFields.size() ; i++)
							{
								final String field = timeSeriesFields.get(i);
								record.put(field, timeSeries.average(field, timestamp-TimeUnit.SECONDS.toSeconds(30), timestamp, TimeUnit.SECONDS));
							}
						}
						else if (timeSeriesName.equals("executions"))
						{
							record = new JSONObject().put("timestamp", timestamp);

							JSONObject local = new JSONObject();
							JSONObject total = new JSONObject();
							record.put("local", local);
							record.put("total", total);
							
							for (int i = 0 ; i < timeSeriesFields.size() ; i++)
							{
								final String field = timeSeriesFields.get(i);
								if (field.startsWith("local:"))
									local.put(trimmedFieldCache.computeIfAbsent(field, k -> k.substring("local:".length())), timeSeries.average(field, timestamp-TimeUnit.SECONDS.toSeconds(30), timestamp, TimeUnit.SECONDS));
								else if (field.startsWith("total:"))
									total.put(trimmedFieldCache.computeIfAbsent(field, k -> k.substring("total:".length())), timeSeries.average(field, timestamp-TimeUnit.SECONDS.toSeconds(30), timestamp, TimeUnit.SECONDS));
								else
									throw new IllegalArgumentException("Field "+field+" is unsupported in execution timeseries");
							}
						}
						else if (timeSeriesName.equals("shardratio"))
						{
							double touchedAvgValue = timeSeries.average("touched", timestamp-TimeUnit.SECONDS.toSeconds(30), timestamp, TimeUnit.SECONDS);
							record = new JSONObject().put("timestamp", timestamp).put("touched", touchedAvgValue);
						}
						else  if (timeSeriesName.equals("proposals"))
						{
							double phaseValue = timeSeries.mean("phase", 30, timestamp, TimeUnit.SECONDS);
							double roundValue = timeSeries.mean("round", 30, timestamp, TimeUnit.SECONDS);
							double commitValue = timeSeries.mean("commit", 30, timestamp, TimeUnit.SECONDS);
							
							record = new JSONObject().put("timestamp", timestamp);
							record.put("phase", phaseValue);
							record.put("round", roundValue);
							record.put("commit", commitValue);
						}
						else  if (timeSeriesName.equals("finality"))
						{
							double clientValue = timeSeries.mean("client", 30, timestamp, TimeUnit.SECONDS);
							double consensusValue = timeSeries.mean("consensus", 30, timestamp, TimeUnit.SECONDS);
							
							try
							{
								record = new JSONObject().put("timestamp", timestamp);
								record.put("client", clientValue);
								record.put("consensus", consensusValue);
							}
							catch (Exception ex)
							{
								throw ex;
							}
						}
						else if (timeSeriesName.equals("bandwidth"))
						{
							record = new JSONObject().put("timestamp", timestamp);
							for (int i = 0 ; i < timeSeriesFields.size() ; i++)
							{
								final String field = timeSeriesFields.get(i);
								record.put(field, timeSeries.average(field, timestamp-TimeUnit.SECONDS.toSeconds(30), timestamp, TimeUnit.SECONDS));
							}
						}
						else if (timeSeriesName.equals("messages"))
						{
							record = new JSONObject().put("timestamp", timestamp);
							for (int i = 0 ; i < timeSeriesFields.size() ; i++)
							{
								final String field = timeSeriesFields.get(i);
								record.put(field, timeSeries.average(field, timestamp-TimeUnit.SECONDS.toSeconds(30), timestamp, TimeUnit.SECONDS));
							}
						}
						else
							throw new UnsupportedOperationException("No timeseries statistics for "+req.params("name"));
						
						statistics.put(record);
					}
					finally
					{
						timestamp -= TimeUnit.SECONDS.toSeconds(1);
					}
				}
				
				responseJSON.put(req.params("name"), statistics);
				
				status(responseJSON, 200);
			}
			catch(Throwable t)
			{
				status(responseJSON, 500, t.toString());
				apiLog.error("Failed to fetch time series statistics: "+req.params("name"), t);
			}

			return responseJSON.toString(4);
		});
	}
	
	// CONTENT
	private void webContent()
	{
		spark.Spark.head("/*", (req, res) -> 
		{
			final String endPoint = req.pathInfo().toLowerCase();
			
			final InputStream resourceStream = WebService.class.getResourceAsStream(endPoint);
		    if (resourceStream != null) 
		    {
		        try 
		        {
		            res.type(MimeTypes.getDefaultMimeByExtension(endPoint));
		            // Calculate total length for HEAD request
		            final byte[] buf = new byte[8192];
		            int totalLength = 0;
		            int length;
		            while ((length = resourceStream.read(buf)) > 0) 
		                totalLength += length;

		            res.header("Content-Length", String.valueOf(totalLength));
		            res.status(200);
		            resourceStream.close();
		            return "";
		        }
		        catch (Exception ex)
		        {
		            apiLog.error("HEAD: Failed to fetch embedded resource " + endPoint, ex);
		            res.status(500);
		            return "";
		        }
		    }
			
		    final String dir = Context.get().getConfiguration().get("web3.static.dir"); 
		    final String url = Context.get().getConfiguration().get("web3.static.url"); 
		    if (dir != null && url != null && endPoint.startsWith(url.toLowerCase()))
		    {
		    	final File staticFile = new File(dir+File.separatorChar+endPoint);
		        if (staticFile.exists() == false || staticFile.isDirectory())
		            res.status(404);
		        else
		        {
		            res.type(MimeTypes.getDefaultMimeByExtension(staticFile.getName()));
		            res.header("Content-Length", String.valueOf(staticFile.length()));
		            res.status(200);
		        }
		        
		        return "";
		    }
		    
			final StateAddress stateAddress;
			if (endPoint.startsWith(JacksonCodecConstants.HASH_STR_VALUE))
				stateAddress = StateAddress.from(Blob.class, Serialization.getInstance().<Hash>fromTypedString(endPoint.toLowerCase()));
			else
				stateAddress = StateAddress.from(Blob.class, endPoint.toLowerCase());
			
			final Future<SubstateSearchResponse> substateSearchResponseFuture = Context.get().getLedger().get(new SubstateSearchQuery(stateAddress, Isolation.valueOf(req.queryParamOrDefault("isolation", "COMMITTED").toUpperCase())));
			try
			{
				final SubstateSearchResponse substateSearchResponse = substateSearchResponseFuture.get(5, TimeUnit.SECONDS);
				if (substateSearchResponse.getResult() == null)
					res.status(404);
				else
				{
					final Atom atom = substateSearchResponse.getResult().getSubstate().get("primitive");
					if (atom == null)
						res.status(404);
					else
					{
						boolean found = false;
/*								for (HostedDataResource hostedDataResource : atom.getParticles(HostedDataResource.class))
							{
								if (hostedDataResource.getEndpoint().compareToIgnoreCase(endPoint) != 0)
									continue;
								
								boolean acceptRanges = (hostedDataResource.isStream() == false && hostedDataResource.totalChunks() == 1) ? false : true;
								res.type(hostedDataResource.getContentType());
								res.header("Content-Length",  hostedDataResource.isStream() ? "*" : String.valueOf(hostedDataResource.totalSize()));
								if (acceptRanges)
									res.header("Accept-Ranges",  "bytes");

								found = true;
								res.status(200);
								break;
							}*/
	
						if (found == false)
							res.status(404);
					}
				}
			}
			catch (TimeoutException toex)
			{
				res.status(408);
			}
			catch (Exception ex)
			{
				res.status(500);
				apiLog.error("HEAD: Failed to fetch endpoint "+endPoint, ex);
			}
			
			return "";
		});
		
		spark.Spark.get("/*", (req, res) -> 
		{
			final String endPoint = req.pathInfo().toLowerCase();
			
			final InputStream resourceStream = WebService.class.getResourceAsStream(endPoint);
		    if (resourceStream != null) 
		    {
		        try {
		            res.raw().setContentType(MimeTypes.getDefaultMimeByExtension(endPoint));
		            
		            final byte[] buf = new byte[8192];
		            int length;
		            while ((length = resourceStream.read(buf)) > 0)
		                res.raw().getOutputStream().write(buf, 0, length);
		            
		            res.raw().getOutputStream().flush();
		            res.status(200);
		            resourceStream.close();
		            return "";
		        }
		        catch (Exception ex)
		        {
		            res.status(500);
		            apiLog.error("Failed to fetch embedded resource " + endPoint + " " + ex.getMessage());
		            return "";
		        }
		    }
		    
		    final String dir = Context.get().getConfiguration().get("web3.static.dir"); 
		    final String url = Context.get().getConfiguration().get("web3.static.url"); 
		    if (dir != null && url != null && endPoint.startsWith(url.toLowerCase()))
		    {
		        final File staticFile = new File(dir + File.separatorChar + endPoint);
		        if (staticFile.exists() && !staticFile.isDirectory())
		        {
		            res.raw().setContentType(MimeTypes.getDefaultMimeByExtension(staticFile.getName()));
		            
		            try(final FileInputStream fis = new FileInputStream(staticFile))
		            {
		                final byte[] buf = new byte[8192];
		                int length;
		                while ((length = fis.read(buf)) > 0)
		                    res.raw().getOutputStream().write(buf, 0, length);
		                
		                res.raw().getOutputStream().flush();
		                res.status(200);
		                return "";
		            }
		            catch (Exception ex)
		            {
		                res.status(500);
		                apiLog.error("Failed to fetch static content "+endPoint+" "+ex.getMessage());
		                return "";
		            }
		        }
		    }
			
			return "";
		});
	}
	
	private JSONObject status(int code)
	{
		JSONObject status = new JSONObject();
		status.put("code", code);
		return status;
	}
	
	private JSONObject status(int code, long offset, long nextOffset, int limit, boolean eor)
	{
		JSONObject status = new JSONObject();
		status.put("code", code);
		status.put("offset", offset);
		status.put("next_offset", nextOffset);
		status.put("limit", limit);
		status.put("eor", eor);
		return status;
	}

	private JSONObject status(int code, String message)
	{
		JSONObject status = new JSONObject();
		status.put("code", code);
		if (message != null && message.isEmpty() == false)
			status.put("message", message);
		return status;
	}

	private JSONObject status(int code, String message, long offset, long nextOffset, int limit, boolean eor)
	{
		JSONObject status = new JSONObject();
		status.put("code", code);
		status.put("offset", offset);
		status.put("next_offset", nextOffset);
		status.put("limit", limit);
		status.put("eor", eor);
		if (message != null && message.isEmpty() == false)
			status.put("message", message);
		return status;
	}

	private void status(JSONObject response, int code)
	{
		JSONObject status = status(code);
		response.put("status", status);
	}

	private void status(JSONObject response, int code, String message)
	{
		JSONObject status = status(code, message);
		response.put("status", status);
	}
	
	private void status(JSONObject response, int code, long offset, long nextOffset, int limit, boolean eor)
	{
		JSONObject status = status(code, offset, nextOffset, limit, eor);
		response.put("status", status);
	}

	private void status(JSONObject response, int code, String message, long offset, long nextOffset, int limit, boolean eor)
	{
		JSONObject status = status(code, message, offset, nextOffset, limit, eor);
		response.put("status", status);
	}
	
	private Class<?> decodeQueryType(String value)
	{
		if (value.startsWith(JacksonCodecConstants.BYTE_STR_VALUE))
			return byte[].class;

		if (value.startsWith(JacksonCodecConstants.HASH_STR_VALUE))
			return Hash.class;
		
		if (value.startsWith(JacksonCodecConstants.IDENTITY_STR_VALUE))
			return Identity.class;

		if (value.startsWith(JacksonCodecConstants.U256_STR_VALUE))
			return UInt256.class;

		return String.class;
	}

	@SuppressWarnings("unchecked")
	private <T> T decodeQueryValue(String value) throws CryptoException
	{
		if (value.startsWith(JacksonCodecConstants.BYTE_STR_VALUE))
			return (T) Bytes.fromBase64String(value.substring(JacksonCodecConstants.STR_VALUE_LEN));

		if (value.startsWith(JacksonCodecConstants.HASH_STR_VALUE))
			return (T) Hash.from(value, JacksonCodecConstants.STR_VALUE_LEN);
		
		if (value.startsWith(JacksonCodecConstants.IDENTITY_STR_VALUE))
			return (T) Identity.from(value, JacksonCodecConstants.STR_VALUE_LEN);

		if (value.startsWith(JacksonCodecConstants.U256_STR_VALUE))
			return (T) UInt256.from(value, JacksonCodecConstants.STR_VALUE_LEN);

		if (value.startsWith(JacksonCodecConstants.STATE_ADDRESS_STR_VALUE))
			return (T) StateAddress.from(value, JacksonCodecConstants.STR_VALUE_LEN);

		return (T) value.toLowerCase();
	}
}
