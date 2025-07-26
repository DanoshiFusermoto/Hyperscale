package org.radix.hyperscale.ledger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.radix.hyperscale.Context;
import org.radix.hyperscale.Service;
import org.radix.hyperscale.common.Order;
import org.radix.hyperscale.common.Primitive;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.exceptions.StartupException;
import org.radix.hyperscale.exceptions.TerminationException;
import org.radix.hyperscale.executors.Executable;
import org.radix.hyperscale.executors.Executor;
import org.radix.hyperscale.executors.ScheduledExecutable;
import org.radix.hyperscale.ledger.Substate.NativeField;
import org.radix.hyperscale.ledger.messages.AssociationSearchQueryMessage;
import org.radix.hyperscale.ledger.messages.AssociationSearchResponseMessage;
import org.radix.hyperscale.ledger.messages.PrimitiveSearchQueryMessage;
import org.radix.hyperscale.ledger.messages.PrimitiveSearchResponseMessage;
import org.radix.hyperscale.ledger.messages.SubstateSearchQueryMessage;
import org.radix.hyperscale.ledger.messages.SubstateSearchResponseMessage;
import org.radix.hyperscale.ledger.primitives.Atom;
import org.radix.hyperscale.ledger.primitives.AtomCertificate;
import org.radix.hyperscale.ledger.primitives.Blob;
import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;
import org.radix.hyperscale.network.AbstractConnection;
import org.radix.hyperscale.network.ConnectionState;
import org.radix.hyperscale.network.MessageProcessor;
import org.radix.hyperscale.network.StandardConnectionFilter;
import org.radix.hyperscale.serialization.SerializationException;
import org.radix.hyperscale.serialization.SerializerId2;

/**
 * Implementation of ledger search service for primitives, substates and associations
 * 
 * Currently only allows searches for committed elements. 
 *  
 * @author Dan
 *
 */
final class LedgerSearch implements Service, LedgerSearchInterface
{
	private static final Logger searchLog = Logging.getLogger("search");
	
	private final Context 	context;
	
	private final class PrimitiveSearchTask extends ScheduledExecutable
	{
		private final PrimitiveSearchQuery query;
		private final Map<ShardGroupID, PrimitiveSearchResponse> responses;
		private final CompletableFuture<PrimitiveSearchResponse> queryFuture;
		
		public PrimitiveSearchTask(PrimitiveSearchQuery query, long delay, TimeUnit unit)
		{
			super(delay, unit);
			
			this.query = Objects.requireNonNull(query, "Primitive search query is null");
			this.responses = Collections.synchronizedMap(new HashMap<ShardGroupID, PrimitiveSearchResponse>());
			this.queryFuture = new CompletableFuture<PrimitiveSearchResponse>();
		}
		
		public void response(ShardGroupID shardGroupID, PrimitiveSearchResponse response)
		{
			this.responses.put(shardGroupID, response);
			if (this.queryFuture.isDone())
				return;

			if (response.getResult() == null)
			{
				if (this.responses.size() == LedgerSearch.this.context.getLedger().numShardGroups())
				{
					this.queryFuture.complete(new PrimitiveSearchResponse(this.query));

					if (LedgerSearch.this.primitiveSearchTasks.remove(this.query.getHash(), this) == false)
						searchLog.error(LedgerSearch.this.context.getName()+": Primitive search query not found for removal "+this.query);
				}

				return;
			}
			
			this.queryFuture.complete(response);

			if (LedgerSearch.this.primitiveSearchTasks.remove(this.query.getHash(), this) == false)
				searchLog.error(LedgerSearch.this.context.getName()+": Primitive search query not found for removal "+this.query);
		}

		public Future<PrimitiveSearchResponse> getQueryFuture()
		{
			return this.queryFuture;
		}

		@Override
		public void execute()
		{
			if (this.queryFuture.isDone())
				return;
			
			this.queryFuture.completeExceptionally(new TimeoutException("Primitive search query timeout "+this.query));

			if (LedgerSearch.this.primitiveSearchTasks.remove(this.query.getHash(), this) == false)
				searchLog.error(LedgerSearch.this.context.getName()+": Primitive search query not found for removal "+this.query);
		}
	}

	private final class SubstateSearchTask extends ScheduledExecutable
	{
		private final SubstateSearchQuery query;
		private final CompletableFuture<SubstateSearchResponse> queryFuture;
		
		public SubstateSearchTask(SubstateSearchQuery query, long delay, TimeUnit unit)
		{
			super(delay, unit);
			
			this.query = Objects.requireNonNull(query, "Substate search query is null");
			this.queryFuture = new CompletableFuture<SubstateSearchResponse>();
		}
		
		public void response(SubstateSearchResponse response)
		{
			if (this.queryFuture.isDone())
				return;
			
			this.queryFuture.complete(response);
			if (LedgerSearch.this.substateSearchTasks.remove(this.query.getHash(), this) == false)
				searchLog.error(LedgerSearch.this.context.getName()+": Substate search query not found for removal "+this.query);
		}
		
		public Future<SubstateSearchResponse> getQueryFuture()
		{
			return this.queryFuture;
		}

		@Override
		public void execute()
		{
			if (this.queryFuture.isDone())
				return;
			
			this.queryFuture.completeExceptionally(new TimeoutException("Substate search query timeout "+this.query));
			
			if (LedgerSearch.this.substateSearchTasks.remove(this.query.getHash(), this) == false)
				searchLog.error(LedgerSearch.this.context.getName()+": Substate search query not found for removal "+this.query);
		}
	}
	
	private final class AssociationSearchTask extends ScheduledExecutable
	{
		private final AssociationSearchQuery query;
		private final Map<ShardGroupID, AssociationSearchResponse> responses;
		private final CompletableFuture<AssociationSearchResponse> queryFuture;

		public AssociationSearchTask(AssociationSearchQuery query, long delay, TimeUnit unit)
		{
			super(delay, unit);
			
			this.query = Objects.requireNonNull(query, "Association search query is null");
			this.responses = Collections.synchronizedMap(new HashMap<ShardGroupID, AssociationSearchResponse>());
			this.queryFuture = new CompletableFuture<AssociationSearchResponse>();
		}
		
		public void response(ShardGroupID shardGroupID, AssociationSearchResponse response)
		{
			if (this.queryFuture.isDone())
				return;

			this.responses.put(shardGroupID, response);
			if (this.responses.size() < LedgerSearch.this.context.getLedger().numShardGroups())
				return;
			
			build();
			
			if (LedgerSearch.this.associationSearchTasks.remove(this.query.getHash(), this) == false)
				searchLog.error(LedgerSearch.this.context.getName()+": Association search query not found for removal "+this.query);
		}
		
		private AssociationSearchQuery getQuery()
		{
			return this.query;
		}

		public Future<AssociationSearchResponse> getQueryFuture()
		{
			return this.queryFuture;
		}
		
		@Override
		public void execute()
		{
			if (this.queryFuture.isDone())
				return;
			
			// TODO return partials?
			this.queryFuture.completeExceptionally(new TimeoutException("Association search query timeout "+this.query));
			
			if (LedgerSearch.this.associationSearchTasks.remove(this.query.getHash(), this) == false)
				searchLog.error(LedgerSearch.this.context.getName()+": Association search query not found for removal "+this.query);
		}
		
		// FIXME deal with the fact that associations to a primitive can live on multiple shards @ different index heights.
		//		 Index heights are used to define the next offset, which may result in the same primitive being returned from 
		//		 the non-index shard group, or omitting some primitives altogether.
		//		 e.g A is @100 on SG1, A is @90 on SG2 with B @95 on SG2.  Using 100 as next offset may omit B on SG2 if relevant.
		private void build()
		{
			boolean isEOR = true;
			List<SubstateCommit> substateCommits = new ArrayList<SubstateCommit>();

			synchronized(this.responses)
			{
				for (AssociationSearchResponse response : this.responses.values())
				{
					substateCommits.addAll(response.getResults());
					
					if (response.isEOR() == false)
						isEOR = false;
				}
			}
			
			substateCommits.sort((ss1, ss2) -> {
				if (getQuery().getOrder().equals(Order.NATURAL))
				{
					long id1 = ss1.getID();
					long id2 = ss2.getID();

					if (id1 < id2)
						return -1;
					if (id1 > id2)
						return 1;
					
					return 0;
				}
				else
				{
					long idx1 = ss1.getIndex();
					long idx2 = ss2.getIndex();
						
					if (idx1 < idx2)
						return getQuery().getOrder().equals(Order.ASCENDING) ? -1 : 1;
						
					if (idx1 > idx2)
						return getQuery().getOrder().equals(Order.ASCENDING) ? 1 : -1;
						
					return 0;
				}
			});
			
			long nextOffset = -1;
			final Map<StateAddress, SubstateCommit> filteredSubstateResults = new LinkedHashMap<StateAddress, SubstateCommit>();
			for (final SubstateCommit substateCommit : substateCommits)
			{
				if (filteredSubstateResults.containsKey(substateCommit.getSubstate().getAddress()))
					continue;
				
				long thisOffset = this.query.getOrder().equals(Order.NATURAL) ? substateCommit.getID() : substateCommit.getIndex();
				if (nextOffset == -1 || 
					(getQuery().getOrder().equals(Order.DESCENDING) == false && thisOffset > nextOffset) ||
					(getQuery().getOrder().equals(Order.DESCENDING) == true && thisOffset < nextOffset))
					nextOffset = thisOffset;
				
				filteredSubstateResults.put(substateCommit.getSubstate().getAddress(), substateCommit);
				if (filteredSubstateResults.size() == getQuery().getLimit())
					break;
			}

			final List<SubstateCommit> finalizedSubstateResults;
			if (getQuery().getOrder().equals(Order.ASCENDING) || getQuery().getOrder().equals(Order.DESCENDING))
			{
				finalizedSubstateResults = new ArrayList<SubstateCommit>(filteredSubstateResults.values());

				// Now sort on the agreed timestamps
				finalizedSubstateResults.sort((ss1, ss2) -> {
					long idx0 = ss1.getTimestamp();
					long idx1 = ss2.getTimestamp();
	
					if (idx0 < idx1)
						return getQuery().getOrder().equals(Order.ASCENDING) ? -1 : 1;
						
					if (idx0 > idx1)
						return getQuery().getOrder().equals(Order.ASCENDING) ? 1 : -1;
						
					return 0;
				});
			}
			else
				finalizedSubstateResults = new ArrayList<SubstateCommit>(filteredSubstateResults.values());

			this.queryFuture.complete(new AssociationSearchResponse(getQuery(), nextOffset, finalizedSubstateResults, isEOR));
		}
	}

	private final Map<Hash, SubstateSearchTask> substateSearchTasks = Collections.synchronizedMap(new LinkedHashMap<Hash, SubstateSearchTask>());
	private final Map<Hash, PrimitiveSearchTask> primitiveSearchTasks = Collections.synchronizedMap(new LinkedHashMap<Hash, PrimitiveSearchTask>());
	private final Map<Hash, AssociationSearchTask> associationSearchTasks = Collections.synchronizedMap(new LinkedHashMap<Hash, AssociationSearchTask>());
	private final ExecutorService searchExecutor = Executors.newFixedThreadPool(1); // TODO want more than one? Also need a factory 
	
	public LedgerSearch(Context context)
	{
		this.context = Objects.requireNonNull(context);
	}

	@Override
	public void start() throws StartupException
	{
		this.context.getNetwork().getMessaging().register(PrimitiveSearchQueryMessage.class, this.getClass(), new MessageProcessor<PrimitiveSearchQueryMessage>()
		{
			@Override
			public void process(final PrimitiveSearchQueryMessage primitiveSearchQueryMessage, final AbstractConnection connection)
			{
				LedgerSearch.this.searchExecutor.execute(new Executable() 
				{
					@Override
					public void execute()
					{
						try
						{
							PrimitiveSearchResponse primitiveSearchResponse;
							PrimitiveSearchResponseMessage primitiveSearchResponseMessage; 
							try
							{
								primitiveSearchResponse = LedgerSearch.this.doPrimitiveSearch(primitiveSearchQueryMessage.getQuery());
								primitiveSearchResponseMessage = new PrimitiveSearchResponseMessage(primitiveSearchResponse); 
							}
							catch (SerializationException sex)
							{
								// FIXME hack to respond with null if de-serialization type was incorrect 
								primitiveSearchResponseMessage = new PrimitiveSearchResponseMessage(new PrimitiveSearchResponse(primitiveSearchQueryMessage.getQuery())); 
							}
							
							LedgerSearch.this.context.getNetwork().getMessaging().send(primitiveSearchResponseMessage, connection);
						}
						catch (Exception ex)
						{
							searchLog.error(LedgerSearch.this.context.getName()+": ledger.messages.search.query.primitive " + connection, ex);
						}
					}
				});
			}
		});

		this.context.getNetwork().getMessaging().register(PrimitiveSearchResponseMessage.class, this.getClass(), new MessageProcessor<PrimitiveSearchResponseMessage>()
		{
			@Override
			public void process(final PrimitiveSearchResponseMessage primitiveSearchResponseMessage, final AbstractConnection connection)
			{
				LedgerSearch.this.searchExecutor.execute(new Executable() 
				{
					@Override
					public void execute()
					{
						try
						{
							if (searchLog.hasLevel(Logging.DEBUG))
								searchLog.debug(LedgerSearch.this.context.getName()+": Primitive response for "+primitiveSearchResponseMessage.getResponse().getQuery()+" returning "+primitiveSearchResponseMessage.getResponse().getResult()+" from " + connection);
							
							final PrimitiveSearchTask primitiveSearchTask = LedgerSearch.this.primitiveSearchTasks.get(primitiveSearchResponseMessage.getResponse().getQuery().getHash());
							if (primitiveSearchTask == null)
							{
								searchLog.error(LedgerSearch.this.context.getName()+": Received response for primitive "+primitiveSearchResponseMessage.getResponse().getQuery()+" not found or completed from "+connection);
								return;
							}
							
							primitiveSearchTask.response(ShardMapper.toShardGroup(connection.getNode().getIdentity(), LedgerSearch.this.context.getLedger().numShardGroups()), primitiveSearchResponseMessage.getResponse());
						}
						catch (Exception ex)
						{
							searchLog.error(LedgerSearch.this.context.getName()+": ledger.messages.search.response.primitive " + connection, ex);
						}
					}
				});
			}
		});

		this.context.getNetwork().getMessaging().register(SubstateSearchQueryMessage.class, this.getClass(), new MessageProcessor<SubstateSearchQueryMessage>()
		{
			@Override
			public void process(final SubstateSearchQueryMessage substateSearchQueryMessage, final AbstractConnection connection)
			{
				LedgerSearch.this.searchExecutor.execute(new Executable() 
				{
					@Override
					public void execute()
					{
						try
						{
							final SubstateSearchResponse substateSearchResponse = LedgerSearch.this.doSubstateSearch(substateSearchQueryMessage.getQuery());
							final SubstateSearchResponseMessage substateSearchResponseMessage = new SubstateSearchResponseMessage(substateSearchResponse); 
							
							if (searchLog.hasLevel(Logging.DEBUG))
								searchLog.debug(LedgerSearch.this.context.getName()+": Search response for substate "+substateSearchResponse.getQuery()+" returning "+substateSearchResponse.getResult()+" to " + connection);

							LedgerSearch.this.context.getNetwork().getMessaging().send(substateSearchResponseMessage, connection);
						}
						catch (Exception ex)
						{
							searchLog.error(LedgerSearch.this.context.getName()+": "+SubstateSearchQueryMessage.class.getAnnotation(SerializerId2.class).value()+" " + connection, ex);
						}
					}
				});
			}
		});

		this.context.getNetwork().getMessaging().register(SubstateSearchResponseMessage.class, this.getClass(), new MessageProcessor<SubstateSearchResponseMessage>()
		{
			@Override
			public void process(final SubstateSearchResponseMessage substateSearchResponseMessage, final AbstractConnection connection)
			{
				LedgerSearch.this.searchExecutor.execute(new Executable() 
				{
					@Override
					public void execute()
					{
						try
						{
							if (searchLog.hasLevel(Logging.DEBUG))
								searchLog.debug(LedgerSearch.this.context.getName()+": Search response for substate "+substateSearchResponseMessage.getResponse().getQuery()+" returning "+substateSearchResponseMessage.getResponse().getResult()+" from " + connection);
							
							final SubstateSearchTask substateSearchTask = LedgerSearch.this.substateSearchTasks.get(substateSearchResponseMessage.getResponse().getQuery().getHash());
							if (substateSearchTask == null)
							{
								searchLog.error(LedgerSearch.this.context.getName()+": Received response for substate "+substateSearchResponseMessage.getResponse().getQuery()+" not found or completed from "+connection);
								return;
							}
								
							substateSearchTask.response(substateSearchResponseMessage.getResponse());
						}
						catch (Exception ex)
						{
							searchLog.error(LedgerSearch.this.context.getName()+": "+SubstateSearchResponseMessage.class.getAnnotation(SerializerId2.class).value()+" " + connection, ex);
						}
					}
				});
			}
		});
		
		this.context.getNetwork().getMessaging().register(AssociationSearchQueryMessage.class, this.getClass(), new MessageProcessor<AssociationSearchQueryMessage>()
		{
			@Override
			public void process(final AssociationSearchQueryMessage associationSearchQueryMessage, final AbstractConnection connection)
			{
				LedgerSearch.this.searchExecutor.execute(new Executable() 
				{
					@Override
					public void execute()
					{
						try
						{
							AssociationSearchResponse associationSearchResponse = LedgerSearch.this.doAssociationSearch(associationSearchQueryMessage.getQuery());
							AssociationSearchResponseMessage associationSearchResponseMessage = new AssociationSearchResponseMessage(associationSearchResponse); 
							LedgerSearch.this.context.getNetwork().getMessaging().send(associationSearchResponseMessage, connection);
						}
						catch (Exception ex)
						{
							searchLog.error(LedgerSearch.this.context.getName()+": ledger.messages.search.query.association " + connection, ex);
						}
					}
				});
			}
		});

		this.context.getNetwork().getMessaging().register(AssociationSearchResponseMessage.class, this.getClass(), new MessageProcessor<AssociationSearchResponseMessage>()
		{
			@Override
			public void process(final AssociationSearchResponseMessage associationSearchResponseMessage, final AbstractConnection connection)
			{
				LedgerSearch.this.searchExecutor.execute(new Executable() 
				{
					@Override
					public void execute()
					{
						try
						{
							if (searchLog.hasLevel(Logging.DEBUG))
								searchLog.debug(LedgerSearch.this.context.getName()+": Search response for association "+associationSearchResponseMessage.getResponse().getQuery()+" returning "+associationSearchResponseMessage.getResponse().getResults().size()+" results from " + connection);
							
							final AssociationSearchTask associationSearchTask = LedgerSearch.this.associationSearchTasks.get(associationSearchResponseMessage.getResponse().getQuery().getHash());
							if (associationSearchTask == null)
							{
								searchLog.error(LedgerSearch.this.context.getName()+": Received response for association "+associationSearchResponseMessage.getResponse().getQuery()+" not found or completed from "+connection);
								return;
							}
							
							associationSearchTask.response(ShardMapper.toShardGroup(connection.getNode().getIdentity(), LedgerSearch.this.context.getLedger().numShardGroups()), associationSearchResponseMessage.getResponse());
						}
						catch (Exception ex)
						{
							searchLog.error(LedgerSearch.this.context.getName()+": ledger.messages.search.response.association " + connection, ex);
						}
					}
				});
			}
		});

	}

	@Override
	public void stop() throws TerminationException
	{
		// TODO Auto-generated method stub
		
	}
	
	PrimitiveSearchResponse doPrimitiveSearch(final PrimitiveSearchQuery query) throws IOException
	{
		if (searchLog.hasLevel(Logging.DEBUG))
			searchLog.debug(LedgerSearch.this.context.getName()+": Search for primitive "+query.getQuery()+" of type "+query.getType());
		
		if (query.getIsolation().equals(Isolation.PENDING))
		{
			Future<PrimitiveSearchResponse> pendingResult = this.context.getLedger().getAtomHandler().get(query);
			if (pendingResult != null)
				try { return pendingResult.get(); } catch (Throwable t) { /* DONT CARE */ }
		}
		
		Primitive primitive = null;
		if (query.getType().equals(Blob.class))
		{
			// Blobs are wrapped in atoms, not stored explicitly
			// Need to fetch the atom then extract the blob
			// TODO is this the best way?
			// TODO what about data-sharding?
			
			StateAddress stateAddress = StateAddress.from(query.getType(), query.getQuery());
			SubstateCommit substateCommit = LedgerSearch.this.context.getLedger().getLedgerStore().search(stateAddress);
			if (substateCommit != null && substateCommit.getSubstate().get(NativeField.ATOM) != null)
			{
				Atom atom = LedgerSearch.this.context.getLedger().getLedgerStore().get(substateCommit.getSubstate().<Hash>get(NativeField.ATOM), Atom.class);
				primitive = atom.get(query.getQuery());
			}
		}
		else
			primitive = LedgerSearch.this.context.getLedger().getLedgerStore().get(query.getQuery(), query.getType());
		
		if (primitive != null)
		{
			if (primitive.getClass().equals(Block.class) || primitive.getClass().equals(BlockHeader.class))
			{
			}
			else if (primitive.getClass().equals(Atom.class))
			{
				// If atom type make sure it was processed by local shard group as local replica may have just been a relay
				// TODO should be able to remove this if atom persistence process ever changes from active to passive (storing AFTER accept)
				Substate atomSubstate = this.context.getLedger().getLedgerStore().get(StateAddress.from(Atom.class, primitive.getHash()));
				if (atomSubstate.isVoid())
					return new PrimitiveSearchResponse(query);
			}
			else if (primitive.getClass().equals(AtomCertificate.class))
			{
				
			}
			else if (primitive.getClass().equals(Blob.class))
			{
				
			}

			return new PrimitiveSearchResponse(query, primitive);
		}
		
		return new PrimitiveSearchResponse(query);
	}

	SubstateSearchResponse doSubstateSearch(final SubstateSearchQuery query) throws IOException
	{
		if (searchLog.hasLevel(Logging.DEBUG))
			searchLog.debug(LedgerSearch.this.context.getName()+": Search for substate "+query.getAddress());
		
		if (query.getIsolation().equals(Isolation.PENDING))
		{
			Future<SubstateSearchResponse> pendingResult = this.context.getLedger().getAtomHandler().get(query);
			if (pendingResult != null)
				try { return pendingResult.get(); } catch (Throwable t) { /* DONT CARE */ }
		}
		
		SubstateCommit substateCommit = LedgerSearch.this.context.getLedger().getLedgerStore().search(query.getAddress());
		if (substateCommit != null)
			return new SubstateSearchResponse(query, substateCommit);

		return new SubstateSearchResponse(query);
	}

	AssociationSearchResponse doAssociationSearch(final AssociationSearchQuery query) throws IOException
	{
		if (searchLog.hasLevel(Logging.DEBUG))
			searchLog.debug(LedgerSearch.this.context.getName()+": Search for Association "+query);
		
		Collection<SubstateCommit> localSubstateCommits = LedgerSearch.this.context.getLedger().getLedgerStore().search(query);
		if (localSubstateCommits.isEmpty() == false)
		{
			long nextOffset = query.getOffset();
			for (SubstateCommit substateCommit : localSubstateCommits)
			{
				if (substateCommit.getIndex() <= nextOffset)
					searchLog.warn(this.context.getName()+": Association search result substate commit index "+substateCommit.getIndex()+" is less than or equal to current next offset "+nextOffset);

				nextOffset = substateCommit.getIndex();
			}

			return new AssociationSearchResponse(query, nextOffset, localSubstateCommits, localSubstateCommits.size() < query.getLimit());
		}
		
		return new AssociationSearchResponse(query);
	}

	@Override
	public Future<PrimitiveSearchResponse> get(final PrimitiveSearchQuery query, final long timeout, final TimeUnit timeUnit) 
	{
		ShardGroupID localShardGroupID = ShardMapper.toShardGroup(this.context.getNode().getIdentity(), this.context.getLedger().numShardGroups());
		try
		{
			synchronized(this.primitiveSearchTasks)
			{
				PrimitiveSearchTask primitiveSearchTask = this.primitiveSearchTasks.get(query.getHash());
				if (primitiveSearchTask == null)
				{					
					primitiveSearchTask = new PrimitiveSearchTask(query, timeout, timeUnit);
					this.primitiveSearchTasks.put(query.getHash(), primitiveSearchTask);
					Executor.getInstance().schedule(primitiveSearchTask);
					
					PrimitiveSearchResponse localPrimitiveSearchResponse;
					try
					{
						localPrimitiveSearchResponse = LedgerSearch.this.doPrimitiveSearch(query);
					}
					catch (SerializationException sex)
					{
						// 	FIXME hack to respond with null if de-serialization type was incorrect 
						localPrimitiveSearchResponse =new PrimitiveSearchResponse(query); 
					}
					primitiveSearchTask.response(localShardGroupID, localPrimitiveSearchResponse);
					
					if (localPrimitiveSearchResponse.getResult() == null)
					{
						// Remote search
						// TODO brute force search of all shard groups ... inefficient
						for (int searchShardGroupID = 0 ; searchShardGroupID < this.context.getLedger().numShardGroups() ; searchShardGroupID++)
						{
							if (searchShardGroupID == localShardGroupID.intValue())
								continue;
							
							StandardConnectionFilter standardPeerFilter = StandardConnectionFilter.build(this.context).setStates(ConnectionState.SELECT_CONNECTED).setShardGroupID(ShardGroupID.from(searchShardGroupID)).setSynced(true);
							Collection<AbstractConnection> connectedPeers = this.context.getNetwork().get(standardPeerFilter);
							if (connectedPeers.isEmpty())
							{
								searchLog.error(this.context.getName()+": No peers available to query primitive "+query+" @ shard group ID "+searchShardGroupID);
								continue;
							}
						
							for (AbstractConnection connectedPeer : connectedPeers)
							{
								try
								{
									PrimitiveSearchQueryMessage primitiveSearchQueryMessage = new PrimitiveSearchQueryMessage(query);
									this.context.getNetwork().getMessaging().send(primitiveSearchQueryMessage, connectedPeer);
									break;
								}
								catch (IOException ex)
								{
									searchLog.error(this.context.getName()+": Unable to send PrimitiveSearchQueryMessage for "+query+" to " + connectedPeer, ex);
								}
							}
						}
					}
				}

				return primitiveSearchTask.getQueryFuture();
			}
		}
		catch (Throwable t)
		{
			CompletableFuture<PrimitiveSearchResponse> future = new CompletableFuture<>();
			future.completeExceptionally(t);
			return future;
		}
	}

	@Override
	public Future<SubstateSearchResponse> get(final SubstateSearchQuery query, final long timeout, final TimeUnit timeUnit)
	{
		try
		{
			ShardGroupID localShardGroupID = ShardMapper.toShardGroup(this.context.getNode().getIdentity(), this.context.getLedger().numShardGroups());
			ShardGroupID searchShardGroupID = ShardMapper.toShardGroup(query.getAddress(), this.context.getLedger().numShardGroups());
			if (localShardGroupID.equals(searchShardGroupID) || query.isLocalOnly())
			{
				if (query.getIsolation().equals(Isolation.PENDING))
				{
					Future<SubstateSearchResponse> pendingResult = this.context.getLedger().getAtomHandler().get(query);
					if (pendingResult != null)
						return pendingResult;
				}
				
				SubstateCommit substateCommit = LedgerSearch.this.context.getLedger().getLedgerStore().search(query.getAddress());
				if (substateCommit == null)
					return CompletableFuture.completedFuture(new SubstateSearchResponse(query));
				
				return CompletableFuture.completedFuture(new SubstateSearchResponse(query, substateCommit));
			}
			else	
			{
				StandardConnectionFilter standardPeerFilter = StandardConnectionFilter.build(this.context).setStates(ConnectionState.SELECT_CONNECTED).setShardGroupID(searchShardGroupID).setSynced(true);
				Collection<AbstractConnection> connectedStatePeers = this.context.getNetwork().get(standardPeerFilter);
				if (connectedStatePeers.isEmpty())
					throw new IOException(this.context.getName()+": No peers available to query substate "+query.getAddress()+" @ shard group ID "+searchShardGroupID);

				AtomicBoolean isExisting = new AtomicBoolean(true);
				SubstateSearchTask substateSearchTask = this.substateSearchTasks.computeIfAbsent(query.getHash(), k -> {
					SubstateSearchTask newSubstateSearchTask = new SubstateSearchTask(query, timeout, timeUnit); 
					Executor.getInstance().schedule(newSubstateSearchTask);
					isExisting.set(false);
					return newSubstateSearchTask;
				});
					
				if (isExisting.get() == false)
				{					
	    			for (AbstractConnection connectedPeer : connectedStatePeers)
					{
						try
						{
							SubstateSearchQueryMessage substateSearchQueryMessage = new SubstateSearchQueryMessage(query);
							this.context.getNetwork().getMessaging().send(substateSearchQueryMessage, connectedPeer);
							break;
						}
						catch (IOException ex)
						{
							searchLog.error(this.context.getName()+": Unable to send "+SubstateSearchQueryMessage.class.getAnnotation(SerializerId2.class)+" for "+query.getAddress()+" to " + connectedPeer, ex);
						}
					}
				}
				
				return substateSearchTask.getQueryFuture();
			}
		}
		catch (Throwable t)
		{
			CompletableFuture<SubstateSearchResponse> future = new CompletableFuture<>();
			future.completeExceptionally(t);
			return future;
		}			
	}
	
	@Override
	public Future<AssociationSearchResponse> get(final AssociationSearchQuery query, final long timeout, final TimeUnit timeUnit)
	{
		ShardGroupID localShardGroupID = ShardMapper.toShardGroup(this.context.getNode().getIdentity(), this.context.getLedger().numShardGroups());
		try
		{
			synchronized(this.associationSearchTasks)
			{
				AssociationSearchTask associationSearchTask = this.associationSearchTasks.get(query.getHash());
				if (associationSearchTask == null)
				{					
					associationSearchTask = new AssociationSearchTask(query, timeout, timeUnit);
					this.associationSearchTasks.put(associationSearchTask.getQuery().getHash(), associationSearchTask);
					Executor.getInstance().schedule(associationSearchTask);

					associationSearchTask.response(localShardGroupID, doAssociationSearch(query));
					
					// Remote search
					// TODO brute force search of all shard groups ... inefficient
					for (int searchShardGroupID = 0 ; searchShardGroupID < this.context.getLedger().numShardGroups() ; searchShardGroupID++)
					{
						if (searchShardGroupID == localShardGroupID.intValue())
							continue;
						
						StandardConnectionFilter standardPeerFilter = StandardConnectionFilter.build(this.context).setStates(ConnectionState.SELECT_CONNECTED).setShardGroupID(ShardGroupID.from(searchShardGroupID)).setSynced(true);
						Collection<AbstractConnection> connectedPeers = this.context.getNetwork().get(standardPeerFilter);
						if (connectedPeers.isEmpty())
						{
							searchLog.error(this.context.getName()+": No peers available to query associations "+query+" @ shard group ID "+searchShardGroupID);
							continue;
						}
					
						for (AbstractConnection connectedPeer : connectedPeers)
						{
							try
							{
								AssociationSearchQueryMessage associationSearchQueryMessage = new AssociationSearchQueryMessage(query);
								this.context.getNetwork().getMessaging().send(associationSearchQueryMessage, connectedPeer);
								break;
							}
							catch (IOException ex)
							{
								searchLog.error(this.context.getName()+": Unable to send AssociationSearchQueryMessage for "+query+" to " + connectedPeer, ex);
							}
						}
					}
				}

				return associationSearchTask.getQueryFuture();
			}
		}
		catch (Throwable t)
		{
			CompletableFuture<AssociationSearchResponse> future = new CompletableFuture<>();
			future.completeExceptionally(t);
			return future;
		}
	}
}
