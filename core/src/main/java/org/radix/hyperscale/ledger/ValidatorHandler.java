package org.radix.hyperscale.ledger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.Objects;
import java.util.Set;

import org.eclipse.collections.api.factory.Sets;
import org.radix.hyperscale.Constants;
import org.radix.hyperscale.Context;
import org.radix.hyperscale.Service;
import org.radix.hyperscale.Universe;
import org.radix.hyperscale.collections.Bloom;
import org.radix.hyperscale.collections.LRUCacheMap;
import org.radix.hyperscale.concurrency.MonitoredReadWriteLock;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.crypto.Identity;
import org.radix.hyperscale.crypto.PublicKey;
import org.radix.hyperscale.database.DatabaseException;
import org.radix.hyperscale.events.EventListener;
import org.radix.hyperscale.events.SynchronousEventListener;
import org.radix.hyperscale.exceptions.ServiceException;
import org.radix.hyperscale.exceptions.StartupException;
import org.radix.hyperscale.exceptions.TerminationException;
import org.radix.hyperscale.exceptions.ValidationException;
import org.radix.hyperscale.ledger.events.AtomCommitEvent;
import org.radix.hyperscale.ledger.events.BlockCommittedEvent;
import org.radix.hyperscale.ledger.events.SyncAtomCommitEvent;
import org.radix.hyperscale.ledger.events.SyncBlockEvent;
import org.radix.hyperscale.ledger.events.SyncPrepareEvent;
import org.radix.hyperscale.ledger.primitives.Atom;
import org.radix.hyperscale.ledger.primitives.Blob;
import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;
import org.radix.hyperscale.serialization.Serialization;
import org.radix.hyperscale.serialization.DsonOutput.Output;
import org.radix.hyperscale.utils.MathUtils;
import org.radix.hyperscale.utils.Numbers;

import com.google.common.collect.ImmutableList;
import com.google.common.eventbus.Subscribe;

public final class ValidatorHandler implements Service
{
	private static final Logger powerLog = Logging.getLogger("power");
	
	public static final long twoFPlusOne(long power)
	{
		long F = Math.max(1, power / 3);
		long TWOF = F * 2;
		long T = Math.min(power, TWOF + 1);
		return T;
	}
	
	private final class EpochShardKey
	{
		private final Epoch 		epoch;
		private final ShardGroupID	shardGroupID;
		
		EpochShardKey(final Epoch epoch, final ShardGroupID shardGroupID)
		{
			Objects.requireNonNull(epoch, "Epoch is null");
			Objects.requireNonNull(shardGroupID, "Shard group ID is null");
			
			this.epoch = epoch;
			this.shardGroupID = shardGroupID;
		}

		@Override
		public int hashCode()
		{
			final int prime = 31;
			int result = 1;
			result = prime * result + this.epoch.hashCode();
			result = prime * result + this.shardGroupID.hashCode();
			return result;
		}

		@Override
		public boolean equals(Object object)
		{
			if (this == object)
				return true;
			
			if (object == null)
				return false;
			
			if (object instanceof EpochShardKey epochShardKey)
			{
				if (this.epoch.equals(epochShardKey.epoch) == false)
					return false;
			
				if (this.shardGroupID.equals(epochShardKey.shardGroupID) == false)
					return false;

				return true;
			}
				
			return false;
		}
	}

	private final Context context;
	private final ValidatorStore validatorStore;
	
	private final Set<Identity> withVotePower;
	private final LRUCacheMap<Epoch, VotePowers> 	votePowerCache;
	private final LRUCacheMap<EpochShardKey, Long> 	votePowerTotalCache;
	
	private volatile Epoch pendingEpoch;
	private final Map<Identity, Long> accumulatorLocalEpochPowers;
	private final Map<Identity, Long> pendingLocalEpochPowers;
	private final Map<ShardGroupID, VotePowers> pendingGlobalEpochPowers;
	
	private final ReentrantReadWriteLock lock;

	ValidatorHandler(final Context context)
	{
		this.context = Objects.requireNonNull(context, "Context is null");
		this.votePowerCache = new LRUCacheMap<Epoch, VotePowers>(this.context.getConfiguration().get("ledger.vote.power.cache", 1<<10));
		this.votePowerTotalCache = new LRUCacheMap<EpochShardKey, Long>(this.context.getConfiguration().get("ledger.vote.power.total.cache", 1<<10));
		this.withVotePower = new HashSet<Identity>();
		
		this.accumulatorLocalEpochPowers = new HashMap<Identity, Long>();

		this.pendingLocalEpochPowers = new HashMap<Identity, Long>();
		this.pendingGlobalEpochPowers = new HashMap<ShardGroupID, VotePowers>();
		
		this.validatorStore = new ValidatorStore(context);
		
		this.lock = new MonitoredReadWriteLock(this.context.getName()+" Validator Handler Lock", true);
	}
	
	@Override
	public void start() throws StartupException
	{
		try
		{
			this.validatorStore.start();
			
			final Hash headHash = this.context.getLedger().getLedgerStore().head();
			
			// Setup Genesis
			if (headHash.equals(Universe.get().getGenesis().getHash()))
			{
				final Map<Identity, Long> genesisPowers = new HashMap<>();
				for (final Identity validator : Universe.get().getValidators())
				{
					powerLog.info(this.context.getName()+": Setting vote power for genesis validator "+validator.toString(Constants.TRUNCATED_IDENTITY_LENGTH)+":"+ShardMapper.toShardGroup(validator, Universe.get().shardGroupCount())+" to "+Constants.VOTE_POWER_BOOTSTRAP);
					genesisPowers.put(validator, Constants.VOTE_POWER_BOOTSTRAP);
				}
				
				for (long e = 0 ; e < Constants.VOTE_POWER_MATURITY_EPOCHS ; e++)
					this.validatorStore.store(new VotePowers(e, genesisPowers));
				
				final ShardGroupID localShardGroup = ShardMapper.toShardGroup(this.context.getNode().getIdentity(), Universe.get().shardGroupCount());
				final VotePowers genesisVotePowers = this.validatorStore.get(Epoch.from(Constants.VOTE_POWER_MATURITY_EPOCHS-1));
				for (final Entry<Identity, Long> validator : genesisVotePowers.getAll().entrySet())
				{
					if (ShardMapper.toShardGroup(validator.getKey(), Universe.get().shardGroupCount()).equals(localShardGroup))
						this.accumulatorLocalEpochPowers.put(validator.getKey(), validator.getValue());
					
					this.withVotePower.add(validator.getKey());
				}
				
				this.pendingEpoch = Epoch.from(Constants.VOTE_POWER_MATURITY_EPOCHS-1);
			}
			else
				this.pendingEpoch = Epoch.from(headHash).increment(Constants.VOTE_POWER_MATURITY_EPOCHS-1);
			
			this.context.getEvents().register(this.syncChangeListener);
			this.context.getEvents().register(this.syncBlockListener);
			this.context.getEvents().register(this.asyncAtomListener);
		}
		catch (Exception ex)
		{
			throw new StartupException(ex);
		}
	}

	@Override
	public void stop() throws TerminationException
	{
		this.context.getEvents().unregister(this.asyncAtomListener);
		this.context.getEvents().unregister(this.syncBlockListener);
		this.context.getEvents().unregister(this.syncChangeListener);
		this.context.getNetwork().getMessaging().deregisterAll(getClass());
		this.validatorStore.stop();
		this.withVotePower.clear();
		this.votePowerCache.clear();
		this.votePowerTotalCache.clear();
	}
	
	@Override
	public void clean() throws ServiceException
	{
		try
		{
			this.validatorStore.clean();
		}
		catch(Exception ex)
		{
			throw new ServiceException(ex, this.getClass());
		}
	}

	private VotePowers loadVotePower(final Epoch epoch) throws IOException
	{
		Objects.requireNonNull(epoch, "Epoch is null");
		
		VotePowers votePowers = this.votePowerCache.get(epoch);
		if (votePowers != null)
			return votePowers;

		votePowers = this.validatorStore.get(epoch);
		if (votePowers == null)
			throw new DatabaseException("Vote powers for epoch "+epoch.getClock()+" not found");
			
		this.votePowerCache.put(epoch, votePowers);
		
		return votePowers;
	}

	
	public VotePowers getVotePower(final Epoch epoch) throws IOException
	{
		Objects.requireNonNull(epoch, "Epoch is null");
		
		this.lock.readLock().lock();
		try
		{
			return loadVotePower(epoch);
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}

	public List<Identity> getIdentities() throws IOException
	{
		this.lock.readLock().lock();
		try
		{
			if (this.withVotePower.isEmpty())
				throw new IOException("Identities with vote power cache is empty");
			
			return ImmutableList.copyOf(this.withVotePower);
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}
	
	// NOTE Can use the identity cache directly providing that access outside of this function
	// wraps the returned set in a lock or a sync block
	public List<Identity> getIdentities(final Bloom identityBloom) throws IOException
	{
		Objects.requireNonNull(identityBloom, "Identity bloom is null");

		this.lock.readLock().lock();
		try
		{
			Set<Identity> identities = new HashSet<Identity>();
			for (Identity powerOwner : getIdentities())
			{
				if (identityBloom.contains(powerOwner.toByteArray()))
					identities.add(powerOwner);
			}
				
			return new ArrayList<Identity>(identities);
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}
	
	public <T extends PublicKey<?>> List<T> getKeys(final Bloom identityBloom) throws IOException
	{
		Objects.requireNonNull(identityBloom, "Identity bloom is null");

		this.lock.readLock().lock();
		try
		{
			Set<T> keys = new HashSet<T>();
			for (Identity powerOwner : getIdentities())
			{
				if (identityBloom.contains(powerOwner.toByteArray()))
					keys.add(powerOwner.getKey());
			}
				
			return new ArrayList<T>(keys);
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}

	public Set<Identity> getProposers(final long height, final Hash seed, final ShardGroupID shardGroupID) throws DatabaseException
	{
		Numbers.isNegative(height, "Height is negative");
		Objects.requireNonNull(seed, "Seed hash is null");
		Objects.requireNonNull(shardGroupID, "Shard group ID is null");

		this.lock.readLock().lock();
		try
		{
			final Epoch epoch = Epoch.from(height / Ledger.definitions().proposalsPerEpoch());
			final int numShardGroups = this.context.getLedger().numShardGroups(epoch);
			List<Identity> proposers = new ArrayList<Identity>();
			for (Identity proposer : this.withVotePower)
			{
				if (shardGroupID.equals(ShardMapper.toShardGroup(proposer, numShardGroups)) == false)
					continue;

				proposers.add(proposer); 
			}
			
			proposers.sort((i1, i2) -> {
				Hash hh = Hash.valueOf(height);
				Hash ph1 = Hash.hash(hh, seed, i1.getHash());
				Hash ph2 = Hash.hash(hh, seed, i2.getHash());
					
				return ph1.compareTo(ph2);
			});
			
			// Need minimum of 4 proposers per round to satisfy BFT 2f+1
			int numProposers = Math.max(4, MathUtils.log2(proposers.size()));
			if (numProposers > proposers.size())
				numProposers = proposers.size();
			
			return Sets.immutable.<Identity>ofAll(proposers.subList(0, numProposers)).castToSet();
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}

	// TODO need this to return any and all pending vote power?
	public long verify(final Epoch epoch, final Identity identity)
	{
		Objects.requireNonNull(identity, "Identity is null");
		Objects.requireNonNull(epoch, "Epoch is null");
		
		if (epoch.equals(this.pendingEpoch) == false)
			throw new IllegalStateException("Epoch "+epoch.getClock()+" is not the current pending epoch "+this.pendingEpoch.getClock());

		this.lock.readLock().lock();
		try
		{
			if (this.pendingLocalEpochPowers.isEmpty())
				throw new IllegalStateException("Epoch "+epoch.getClock()+" is not a in a current pending state");
			
			return this.pendingLocalEpochPowers.getOrDefault(identity, 0l);
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}


	public long getVotePower(final Epoch epoch, final Identity identity) throws IOException
	{
		Objects.requireNonNull(identity, "Identity is null");
		Objects.requireNonNull(epoch, "Epoch is null");

		this.lock.readLock().lock();
		try
		{
			if (this.withVotePower.contains(identity) == false)
				return 0l;
			
			final VotePowers votePowers = loadVotePower(epoch);
			return votePowers.get(identity);
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}
	
	public long getTotalVotePower(final Epoch epoch, final ShardGroupID shardGroupID) throws IOException
	{
		final EpochShardKey cacheKey = new EpochShardKey(epoch, shardGroupID);
		this.lock.readLock().lock();
		try
		{
			long totalVotePower = this.votePowerTotalCache.getOrDefault(cacheKey, -1l);
			if (totalVotePower > -1)
				return totalVotePower;
			
			final VotePowers votePowers = loadVotePower(epoch);
			final int numShardGroups = this.context.getLedger().numShardGroups(epoch);
			for (Entry<Identity, Long> powerEntry : votePowers.getAll().entrySet())
			{
				if (shardGroupID.equals(ShardMapper.toShardGroup(powerEntry.getKey(), numShardGroups)) == false)
					continue;

				totalVotePower += powerEntry.getValue(); 
			}
			
			this.votePowerTotalCache.put(cacheKey, totalVotePower);
			return totalVotePower;
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}

	public long getTotalVotePower(final Epoch epoch, final Set<ShardGroupID> shardGroupIDs) throws IOException
	{
		Objects.requireNonNull(epoch, "Epoch is null");
		Objects.requireNonNull(shardGroupIDs, "Shard group IDs is null");
		Numbers.isZero(shardGroupIDs.size(), "Shard group IDs is empty");

		this.lock.readLock().lock();
		try
		{
			long totalVotePower = 0;
			for (ShardGroupID shardGroupID : shardGroupIDs)
				totalVotePower += getTotalVotePower(epoch, shardGroupID);
			return totalVotePower;
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}
	
	public long getVotePower(final Epoch epoch, final Bloom identities) throws IOException
	{
		Objects.requireNonNull(epoch, "Epoch is null");
		Objects.requireNonNull(identities, "Identities is null");

		this.lock.readLock().lock();
		try
		{
			long votePower = 0l;
			final VotePowers votePowers = loadVotePower(epoch);
			for (Entry<Identity, Long> powerEntry : votePowers.getAll().entrySet())
			{
				if (identities.contains(powerEntry.getKey().toByteArray()))
					votePower += powerEntry.getValue();
			}
			
			return votePower;
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}

	public long getVotePower(final Epoch epoch, final Set<Identity> owners) throws IOException
	{
		Objects.requireNonNull(epoch, "Epoch is null");
		Objects.requireNonNull(owners, "Identities is null");

		this.lock.readLock().lock();
		try
		{
			long votePower = 0l;
			for (Identity owner : owners)
				votePower += getVotePower(epoch, owner);
			
			return votePower;
		}
		finally
		{
			this.lock.readLock().unlock();
		}
	}

	public long getVotePowerThreshold(final Epoch epoch, final ShardGroupID shardGroupID) throws IOException
	{
		return twoFPlusOne(getTotalVotePower(epoch, shardGroupID));
	}
	
	public long getVotePowerThreshold(final Epoch epoch, final Set<ShardGroupID> shardGroupIDs) throws IOException
	{
		Objects.requireNonNull(shardGroupIDs, "Shard group IDs is null");

		return twoFPlusOne(getTotalVotePower(epoch, shardGroupIDs));
	}
	
	void updateLocal(final Block block) throws IOException
	{
		Objects.requireNonNull(block, "Block for vote update is null");

		final Epoch currentEpoch = Epoch.from(block.getHeader());

		this.lock.writeLock().lock();
		try
		{
			// Increment the pending Epoch if the current one has elapsed and flip the vote powers into pending state
			if (block.getHeader().getHeight() % Ledger.definitions().proposalsPerEpoch() == 0)
			{
				this.pendingEpoch = currentEpoch.increment(Constants.VOTE_POWER_MATURITY_EPOCHS-1);
				this.pendingLocalEpochPowers.clear();
				this.pendingLocalEpochPowers.putAll(this.accumulatorLocalEpochPowers);
			}
			
			if (this.context.getLedger().isSynced())
			{
				// Spread the submission of Epoch validator messages over the course of the current Epoch to avoid a load spike
				if (getVotePower(currentEpoch, this.context.getNode().getIdentity()) > 0 && this.pendingLocalEpochPowers.isEmpty() == false && 
					block.getHeader().getHeight() % Ledger.definitions().proposalsPerEpoch() == Math.abs(this.context.getNode().getIdentity().getHash().asLong() % Ledger.definitions().proposalsPerEpoch()))
				{
					final int numShardGroups = this.context.getLedger().numShardGroups(currentEpoch);
					final ShardGroupID shardGroupID = ShardMapper.toShardGroup(this.context.getNode().getIdentity(), numShardGroups);

					try
					{
						final VotePowers pendingEpochVotePowers = new VotePowers(this.pendingEpoch.getClock(), this.pendingLocalEpochPowers);
	
						final Atom.Builder epochAtomBuilder = new Atom.Builder();
						final Blob votePowersBlob = new Blob("application/json", Serialization.getInstance().toJson(pendingEpochVotePowers, Output.WIRE));
						epochAtomBuilder.push(votePowersBlob.asDataURL());
						epochAtomBuilder.push("ledger::epoch("+this.pendingEpoch.getClock()+", "+shardGroupID+", hash('"+votePowersBlob.getHash()+"'))");
						epochAtomBuilder.signer(this.context.getNode().getKeyPair());
						this.context.getLedger().submit(epochAtomBuilder.build(Universe.get().getPrimitivePOW()));
							
						if (powerLog.hasLevel(Logging.DEBUG))
							powerLog.debug(ValidatorHandler.this.context.getName()+": Submitted local vote powers for epoch "+ValidatorHandler.this.pendingEpoch.getClock());
						
						// TODO update this.nextEpochPowers with validators which have shuffled in/out of the local nodes shards
						// NOTE remember it may have been this validator which has moved shards!
					}
					catch(Exception ex)
					{
						powerLog.error(this.context.getName()+": Failed to create epoch atom for epoch "+currentEpoch.getClock(), ex);
					}
				}
			}
			
			this.accumulatorLocalEpochPowers.compute(block.getHeader().getProposer(), (identity, power) -> power == null ? 1l : power+Constants.VOTE_POWER_PROPOSAL_REWARD);
		}
		finally
		{
			this.lock.writeLock().unlock();
		}
	}

	void updateRemote(final Block block) throws IOException
	{
		Objects.requireNonNull(block, "Block for vote update is null");
		
		// TODO reimplement once epoch based vote power distribution is completed 
	}
	
	private EventListener asyncAtomListener = new EventListener()
	{
		@Subscribe
		public void on(AtomCommitEvent event) 
		{
			if (event.getDecision().equals(CommitDecision.ACCEPT) == false)
				return;
			
			process(event.getProposalHeader(), event.getPendingAtom());
		}
		
		@Subscribe
		public void on(final SyncAtomCommitEvent event) 
		{
			if (event.getDecision().equals(CommitDecision.ACCEPT) == false)
				return;

			process(event.getProposalHeader(), event.getPendingAtom());
		}
		
		private void process(final BlockHeader proposalHeader, final PendingAtom pendingAtom)
		{
			ValidatorHandler.this.lock.writeLock().lock();
			try
			{
				final List<org.radix.hyperscale.ledger.sme.Epoch> epochInstructions = pendingAtom.getInstructions(org.radix.hyperscale.ledger.sme.Epoch.class);
				if (epochInstructions.isEmpty() == false)
				{
					// Should never throw, multiple epoch instructions should have been caught a lot sooner
					if (epochInstructions.size() > 1)
						throw new ValidationException(pendingAtom, "Multiple epoch instructions discovered");
					
					final int numShardGroups = ValidatorHandler.this.context.getLedger().numShardGroups(epochInstructions.get(0).getClock());
					final ShardGroupID localShardGroupID = ShardMapper.toShardGroup(ValidatorHandler.this.context.getNode().getIdentity(), numShardGroups);

					if (ValidatorHandler.this.pendingGlobalEpochPowers.containsKey(epochInstructions.get(0).getShardGroupID()))
						throw new ValidationException(pendingAtom, "Epoch powers already present for shard "+epochInstructions.get(0).getShardGroupID()+" at epoch "+epochInstructions.get(0).getClock());
						
					ValidatorHandler.this.pendingGlobalEpochPowers.put(epochInstructions.get(0).getShardGroupID(), epochInstructions.get(0).getPowers());
					
					// Completed?
					if (ValidatorHandler.this.pendingGlobalEpochPowers.size() == numShardGroups)
					{
						// Construct a merged vote power representation
						final Map<Identity, Long> mergedVotePowers = new HashMap<>();
						for (VotePowers votePowers : ValidatorHandler.this.pendingGlobalEpochPowers.values())
							mergedVotePowers.putAll(votePowers.getAll());
						
						final VotePowers mergedGlobalNextEpochVotePowers = new VotePowers(epochInstructions.get(0).getClock(), mergedVotePowers);
						ValidatorHandler.this.votePowerCache.put(Epoch.from(epochInstructions.get(0).getClock()), mergedGlobalNextEpochVotePowers);
						ValidatorHandler.this.withVotePower.addAll(mergedGlobalNextEpochVotePowers.getAll().keySet());
						ValidatorHandler.this.validatorStore.store(mergedGlobalNextEpochVotePowers);
						
						if (powerLog.hasLevel(Logging.DEBUG))
						{
							powerLog.debug(ValidatorHandler.this.context.getName()+": Completed vote powers for epoch "+ValidatorHandler.this.pendingEpoch.getClock());
							mergedGlobalNextEpochVotePowers.getAll().forEach((identity, power) -> powerLog.debug(ValidatorHandler.this.context.getName()+":  "+identity.toString(Constants.TRUNCATED_IDENTITY_LENGTH)+" = "+power));
						}
						
						ValidatorHandler.this.pendingGlobalEpochPowers.clear();
					}
					
					// Was for local shard, clear the pending as now completed
					if (localShardGroupID.equals(epochInstructions.get(0).getShardGroupID()))
						ValidatorHandler.this.pendingLocalEpochPowers.clear();
				}
			}
			catch (Exception ex)
			{
				powerLog.error(ValidatorHandler.this.context.getName()+": Failed to inspect / update vote powers in atom "+pendingAtom.getHash()+" committed in "+proposalHeader, ex);
			}
			finally
			{
				ValidatorHandler.this.lock.writeLock().unlock();
			}
		}
	};

	private SynchronousEventListener syncBlockListener = new SynchronousEventListener()
	{
		@Subscribe
		public void on(final BlockCommittedEvent event) 
		{
			try
			{
				updateLocal(event.getPendingBlock().getBlock());
			}
			catch (IOException ex)
			{
				powerLog.error(ValidatorHandler.this.context.getName()+": Failed to update vote powers in block "+event.getPendingBlock().getHeader(), ex);
			}
		}
		
		@Subscribe
		public void on(final SyncBlockEvent event) 
		{
			try
			{
				updateLocal(event.getBlock());
				updateRemote(event.getBlock());
			}
			catch (IOException ex)
			{
				powerLog.error(ValidatorHandler.this.context.getName()+": Failed to update vote powers in block "+event.getBlock().getHeader(), ex);
			}
		}
	};
	
	// SYNC CHANGE LISTENER //
	private SynchronousEventListener syncChangeListener = new SynchronousEventListener()
	{
		@Subscribe
		public void on(final SyncPrepareEvent event) throws IOException 
		{
			powerLog.info(ValidatorHandler.this.context.getName()+": Sync preparation required for validator handler");
			
			// Keep a copy of these for now for testing syncing
			Map<Identity, Long> accumulatingLocalEpochPowers = new HashMap<Identity, Long>();
			
			ValidatorHandler.this.accumulatorLocalEpochPowers.clear();
			ValidatorHandler.this.pendingLocalEpochPowers.clear();
			ValidatorHandler.this.pendingGlobalEpochPowers.clear();
			
			final int VOTE_POWERS_TO_LOAD_LOOKBACK = 3;
			final int COMMITTED_SOURCE_LOOKBACK = 2;
			final int PENDING_SOURCE_LOOKBACK = 1;
			final int ACCUMULATOR_SOURCE_LOOKBACK = 0;
			
			final Epoch headEpoch = Epoch.from(event.getHead());
			final Epoch committedSourceEpoch = headEpoch.getClock()-COMMITTED_SOURCE_LOOKBACK < 0 ? null : Epoch.from(headEpoch.getClock()-COMMITTED_SOURCE_LOOKBACK);
			final Epoch pendingSourceEpoch = headEpoch.getClock()-PENDING_SOURCE_LOOKBACK < 0 ? null : Epoch.from(headEpoch.getClock()-PENDING_SOURCE_LOOKBACK);
			final Epoch accumulatorSourceEpoch = headEpoch;
			
			final int numShardGroups = ValidatorHandler.this.context.getLedger().numShardGroups(headEpoch);
			final ShardGroupID localShardGroup = ShardMapper.toShardGroup(ValidatorHandler.this.context.getNode().getIdentity(), numShardGroups);
			
			final Epoch votePowersEpochToLoad = headEpoch.getClock()-COMMITTED_SOURCE_LOOKBACK < 0 ? Epoch.from(0) : headEpoch.increment(Constants.VOTE_POWER_MATURITY_EPOCHS-VOTE_POWERS_TO_LOAD_LOOKBACK);
			final VotePowers loadedEpochVotePowers = ValidatorHandler.this.validatorStore.get(votePowersEpochToLoad);
			for (final Entry<Identity, Long> validator : loadedEpochVotePowers.getAll().entrySet())
			{
				if (ShardMapper.toShardGroup(validator.getKey(), numShardGroups).equals(localShardGroup))
					accumulatingLocalEpochPowers.put(validator.getKey(), validator.getValue());
			}

			if (committedSourceEpoch != null)
			{
				final long committedFromHeight = Math.max(1, committedSourceEpoch.getClock() * Ledger.definitions().proposalsPerEpoch());			
				if (committedFromHeight >= 0)
				{
					final long committedToHeight = (committedSourceEpoch.getClock() + 1) * Ledger.definitions().proposalsPerEpoch();			
					for (long e = committedFromHeight ; e < committedToHeight ; e++)
					{
						final BlockHeader header = ValidatorHandler.this.context.getLedger().getBlockHeader(e);
						if (header == null)
							throw new IOException("Block header @ "+e+" not found for epoch reconstruction");
						
						accumulatingLocalEpochPowers.compute(header.getProposer(), (identity, power) -> power == null ? 1 : power+Constants.VOTE_POWER_PROPOSAL_REWARD);
					}
				}
			}

			if (pendingSourceEpoch != null)
			{
				final long pendingFromHeight = Math.max(1, pendingSourceEpoch.getClock() * Ledger.definitions().proposalsPerEpoch());			
				if (pendingFromHeight >= 0)
				{
					final long pendingToHeight = (pendingSourceEpoch.getClock() + 1) * Ledger.definitions().proposalsPerEpoch();			
					for (long e = pendingFromHeight ; e < pendingToHeight ; e++)
					{
						final BlockHeader header = ValidatorHandler.this.context.getLedger().getBlockHeader(e);
						if (header == null)
							throw new IOException("Block header @ "+e+" not found for epoch reconstruction");
						
						accumulatingLocalEpochPowers.compute(header.getProposer(), (identity, power) -> power == null ? 1 : power+Constants.VOTE_POWER_PROPOSAL_REWARD);
					}
				}
				
				ValidatorHandler.this.pendingLocalEpochPowers.putAll(accumulatingLocalEpochPowers);
			}

			final long accumulateFromHeight = Math.max(1, accumulatorSourceEpoch.getClock() * Ledger.definitions().proposalsPerEpoch());
			for (long e = accumulateFromHeight ; e <= event.getHead().getHeight() ; e++)
			{
				final BlockHeader header = ValidatorHandler.this.context.getLedger().getBlockHeader(e);
				if (header == null)
					throw new IOException("Block header @ "+e+" not  found for epoch reconstruction");
				
				accumulatingLocalEpochPowers.compute(header.getProposer(), (identity, power) -> power == null ? 1 : power+Constants.VOTE_POWER_PROPOSAL_REWARD);
			}
			ValidatorHandler.this.accumulatorLocalEpochPowers.putAll(accumulatingLocalEpochPowers);
			ValidatorHandler.this.withVotePower.addAll(loadedEpochVotePowers.getAll().keySet());
			
			ValidatorHandler.this.pendingEpoch = pendingSourceEpoch == null ? accumulatorSourceEpoch.increment(Constants.VOTE_POWER_MATURITY_EPOCHS-1) : 
																			  pendingSourceEpoch.increment(Constants.VOTE_POWER_MATURITY_EPOCHS);
		}
	};
}
