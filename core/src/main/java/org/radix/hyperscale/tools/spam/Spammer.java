package org.radix.hyperscale.tools.spam;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.Range;
import org.radix.hyperscale.Constants;
import org.radix.hyperscale.Context;
import org.radix.hyperscale.Universe;
import org.radix.hyperscale.crypto.CryptoException;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.crypto.Identity;
import org.radix.hyperscale.crypto.KeyPair;
import org.radix.hyperscale.executors.Executable;
import org.radix.hyperscale.executors.Executor;
import org.radix.hyperscale.ledger.ShardGroupID;
import org.radix.hyperscale.ledger.StateAddress;
import org.radix.hyperscale.ledger.SubstateCommit;
import org.radix.hyperscale.ledger.SubstateSearchQuery;
import org.radix.hyperscale.ledger.SubstateSearchResponse;
import org.radix.hyperscale.ledger.Substate.NativeField;
import org.radix.hyperscale.ledger.messages.SubmitAtomsMessage;
import org.radix.hyperscale.ledger.primitives.Atom;
import org.radix.hyperscale.network.AbstractConnection;
import org.radix.hyperscale.network.ConnectionState;
import org.radix.hyperscale.network.StandardConnectionFilter;
import org.radix.hyperscale.utils.Numbers;

public abstract class Spammer extends Executable
{
	private final Spamathon 	spamathon;
	private final int 			rate;
	private final int			iterations;
	private final ShardGroupID  targetShardGroup;
	private final double 		shardFactor;
	private final Range<Integer> saturation;
	private final AtomicInteger processed;

	/**
	 * Serves as a cache of submitted atoms in case the spam implementation wants to check completion status.
	 */
	private final Set<Atom> submittedAtoms;

	/**
	 * A collection of supernumerary keys which should also sign atoms prior to submission 
	 */
	private final Map<Identity, KeyPair<?,?,?>> signers;
	
	protected Spammer(final Spamathon spamathon, final int iterations, final int rate, final Range<Integer> saturation, 
				      final double shardFactor, final ShardGroupID targetShardGroup)
	{
		super();
		
		Objects.requireNonNull(spamathon, "Spamathon instance is null");
		Numbers.lessThan(iterations, 1, "Iterations is less than 1");
		Numbers.lessThan(rate, 1, "Rate is less than 1");
		Objects.requireNonNull(saturation, "Saturation is null");
		Numbers.inRange(shardFactor, 0, 1, "Shard factor is not in the 0-1 range");
		
		this.spamathon = spamathon;
		this.rate = rate;
		this.iterations = iterations;
		this.saturation = saturation;
		this.shardFactor = shardFactor;
		this.targetShardGroup = targetShardGroup;
		
		this.signers = Collections.synchronizedMap(new HashMap<Identity, KeyPair<?,?,?>>(4));
		this.submittedAtoms = Collections.synchronizedSet(new LinkedHashSet<Atom>());
		
		this.processed = new AtomicInteger(0);
	}

	Spamathon getSpamathon() 
	{
		return this.spamathon;
	}

	public int getRate() 
	{
		return this.rate;
	}

	public int getIterations() 
	{
		return this.iterations;
	}

	public ShardGroupID getTargetShardGroup() 
	{
		return this.targetShardGroup;
	}

	public double getShardFactor() 
	{
		return this.shardFactor;
	}
	
	public AtomicInteger getProcessed() 
	{
		return this.processed;
	}
	
	public Range<Integer> getSaturation()
	{
		return this.saturation;
	}
	
	public boolean addSigner(final KeyPair<?,?,?> signer)
	{
		Objects.requireNonNull(signer, "Signer is null");
		return this.signers.putIfAbsent(signer.getIdentity(), signer) == null ? true : false;
	}
	
	public Collection<KeyPair<?,?,?>> getSigners()
	{
		synchronized(this.signers)
		{
			return Collections.unmodifiableCollection(this.signers.values());
		}
	}
	
	private void signExtended(final Atom atom) throws CryptoException
	{
		synchronized(this.signers)
		{
			for (final Identity identity : this.signers.keySet())
			{
				if (atom.hasAuthority(identity) == false)
					atom.sign(this.signers.get(identity));
			}
		}
	}
	
	int getPOWDifficulty()
	{
		if (this.signers.containsKey(Universe.get().getCreator().getIdentity()))
			return 0;
		
		return Universe.get().getPrimitivePOW();
	}

	boolean nextIsIsolatedShard()
	{
		return ThreadLocalRandom.current().nextInt(this.iterations) < (this.iterations * this.shardFactor);
	}
	
	int pluckSaturationInRange()
	{
		if (this.saturation.getMinimum().equals(this.saturation.getMaximum()))
			return this.saturation.getMinimum();
		else
		{
			double r = ThreadLocalRandom.current().nextDouble();
	        double x = -Math.log(1 - r) / Math.E;
	        return (int) Math.floor(this.saturation.getMinimum() + (this.saturation.getMaximum() - this.saturation.getMinimum()) * (1 - Math.exp(-x)));
		}
	}
	
	private enum AtomCompletionStatus
	{
		COMPLETED, TIMEDOUT
	}
	
	void submit(final Context context, final Atom atom) throws Exception	
	{
		signExtended(atom);
		
		if (context.getLedger().submit(atom) == false)
			throw new Exception("Atom "+atom.getHash()+" not submitted to context "+context.getName());
		
		this.submittedAtoms.add(atom);
		clearStaleSubmitted();
	}
	
	void submit(final ShardGroupID shardGroup, final Atom atom) throws Exception	
	{
		signExtended(atom);
		
		final List<Context> submissionContexts = Context.getAll();
		Collections.shuffle(submissionContexts);

		Context submissionContext  = submissionContexts.get(ThreadLocalRandom.current().nextInt(submissionContexts.size()));
		if (submissionContext.getNode().isSynced() == false)
		{
			submissionContext = null;
			for (int c = 0 ; c < submissionContexts.size() ; c++)
			{
				if (submissionContexts.get(c).getNode().isSynced() == false)
					continue;
					
				submissionContext = submissionContexts.get(c);
				break;
			}
			
			if (submissionContext == null)
				throw new IOException("No context in sync for submission of atom "+atom.getHash());
		}

		final SubmitAtomsMessage submitAtomsMessage = new SubmitAtomsMessage(atom);
		final StandardConnectionFilter submissionConnectionFilter = StandardConnectionFilter.build(submissionContext).setStates(ConnectionState.CONNECTED).setShardGroupID(shardGroup).setSynced(true).setStale(false);
		final List<AbstractConnection> connections = submissionContext.getNetwork().get(submissionConnectionFilter);
		if (connections.isEmpty() == false)
			submissionContext.getNetwork().getMessaging().send(submitAtomsMessage, connections.getFirst());
		else
			throw new IOException("No connections found for shard group "+shardGroup+" to submit atom "+atom.getHash());
	}

	public void submitOverInterval(final Context context, final Collection<Atom> atoms, final long interval, final TimeUnit unit) throws Exception
	{
		Objects.requireNonNull(atoms, "Atoms is null");
		Numbers.isZero(atoms.size(), "Atoms is empty");
		Objects.requireNonNull(unit, "Time unit for interval is null");
		Numbers.lessThan(interval, 1, "Interval of "+interval+" is invalid");

		final long intervalMillis = unit.toMillis(interval);
		final long intervalMillisStep;
		final int intervalSubmitCount;
		int scheduledDelay = 0;

		if (intervalMillis < atoms.size())
		{
			intervalMillisStep = 1;
			intervalSubmitCount = (int) ((atoms.size() / intervalMillis) + 1);
		}
		else
		{
			intervalSubmitCount = 1;
			intervalMillisStep = intervalMillis / atoms.size();
		}

		final Iterator<Atom> atomIterator = atoms.iterator();
		while(scheduledDelay < intervalMillis)
		{
			int scheduledAtomSubmissions = 0;
			while(scheduledAtomSubmissions < intervalSubmitCount && atomIterator.hasNext())
			{
				final Atom atom = atomIterator.next();
				signExtended(atom);

				Executor.getInstance().schedule(() -> context.getLedger().submit(atom), scheduledDelay, unit);
				this.submittedAtoms.add(atom);
			}
			
			scheduledDelay += intervalMillisStep;
		}
		
		clearStaleSubmitted();
	}

	void submitAtomAndWait(final Context context, final Atom atom) throws Exception	
	{
		submitAtomAndWaitAtPendingThreshold(context, atom, 0);
	}

	void submitAtomAndWaitAtPendingThreshold(final Context context, final Atom atom, final int maxAtomsWaitThreshold) throws Exception	
	{
		submitAtomAndWaitAtPendingThreshold(context, atom, maxAtomsWaitThreshold, null);
	}

	void submitAtomAndWaitAtPendingThreshold(final Context context, final Atom atom, final int maxAtomsWaitThreshold, final Runnable completionCallback) throws Exception	
	{
		submit(context, atom);
		
		if (this.submittedAtoms.size() >= maxAtomsWaitThreshold)
		{
			waitUntilAtomsAreCompleted();
			
			if (completionCallback != null)
				completionCallback.run();
		}
	}
	
	void waitUntilAtomsAreCompleted() throws InterruptedException, ExecutionException, TimeoutException
	{
		waitUntilAtomsAreCompleted(null);
 	}

	void waitUntilAtomsAreCompleted(final Runnable completionCallback) throws InterruptedException, ExecutionException, TimeoutException
	{
		if (this.submittedAtoms.isEmpty())
			return;

		final long start = System.currentTimeMillis();
		final List<Atom> pendingAtoms = new ArrayList<Atom>(this.submittedAtoms);
		final Map<Hash, AtomCompletionStatus> statuses = new HashMap<>();
		final List<Future<SubstateSearchResponse>> searchFutures = new ArrayList<>();
		
		while(pendingAtoms.isEmpty() == false)
		{
			long iteration = System.currentTimeMillis();
			
			searchFutures.clear();
			
			for (final Atom atom : pendingAtoms)
			{
				final SubstateSearchQuery query = new SubstateSearchQuery(StateAddress.from(Atom.class, atom.getHash()));
				final Future<SubstateSearchResponse> response = Context.get().getLedger().get(query);
				searchFutures.add(response);
			}
			
			for (final Future<SubstateSearchResponse> searchFuture : searchFutures)
			{
				final SubstateCommit result = searchFuture.get().getResult();
				if (result != null && result.getSubstate().get(NativeField.CERTIFICATE) != null)
					statuses.put(searchFuture.get().getQuery().getAddress().scope(), AtomCompletionStatus.COMPLETED);
				else if (System.currentTimeMillis() > start + TimeUnit.SECONDS.toMillis(Constants.ATOM_ACCEPT_TIMEOUT_SECONDS))
					statuses.put(searchFuture.get().getQuery().getAddress().scope(), AtomCompletionStatus.TIMEDOUT);
			}
			
			final Iterator<Atom> atomIterator = pendingAtoms.iterator();
			while(atomIterator.hasNext())
			{
				final Atom atom = atomIterator.next();
				final AtomCompletionStatus completionStatus = statuses.get(atom.getHash());
				if (completionStatus == AtomCompletionStatus.COMPLETED)
					atomIterator.remove();
				else if (completionStatus == AtomCompletionStatus.TIMEDOUT)
					throw new TimeoutException("Completion timeout for atom "+atom.getHash());
			}
			
			if (pendingAtoms.isEmpty() == false)
			{
				long iterationInterval = System.currentTimeMillis() - iteration;
				if (iterationInterval > 0)
					Thread.sleep(iterationInterval);
			}
		}
		
		if (completionCallback != null)
			completionCallback.run();
		
		this.submittedAtoms.removeAll(pendingAtoms);
	}

	void waitUntilAtomIsCompleted(final Atom atom, final long timeout, final TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException
	{
		final long start = System.currentTimeMillis();
		boolean isCompleted = false;
		boolean isTimedout = false;
		
		// Wait until completed
		while (isCompleted == false && isTimedout == false)
		{
			long iteration = System.currentTimeMillis();

			if (isAtomCompleted(atom))
				isCompleted = true;
			
			if (isCompleted == false && System.currentTimeMillis() > start + unit.toMillis(timeout))
				isTimedout = true;

			if (isCompleted == false && isTimedout == false)
			{
				long iterationInterval = System.currentTimeMillis() - iteration;
				if (iterationInterval > 0)
					Thread.sleep(iterationInterval);
			}
		}
		
		if (isTimedout)
			throw new TimeoutException("Completion timeout for atom "+atom.getHash());
	}
	
	private boolean isAtomCompleted(final Atom atom) throws InterruptedException, ExecutionException
	{
		final SubstateSearchQuery query = new SubstateSearchQuery(StateAddress.from(Atom.class, atom.getHash()));
		final Future<SubstateSearchResponse> response = Context.get().getLedger().get(query);
		final SubstateCommit result = response.get().getResult();
		if (result != null && result.getSubstate().get(NativeField.CERTIFICATE) != null)
			return true;
		
		return false;
	}
	
	private void clearStaleSubmitted()
	{
		synchronized(this.submittedAtoms)
		{
			final long timestamp = System.currentTimeMillis();
			final Iterator<Atom> submittedAtomsIterator = this.submittedAtoms.iterator();
			while(submittedAtomsIterator.hasNext())
			{
				final Atom atom = submittedAtomsIterator.next();
				if (atom.witnessedAt()+TimeUnit.SECONDS.toMillis(Constants.PRIMITIVE_STALE) > timestamp)
					break;
				
				submittedAtomsIterator.remove();
			}
		}
	}
}
