package org.radix.hyperscale.tools.spam;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.Range;
import org.radix.hyperscale.Constants;
import org.radix.hyperscale.Context;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.executors.Executable;
import org.radix.hyperscale.executors.Executor;
import org.radix.hyperscale.ledger.ShardGroupID;
import org.radix.hyperscale.ledger.StateAddress;
import org.radix.hyperscale.ledger.SubstateCommit;
import org.radix.hyperscale.ledger.SubstateSearchQuery;
import org.radix.hyperscale.ledger.SubstateSearchResponse;
import org.radix.hyperscale.ledger.Substate.NativeField;
import org.radix.hyperscale.ledger.primitives.Atom;
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
	
	protected Spammer(final Spamathon spamathon, final int iterations, final int rate, final Range<Integer> saturation, final double shardFactor, final ShardGroupID targetShardGroup)
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
		this.processed = new AtomicInteger(0);
	}

	protected Spamathon getSpamathon() 
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

	public boolean nextIsIsolatedShard()
	{
		return ThreadLocalRandom.current().nextInt(this.iterations) < (this.iterations * this.shardFactor);
	}
	
	public AtomicInteger getProcessed() 
	{
		return this.processed;
	}
	
	private enum AtomCompletionStatus
	{
		COMPLETED, TIMEDOUT
	}
	
	private final List<Atom> atoms = new ArrayList<Atom>();
	void submitAtom(final Context submitTo, final Atom atom) throws Exception	
	{
		if (submitTo.getLedger().submit(atom) == false)
			throw new Exception("Atom "+atom.getHash()+" not submitted");
		
		this.atoms.add(atom);
	}
	
	public void submitOverInterval(final Context submitTo, final Collection<Atom> atoms, final long interval, final TimeUnit unit)
	{
		Objects.requireNonNull(atoms, "Atoms is null");
		Numbers.isZero(atoms.size(), "Atoms is empty");
		Objects.requireNonNull(unit, "Time unit for interval is null");
		Numbers.lessThan(interval, 1, "Interval of "+interval+" is invalid");

		long intervalMillis = unit.toMillis(interval);
		long intervalMillisStep;
		int intervalSubmitCount;
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
				Executor.getInstance().schedule(() -> submitTo.getLedger().submit(atom), scheduledDelay, unit);
				this.atoms.add(atom);
			}
			
			scheduledDelay += intervalMillisStep;
		}
	}

	void submitAtomAndWait(final Context submitTo, final Atom atom) throws Exception	
	{
		submitAtomAndWaitAtPendingThreshold(submitTo, atom, 0);
	}

	void submitAtomAndWaitAtPendingThreshold(final Context submitTo, final Atom atom, final int maxAtomsWaitThreshold) throws Exception	
	{
		submitAtomAndWaitAtPendingThreshold(submitTo, atom, maxAtomsWaitThreshold, null);
	}

	void submitAtomAndWaitAtPendingThreshold(final Context submitTo, final Atom atom, final int maxAtomsWaitThreshold, final Runnable completionCallback) throws Exception	
	{
		if (submitTo.getLedger().submit(atom) == false)
			throw new Exception("Atom "+atom.getHash()+" not submitted");

		this.atoms.add(atom);
		
		if (this.atoms.size() >= maxAtomsWaitThreshold)
		{
			waitUntilAtomsAreCompleted(this.atoms);
			this.atoms.clear();
			
			if (completionCallback != null)
				completionCallback.run();
		}
	}
	
	int numAtomsPendingCompletion()
	{
		return this.atoms.size();
	}
	
	void waitUntilAtomsAreCompleted() throws InterruptedException, ExecutionException, TimeoutException
	{
		if (this.atoms.isEmpty())
			return;
		
		waitUntilAtomsAreCompleted(this.atoms);
		this.atoms.clear();
 	}

	void waitUntilAtomsAreCompleted(final Runnable completionCallback) throws InterruptedException, ExecutionException, TimeoutException
	{
		if (this.atoms.isEmpty())
			return;
		
		waitUntilAtomsAreCompleted(this.atoms, completionCallback);
		this.atoms.clear();
 	}

	void waitUntilAtomsAreCompleted(final Collection<Atom> atoms) throws InterruptedException, ExecutionException, TimeoutException
	{
		waitUntilAtomsAreCompleted(atoms, null);
	}
	
	void waitUntilAtomsAreCompleted(final Collection<Atom> atoms, final Runnable completionCallback) throws InterruptedException, ExecutionException, TimeoutException
	{
		final long start = System.currentTimeMillis();
		final Map<Hash, AtomCompletionStatus> statuses = new HashMap<>();
		final List<Future<SubstateSearchResponse>> searchFutures = new ArrayList<>();
		
		while(this.atoms.isEmpty() == false)
		{
			long iteration = System.currentTimeMillis();
			
			searchFutures.clear();
			
			for (final Atom atom : this.atoms)
			{
				Future<SubstateSearchResponse> response = Context.get().getLedger().get(new SubstateSearchQuery(StateAddress.from(Atom.class, atom.getHash())));
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
			
			final Iterator<Atom> atomIterator = this.atoms.iterator();
			while(atomIterator.hasNext())
			{
				final Atom atom = atomIterator.next();
				final AtomCompletionStatus completionStatus = statuses.get(atom.getHash());
				if (completionStatus == AtomCompletionStatus.COMPLETED)
					atomIterator.remove();
				else if (completionStatus == AtomCompletionStatus.TIMEDOUT)
					throw new TimeoutException("Completion timeout for atom "+atom.getHash());
			}
			
			if (this.atoms.isEmpty() == false)
			{
				long iterationInterval = System.currentTimeMillis() - iteration;
				if (iterationInterval > 0)
					Thread.sleep(iterationInterval);
			}
		}
		
		if (completionCallback != null)
			completionCallback.run();
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
		Future<SubstateSearchResponse> response = Context.get().getLedger().get(new SubstateSearchQuery(StateAddress.from(Atom.class, atom.getHash())));
		SubstateCommit result = response.get().getResult();
		if (result != null && result.getSubstate().get(NativeField.CERTIFICATE) != null)
			return true;
		
		return false;
	}
	
	Range<Integer> getSaturation()
	{
		return this.saturation;
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
}
