package org.radix.hyperscale.tools.spam;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.apache.commons.lang3.Range;
import org.radix.hyperscale.Context;
import org.radix.hyperscale.Universe;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.crypto.ed25519.EDKeyPair;
import org.radix.hyperscale.ledger.ShardGroupID;
import org.radix.hyperscale.ledger.ShardMapper;
import org.radix.hyperscale.ledger.StateAddress;
import org.radix.hyperscale.ledger.StateContext;
import org.radix.hyperscale.ledger.primitives.Atom;
import org.radix.hyperscale.ledger.sme.components.UniqueValueComponent;
import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;
import org.radix.hyperscale.utils.UInt256;

@SpamConfig(name = "unique")
final class UniqueValueSpammer extends Spammer
{
	private static final Logger spammerLog = Logging.getLogger("spammer");
	
	protected UniqueValueSpammer(final Spamathon spamathon, final int iterations, final int rate, final Range<Integer> saturation, 
			 					 final double shardFactor, final ShardGroupID targetShardGroup)
	{
		super(spamathon, iterations, rate, saturation, shardFactor, targetShardGroup);
	}
	
	@Override
	public void execute()
	{
		final List<Context> contexts = Context.getAll();
		final long spamStart = System.currentTimeMillis();
		try
		{
			int nextBurstInterval = 32;
			long burstStartNano = System.nanoTime();
			long lastProgressReport = System.currentTimeMillis();
			final long waitIntervalNano = TimeUnit.SECONDS.toNanos(1) / getRate();
			final int numShardGroups = Universe.getDefault().shardGroupCount();
			final String atomContext = Atom.class.getAnnotation(StateContext.class).value();
			final String uniqueValueComponentContext = UniqueValueComponent.class.getAnnotation(StateContext.class).value();
			
			final Set<ShardGroupID> atomShardGroups = new HashSet<>(4);
			while(getProcessed().get() < getIterations() && isCancelled() == false)
			{
				final Atom.Builder atomBuilder = new Atom.Builder();
				atomShardGroups.clear();

				try
				{
					final EDKeyPair signer = new EDKeyPair();
					final String instruction;
					final boolean isIsolatedShard = nextIsIsolatedShard();
					final ShardGroupID targetShardGroupID;
					if (getTargetShardGroup() == null)
					{
						long value = ThreadLocalRandom.current().nextLong();
						instruction = uniqueValueComponentContext+"::set(uint256("+value+"), identity('"+signer.getIdentity()+"'))";
						targetShardGroupID = ShardMapper.toShardGroup(StateAddress.from(uniqueValueComponentContext, Hash.valueOf(UInt256.from(value))), numShardGroups);
					}
					else
					{
						long value;
						ShardGroupID shardGroupID;
						do
						{
							value = ThreadLocalRandom.current().nextLong();
							shardGroupID = ShardMapper.toShardGroup(StateAddress.from(uniqueValueComponentContext, Hash.valueOf(UInt256.from(value))), numShardGroups);
						}
						while(shardGroupID.equals(getTargetShardGroup()) == false);
							
						instruction = uniqueValueComponentContext+"::set(uint256("+value+"), identity('"+signer.getIdentity()+"'))";
						targetShardGroupID = shardGroupID;
					}
					
					atomBuilder.push(instruction);
					atomShardGroups.add(targetShardGroupID);
						
					final int saturation = pluckSaturationInRange();
					if (isIsolatedShard == false && saturation > 1)
					{
						for (int u = 1 ; u < saturation ; u++)
						{
							final long extraValue = ThreadLocalRandom.current().nextLong();
							final String extraInstruction = uniqueValueComponentContext+"::set(uint256("+extraValue+"), identity('"+signer.getIdentity()+"'))";
							atomBuilder.push(extraInstruction);
							
							final StateAddress uniqueStateAddress = StateAddress.from(uniqueValueComponentContext, Hash.valueOf(UInt256.from(extraValue)));
							atomShardGroups.add(ShardMapper.toShardGroup(uniqueStateAddress, numShardGroups));
						}
					}
					
					if (isIsolatedShard)
					{
						int isolationAttempts = 1000;
						while(isolationAttempts > 0)
						{
							final StateAddress atomStateAddress = StateAddress.from(atomContext, atomBuilder.discoverHash(getPOWDifficulty(), true));
							final ShardGroupID atomShardGroup = ShardMapper.toShardGroup(atomStateAddress, numShardGroups);
							if (atomShardGroups.contains(atomShardGroup))
								break;
							
							isolationAttempts--;
						}
					}
					
					atomBuilder.signer(signer);
					atomBuilder.build(getPOWDifficulty());

					// Submit local
					for (Context context : contexts)
					{
						if (context.getNode().isSynced() == false)
							continue;
						
						final ShardGroupID contextShardGroupID = ShardMapper.toShardGroup(context.getNode().getIdentity(), numShardGroups);
						if (atomShardGroups.remove(contextShardGroupID))
							submit(context, atomBuilder.get());
					}
					
					// Submit remote
					if (atomShardGroups.isEmpty() == false)
					{
						for (ShardGroupID shardGroup : atomShardGroups)
							submit(shardGroup, atomBuilder.get());
					}
				}
				catch (Throwable t)
				{
					spammerLog.error(t);
				}
				finally
				{
					getProcessed().incrementAndGet();
				}

				if (getProcessed().get() == nextBurstInterval)
				{
					final long burstDurationNano = System.nanoTime()-burstStartNano;
					final long adjustedWaitDurationNano = (waitIntervalNano*32) - burstDurationNano;
					if (adjustedWaitDurationNano > 0)
						LockSupport.parkNanos(adjustedWaitDurationNano);

					nextBurstInterval += 32;
					burstStartNano = System.nanoTime();
					
					if (System.currentTimeMillis() - lastProgressReport > 1000 && getProcessed().get() >= getRate())
					{
						spammerLog.info("Submitted "+getProcessed()+"/"+getIterations()+" of spam atoms to ledger "+(getProcessed().get() / Math.max(1, TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - spamStart))+" / "+getRate()));
						lastProgressReport = System.currentTimeMillis();
					}
				}
			}
		}
		catch (Exception ex)
		{
			spammerLog.out(ex);
			throw new RuntimeException(ex);
		}
		finally
		{
			spammerLog.info("Spammer took "+(System.currentTimeMillis() - spamStart)+"ms to perform "+getProcessed()+" of "+getIterations()+" events");
			getSpamathon().completed(this);
		}
	}
}
