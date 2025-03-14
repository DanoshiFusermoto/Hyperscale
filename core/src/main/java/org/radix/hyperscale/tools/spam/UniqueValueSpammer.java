package org.radix.hyperscale.tools.spam;

import java.util.ArrayList;
import java.util.Collections;
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
import org.radix.hyperscale.crypto.ed25519.EDPublicKey;
import org.radix.hyperscale.crypto.ed25519.EDSignature;
import org.radix.hyperscale.ledger.ShardGroupID;
import org.radix.hyperscale.ledger.ShardMapper;
import org.radix.hyperscale.ledger.StateAddress;
import org.radix.hyperscale.ledger.StateContext;
import org.radix.hyperscale.ledger.messages.SubmitAtomsMessage;
import org.radix.hyperscale.ledger.primitives.Atom;
import org.radix.hyperscale.ledger.sme.components.UniqueValueComponent;
import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;
import org.radix.hyperscale.network.AbstractConnection;
import org.radix.hyperscale.network.ConnectionState;
import org.radix.hyperscale.network.StandardConnectionFilter;
import org.radix.hyperscale.utils.UInt256;

@SpamConfig(name = "unique")
final class UniqueValueSpammer extends Spammer
{
	private static final Logger spammerLog = Logging.getLogger("spammer");
	
	protected UniqueValueSpammer(Spamathon spamathon, int iterations, int rate, Range<Integer> saturation, ShardGroupID targetShardGroup, double shardFactor)
	{
		super(spamathon, iterations, rate, saturation, targetShardGroup, shardFactor);
	}
	
	@Override
	public void execute()
	{
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
			
			final List<Context> contexts = Collections.unmodifiableList(new ArrayList<Context>(Context.getAll()));
			while(getProcessed().get() < getIterations() && isCancelled() == false)
			{
				Atom atom = null;
				final Set<ShardGroupID> atomShardGroups = new HashSet<>(4);

				try
				{
					final EDKeyPair signer;
					if (EDKeyPair.SKIP_SIGNING == false || EDKeyPair.SKIP_VERIFICATION == false)
						signer = new EDKeyPair();
					else 
						signer = null;

					final EDPublicKey pubkey;
					if (signer != null)
						pubkey = signer.getPublicKey();
					else
					{
						byte[] pubbytes = new byte[33];
						ThreadLocalRandom.current().nextBytes(pubbytes);
						pubbytes[0] = 2;
						pubkey = EDPublicKey.from(pubbytes);
					}
					
					final String instruction;
					final boolean isIsolatedShard = nextIsIsolatedShard();
					final ShardGroupID targetShardGroupID;
					if (getTargetShardGroup() == null)
					{
						long value = ThreadLocalRandom.current().nextLong();
						instruction = uniqueValueComponentContext+"::set(uint256("+value+"), identity('"+pubkey.getIdentity()+"'))";
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
							
						instruction = uniqueValueComponentContext+"::set(uint256("+value+"), identity('"+pubkey.getIdentity()+"'))";
						targetShardGroupID = shardGroupID;
					}
					
					do
					{
						Atom.Builder atomBuilder = new Atom.Builder();
						atomShardGroups.clear();

						atomBuilder.push(instruction);
						atomShardGroups.add(targetShardGroupID);
						
						final int saturation = pluckSaturationInRange();
						if (isIsolatedShard == false && saturation > 1)
						{
							for (int u = 1 ; u < saturation ; u++)
							{
								final long extraValue = ThreadLocalRandom.current().nextLong();
								final String extraInstruction = uniqueValueComponentContext+"::set(uint256("+extraValue+"), identity('"+pubkey.getIdentity()+"'))";
								atomBuilder.push(extraInstruction);
								atomShardGroups.add(ShardMapper.toShardGroup(StateAddress.from(uniqueValueComponentContext, Hash.valueOf(UInt256.from(extraValue))), numShardGroups));
							}
						}
						
						atom = atomBuilder.build();
					}
					while(isIsolatedShard && ShardMapper.toShardGroup(StateAddress.from(atomContext, atom.getHash()), numShardGroups).equals(targetShardGroupID) == false);
					
					if (signer != null)
						atom.sign(signer);
					else
						atom.sign(pubkey, EDSignature.NULL);
				}
				catch (Throwable t)
				{
					spammerLog.error(t);
				}
				finally
				{
					getProcessed().incrementAndGet();
				}
				
				try
				{
					boolean submitted = false;
					for (Context context : contexts)
					{
						final ShardGroupID contextShardGroupID = ShardMapper.toShardGroup(context.getNode().getIdentity(), numShardGroups);
						if (atomShardGroups.contains(contextShardGroupID))
						{
							context.getLedger().submit(atom);
							submitted = true;
							break;
						}
					}

					if (submitted == false)
					{
						for (ShardGroupID shardGroupID : atomShardGroups)
						{
							final Context submissionContext = contexts.get(ThreadLocalRandom.current().nextInt(contexts.size()));
							final SubmitAtomsMessage submitAtomsMessage = new SubmitAtomsMessage(atom);
							final StandardConnectionFilter submissionConnectionFilter = StandardConnectionFilter.build(submissionContext).setStates(ConnectionState.CONNECTED).setShardGroupID(shardGroupID).setSynced(true).setStale(false);
							final List<AbstractConnection> connections = submissionContext.getNetwork().get(submissionConnectionFilter);
							if (connections.isEmpty() == false)
							{
								submissionContext.getNetwork().getMessaging().send(submitAtomsMessage, connections.getFirst());
								break;
							}
						}
					}
				}
				catch (Throwable t)
				{
					spammerLog.error(t);
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
