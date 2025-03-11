package org.radix.hyperscale.ledger;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import org.eclipse.collections.api.factory.Maps;
import org.radix.hyperscale.Context;
import org.radix.hyperscale.crypto.CryptoException;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.crypto.Hashable;
import org.radix.hyperscale.exceptions.ValidationException;
import org.radix.hyperscale.ledger.BlockHeader.InventoryType;
import org.radix.hyperscale.ledger.exceptions.LockException;
import org.radix.hyperscale.ledger.sme.PolyglotPackage;
import org.radix.hyperscale.ledger.timeouts.CommitTimeout;
import org.radix.hyperscale.ledger.timeouts.ExecutionTimeout;
import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;
import org.radix.hyperscale.network.messages.Message;
import org.radix.hyperscale.serialization.Serialization;
import org.radix.hyperscale.serialization.DsonOutput.Output;
import org.radix.hyperscale.time.Time;

import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;

class BlockBuilder
{
	private static final Logger blocksLog = Logging.getLogger("blocks");

	private final Context context;
	private final Multimap<InventoryType, Hashable> inventory;
	private final Map<StateAddress, PendingAtom> stateLocks;
	
	BlockBuilder(final Context context)
	{
		Objects.requireNonNull(context, "Context is null");
		this.context = context;
		this.inventory = LinkedHashMultimap.create();
		this.stateLocks = Maps.mutable.ofInitialCapacity(BlockHeader.MAX_INVENTORY_TYPE_PRIMITIVES);
	}

	PendingBlock build(final BlockHeader buildable, final PendingBranch branch, final BlockHeader head) throws IOException, CryptoException, ValidationException
	{
		Objects.requireNonNull(branch, "Branch to build on is null");
		
		if (branch.isConstructed() == false || branch.isPrepared() == false || branch.isCanonical() == false)
			throw new IllegalStateException("Branch is not buildable "+branch);
		
		if (branch.isBuildable(buildable) == false)
		{
			blocksLog.warn(this.context.getName()+": Branch is not buildable at header "+buildable+" in "+branch.getRoot());
			return null;
		}
			
		if ((branch.isEmpty() == false && buildable.getHash().equals(branch.getHigh().getHash()) == false) ||
			(branch.isEmpty() == true && buildable.equals(branch.getRoot()) == false))
			throw new IllegalStateException("Branch buildable head "+buildable+" is not branch tip");
			
		this.context.getMetaData().increment("ledger.mining.attempted");

		if (branch.getRoot().equals(head) == false && branch.contains(head.getHash()) == false)
		{
			blocksLog.warn(this.context.getName()+": Branch is stale, ledger head has changed from expected "+branch.getRoot()+" to "+head);
			return null;
		}
		
		if (blocksLog.hasLevel(Logging.INFO))
			blocksLog.info(this.context.getName()+": Generating proposal on "+buildable);
		
		this.inventory.clear();
		this.stateLocks.clear();

		final long timestamp = Time.getSystemTime();
		final List<PendingAtom> acceptAtoms = this.context.getLedger().getAtomHandler().acceptable(BlockHeader.MAX_INVENTORY_TYPE_PRIMITIVES, o -> { 
			boolean exclude = false;
			if (exclude == false) exclude = this.inventory.containsValue(o);
			if (exclude == false) exclude = branch.references(o.getHash(), BlockHeader.ACCEPT_SELECT_FILTER) != null;
			if (o instanceof PendingAtom a && exclude == false) 
			{
				final Set<PendingState> pendingStates = a.getStates();
				for (final PendingState pendingState : pendingStates)
				{
					exclude = BlockBuilder.this.stateLocks.containsKey(pendingState.getAddress());
					if(exclude == true)
						break;
				   
					try { branch.lockable(a, pendingState.getAddress()); } catch (Exception ex) {exclude = true;}
					if(exclude == true)
						break;
				}
			   
				if (exclude == false)
				{
	  			   	for (final PendingState pendingState : pendingStates)
	  			   	{
	  			   		if (pendingState.getStateLockMode() == StateLockMode.WRITE)
	  			   			BlockBuilder.this.stateLocks.put(pendingState.getAddress(), a);
	  			   	}
				}
			}
			return exclude;
		});

		// TODO Optimize this!
		int candidateAcceptedSize = 0;
		for (PendingAtom acceptAtom : acceptAtoms)
		{
			try
			{
				if (acceptAtom.getStatus().after(AtomStatus.State.PREPARED)) 
					throw new ValidationException(acceptAtom, "Accepted atom "+acceptAtom.getAtom()+" has status "+acceptAtom.getStatus());
						
				InventoryType referenceType = branch.references(acceptAtom.getHash(), BlockHeader.ACCEPT_VERIFY_FILTER);
				if (referenceType != null)
					throw new ValidationException(acceptAtom, "Accepted atom "+acceptAtom.getAtom()+" is "+referenceType+ " in branch");

				this.inventory.put(InventoryType.ACCEPTED, acceptAtom);
				
				if (Block.MAX_BLOCK_SIZE != Integer.MAX_VALUE)
				{
					byte[] bytes = Serialization.getInstance().toDson(acceptAtom.getAtom(), Output.WIRE);
					int candidateAtomSize = bytes.length;
					candidateAcceptedSize += candidateAtomSize; 
					if (candidateAcceptedSize > (Message.MAX_MESSAGE_SIZE / 2))
						break;
				}
			}
			catch (ValidationException vex)
			{
				if (blocksLog.hasLevel(Logging.DEBUG))
					blocksLog.debug(this.context.getName()+": "+vex.getMessage());
			}				
		}
		
		int maxBlockHeaderSize = Block.RESERVED_HEADER_SIZE;
		int blockPayloadSize = candidateAcceptedSize;
		int blockSizeRemaining = (Block.MAX_BLOCK_SIZE-maxBlockHeaderSize) - candidateAcceptedSize;
		Block miningBlock = null;
				
		do
		{
			// TODO is it better to handle timeouts before certificates due to block size limitation?
			for (PendingAtom unacceptedAtom : this.context.getLedger().getAtomHandler().unaccepted(BlockHeader.MAX_INVENTORY_TYPE_PRIMITIVES, 
					  																			       o -> branch.references(o.getHash(), BlockHeader.UNACCEPT_SELECT_FILTER) != null ||
					  																			    		BlockBuilder.this.inventory.containsValue(o)))
			{
				try
				{
					if (unacceptedAtom.getStatus().after(AtomStatus.State.PREPARED)) 
						throw new ValidationException(unacceptedAtom, "Unaccepted atom "+unacceptedAtom.getAtom()+" has status "+unacceptedAtom.getStatus());
							
					InventoryType referenceType = branch.references(unacceptedAtom.getHash(), BlockHeader.UNACCEPT_VERIFY_FILTER);
					if (referenceType != null)
						throw new ValidationException(unacceptedAtom, "Unaccepted atom "+unacceptedAtom.getAtom()+" is "+referenceType+ " in branch");

					if (Block.MAX_BLOCK_SIZE != Integer.MAX_VALUE)
					{
						byte[] bytes = Serialization.getInstance().toDson(unacceptedAtom.getAtom(), Output.WIRE);
						if (blockSizeRemaining - bytes.length < 0)
							break;
						
						blockSizeRemaining -= bytes.length;
						blockPayloadSize += bytes.length;
					}

					this.inventory.put(InventoryType.UNACCEPTED, unacceptedAtom);
				}
				catch (ValidationException vex)
				{
					if (blocksLog.hasLevel(Logging.DEBUG))
						blocksLog.debug(this.context.getName()+": "+vex.getMessage());
				}				
			}
			
			for (PendingAtom executableAtom : this.context.getLedger().getAtomHandler().executable(BlockHeader.MAX_INVENTORY_TYPE_PRIMITIVES, 
																									  o -> branch.references(o.getHash(), BlockHeader.EXECUTE_SELECT_FILTER) != null ||
																										   BlockBuilder.this.inventory.containsValue(o)))
			{
				try
				{
					if (executableAtom.getStatus().after(AtomStatus.State.PREPARED) == false) 
						throw new ValidationException(executableAtom, "Executable atom "+executableAtom.getAtom()+" is not ACCEPTED");

					if (executableAtom.getStatus().after(AtomStatus.State.PROVISIONED)) 
						throw new ValidationException(executableAtom, "Executable atom "+executableAtom.getAtom()+" has status "+executableAtom.getStatus());

					if (executableAtom.isExecuteSignalled()) 
						throw new ValidationException(executableAtom, "Executable atom "+executableAtom.getAtom()+" is already execute signalled");

					InventoryType referenceType = branch.references(executableAtom.getHash(), BlockHeader.EXECUTE_VERIFY_FILTER);
					if (referenceType != null)
						throw new ValidationException(executableAtom, "Executable atom "+executableAtom.getAtom()+" is "+referenceType+ " in branch");

					if (Block.MAX_BLOCK_SIZE != Integer.MAX_VALUE)
					{
						blockSizeRemaining -= Hash.BYTES;
						blockPayloadSize += Hash.BYTES;
						
						if (blockSizeRemaining < 0)
							break;
					}
					
					this.inventory.put(InventoryType.EXECUTABLE, executableAtom);
				}
				catch (ValidationException vex)
				{
					if (blocksLog.hasLevel(Logging.DEBUG))
						blocksLog.debug(this.context.getName()+": "+vex.getMessage());
				}				
			}

			for (PendingAtom latentAtom : this.context.getLedger().getAtomHandler().latent(BlockHeader.MAX_INVENTORY_TYPE_PRIMITIVES, 
																							   o -> branch.references(o.getHash(), BlockHeader.LATENT_SELECT_FILTER) != null || 
																									BlockBuilder.this.inventory.containsValue(o)))
			{
				try
				{
					if (latentAtom.getStatus().after(AtomStatus.State.PREPARED) == false) 
						throw new ValidationException(latentAtom, "Latent atom "+latentAtom.getAtom()+" is not ACCEPTED");
					
					if (latentAtom.getStatus().after(AtomStatus.State.PROVISIONED)) 
						throw new ValidationException(latentAtom, "Latent atom "+latentAtom.getAtom()+" has status "+latentAtom.getStatus());
					
					if (latentAtom.isExecuteLatentSignalled()) 
						throw new ValidationException(latentAtom, "Latent atom "+latentAtom.getAtom()+" is already latent execute signalled");
					
					InventoryType referenceType = branch.references(latentAtom.getHash(), BlockHeader.LATENT_VERIFY_FILTER);
					if (referenceType != null)
						throw new ValidationException(latentAtom, "Latent atom "+latentAtom.getAtom()+" is "+referenceType+ " in branch");

					if (Block.MAX_BLOCK_SIZE != Integer.MAX_VALUE)
					{
						blockSizeRemaining -= Hash.BYTES;
						blockPayloadSize += Hash.BYTES;
						if (blockSizeRemaining < 0)
							break;
					}
					
					this.inventory.put(InventoryType.LATENT, latentAtom);
				}
				catch (ValidationException vex)
				{
					if (blocksLog.hasLevel(Logging.DEBUG))
						blocksLog.debug(this.context.getName()+": "+vex.getMessage());
				}				
			}

			for (PendingAtom unexecutedAtom : this.context.getLedger().getAtomHandler().unexecuted(BlockHeader.MAX_INVENTORY_TYPE_PRIMITIVES, 
				     																				   o -> branch.references(o.getHash(), BlockHeader.UNEXECUTED_SELECT_FILTER) != null ||
				     																						BlockBuilder.this.inventory.containsValue(o)))
			{
				try
				{
					if (unexecutedAtom.getStatus().after(AtomStatus.State.PREPARED) == false) 
						throw new ValidationException(unexecutedAtom, "Unexecuted atom "+unexecutedAtom.getAtom()+" is not ACCEPTED");
					
					if (unexecutedAtom.getStatus().after(AtomStatus.State.EXECUTING)) 
						throw new ValidationException(unexecutedAtom, "Unexecuted atom "+unexecutedAtom.getAtom()+" has status "+unexecutedAtom.getStatus());
					
					InventoryType referenceType = branch.references(unexecutedAtom.getHash(), BlockHeader.UNEXECUTED_VERIFY_FILTER);
					if (referenceType != null)
						throw new ValidationException(unexecutedAtom, "Unexecuted atom "+unexecutedAtom.getAtom()+" is "+referenceType+ " in branch");

					branch.unlockable(unexecutedAtom);
					
					if (Block.MAX_BLOCK_SIZE != Integer.MAX_VALUE)
					{
						byte[] bytes = Serialization.getInstance().toDson(unexecutedAtom.getTimeout(), Output.WIRE);
						if (blockSizeRemaining - bytes.length < 0)
							break;
						
						blockSizeRemaining -= bytes.length;
						blockPayloadSize += bytes.length;
					}
					
					this.inventory.put(InventoryType.UNEXECUTED, unexecutedAtom);
				}
				catch (LockException lex)
				{
					// DONT CARE //
				}	
				catch (ValidationException vex)
				{
					if (blocksLog.hasLevel(Logging.DEBUG))
						blocksLog.debug(this.context.getName()+": "+vex.getMessage());
				}				
			}

			for (PendingAtom commitAtom : this.context.getLedger().getAtomHandler().completed(BlockHeader.MAX_INVENTORY_TYPE_PRIMITIVES, 
																							    o -> branch.references(o.getHash(), BlockHeader.COMMIT_SELECT_FILTER) != null ||
																							     	 BlockBuilder.this.inventory.containsValue(o)))
			{
				try
				{
					if (commitAtom.getStatus().after(AtomStatus.State.PREPARED) == false) 
						throw new ValidationException(commitAtom, "Commit atom "+commitAtom.getAtom()+" is not ACCEPTED");

					if (commitAtom.getStatus().after(AtomStatus.State.FINALIZED)) 
						throw new ValidationException(commitAtom, "Commit atom "+commitAtom.getAtom()+" has status "+commitAtom.getStatus());
							
					InventoryType referenceType = branch.references(commitAtom.getHash(), BlockHeader.COMMIT_VERIFY_FILTER);
					if (referenceType != null)
						throw new ValidationException(commitAtom, "Commit atom "+commitAtom.getAtom()+" is "+referenceType+ " in branch");

					branch.unlockable(commitAtom);

					if (Block.MAX_BLOCK_SIZE != Integer.MAX_VALUE)
					{
						byte[] bytes = Serialization.getInstance().toDson(commitAtom.getCertificate(), Output.WIRE);
						if (blockSizeRemaining - bytes.length < 0)
							break;
						
						blockSizeRemaining -= bytes.length;
						blockPayloadSize += bytes.length;
					}
					
					this.inventory.put(InventoryType.COMMITTED, commitAtom);
				}
				catch (LockException lex)
				{
					// DONT CARE //
				}							
				catch (ValidationException vex)
				{
					if (blocksLog.hasLevel(Logging.DEBUG))
						blocksLog.debug(this.context.getName()+": "+vex.getMessage());
				}				
			}

			for (PendingAtom uncommittedAtom : this.context.getLedger().getAtomHandler().uncommitted(BlockHeader.MAX_INVENTORY_TYPE_PRIMITIVES,
				  	 																				   o -> branch.references(o.getHash(), BlockHeader.UNCOMMITTED_SELECT_FILTER) != null ||
				  	 																						BlockBuilder.this.inventory.containsValue(o)))
			{
				try
				{
					if (uncommittedAtom.getStatus().after(AtomStatus.State.PREPARED) == false) 
						throw new ValidationException(uncommittedAtom, "Uncommitted atom "+uncommittedAtom.getAtom()+" is not ACCEPTED");
					
					if (uncommittedAtom.getStatus().after(AtomStatus.State.FINALIZED)) 
						throw new ValidationException(uncommittedAtom, "Uncommitted atom "+uncommittedAtom.getAtom()+" has status "+uncommittedAtom.getStatus());
					
					InventoryType referenceType = branch.references(uncommittedAtom.getHash(), BlockHeader.UNCOMMITTED_VERIFY_FILTER);
					if (referenceType != null)
						throw new ValidationException(uncommittedAtom, "Uncommitted atom "+uncommittedAtom.getAtom()+" is "+referenceType+ " in branch");
					
					branch.unlockable(uncommittedAtom);
					
					if (Block.MAX_BLOCK_SIZE != Integer.MAX_VALUE)
					{
						byte[] bytes = Serialization.getInstance().toDson(uncommittedAtom.getTimeout(), Output.WIRE);
						if (blockSizeRemaining - bytes.length < 0)
							break;
						
						blockSizeRemaining -= bytes.length;
						blockPayloadSize += bytes.length;
					}
					
					this.inventory.put(InventoryType.UNCOMMITTED, uncommittedAtom);
				}
				catch (LockException lex)
				{
					// DONT CARE //
				}	
				catch (ValidationException vex)
				{
					if (blocksLog.hasLevel(Logging.DEBUG))
						blocksLog.debug(this.context.getName()+": "+vex.getMessage());
				}				
			}
			
			for (PolyglotPackage pakage: this.context.getLedger().getPackageHandler().uncommitted(BlockHeader.MAX_PACKAGES, 
																								     o -> branch.references(o.getHash(), InventoryType.PACKAGES) != null || 
																								    	  BlockBuilder.this.inventory.containsValue(o)))
			{
				try
				{
					if (branch.references(pakage.getHash(), InventoryType.PACKAGES) == InventoryType.PACKAGES)
						throw new ValidationException("Package "+pakage.getHash()+" already in branch");

					if (Block.MAX_BLOCK_SIZE != Integer.MAX_VALUE)
					{
						byte[] bytes = Serialization.getInstance().toDson(pakage, Output.WIRE);
						if (blockSizeRemaining - bytes.length < 0)
							break;
						
						blockSizeRemaining -= bytes.length;
						blockPayloadSize += bytes.length;
					}

					this.inventory.put(InventoryType.PACKAGES, pakage);
				}
				catch (ValidationException vex)
				{
					if (blocksLog.hasLevel(Logging.DEBUG))
						blocksLog.debug(this.context.getName()+": "+vex.getMessage());
				}				
			}

			// if the branch is long, add some extra difficulty to create a stronger block
			miningBlock = new Block(buildable.getHeight()+1, buildable.getHash(), Long.MAX_VALUE, buildable.getTotalWork(), ThreadLocalRandom.current().nextLong(), buildable.getNextIndex(), timestamp, this.context.getNode().getIdentity(), 
									this.inventory.get(InventoryType.ACCEPTED).stream().map(PendingAtom.class::cast).map(pa -> pa.getAtom()).collect(Collectors.toList()), 
									this.inventory.get(InventoryType.UNACCEPTED).stream().map(PendingAtom.class::cast).map(pa -> pa.getAtom()).collect(Collectors.toList()),
									this.inventory.get(InventoryType.UNEXECUTED).stream().map(PendingAtom.class::cast).filter(pa -> pa.getTimeout() instanceof ExecutionTimeout).map(pa -> pa.<ExecutionTimeout>getTimeout()).collect(Collectors.toList()),
									this.inventory.get(InventoryType.COMMITTED).stream().map(PendingAtom.class::cast).map(pa -> pa.getCertificate()).collect(Collectors.toList()),
									this.inventory.get(InventoryType.UNCOMMITTED).stream().map(PendingAtom.class::cast).filter(pa -> pa.getTimeout() instanceof CommitTimeout).map(pa -> pa.<CommitTimeout>getTimeout()).collect(Collectors.toList()),
									this.inventory.get(InventoryType.EXECUTABLE).stream().map(PendingAtom.class::cast).map(pa -> pa.getHash()).collect(Collectors.toList()), 
									this.inventory.get(InventoryType.LATENT).stream().map(PendingAtom.class::cast).map(pa -> pa.getHash()).collect(Collectors.toList()), 
									this.inventory.get(InventoryType.PACKAGES).stream().map(PolyglotPackage.class::cast).collect(Collectors.toList()));
			
			// Adjust the expected header size to accomadate for over sized blocks.
			// This isn't perfect due to the manner in which certificates and timeout atoms are selected on each pass
			// even with an adjustment it may still oversize.  
			// We take 2x the adjustment calculated to prevent this
			// TODO having to 2x the adjustment does seem a little strange though, investigate this isn't a bug
			if (Block.MAX_BLOCK_SIZE != Integer.MAX_VALUE)
			{
				int miningBlockHeaderSize = miningBlock.getHeader().getSize();
				int miningBlockSize = blockPayloadSize + miningBlockHeaderSize;
				if (miningBlockHeaderSize > maxBlockHeaderSize || miningBlockSize > Block.MAX_BLOCK_SIZE)
				{
					int miningSizeAdjustment = 0;
					if (miningBlockSize > Message.MAX_MESSAGE_SIZE)
					{
						miningSizeAdjustment = (miningBlockSize - Message.MAX_MESSAGE_SIZE)*4;
						blocksLog.warn(this.context.getName()+": Block "+miningBlock.getHeader().getHash()+":"+miningBlock.getHeader().getHeight()+" is larger than "+Message.MAX_MESSAGE_SIZE+" bytes at "+miningBlockSize+" bytes");
					}
	
					if (miningBlockHeaderSize > maxBlockHeaderSize)
					{
						if (miningBlockHeaderSize - maxBlockHeaderSize > miningSizeAdjustment) 
						{
							miningSizeAdjustment = (miningBlockHeaderSize - maxBlockHeaderSize)*4;
							blocksLog.warn(this.context.getName()+": Block header "+miningBlock.getHeader().getHash()+":"+miningBlock.getHeader().getHeight()+" is larger than expected size "+maxBlockHeaderSize+" bytes at "+miningBlockHeaderSize+" bytes");
							blocksLog.warn(this.context.getName()+":     Max block header size adjusted to "+maxBlockHeaderSize+" bytes");
						}
					}
	
					miningBlock = null;
					maxBlockHeaderSize += miningSizeAdjustment;
					blockSizeRemaining = (Message.MAX_MESSAGE_SIZE-maxBlockHeaderSize) - candidateAcceptedSize;
					blockPayloadSize = candidateAcceptedSize;
					this.inventory.removeAll(InventoryType.COMMITTED);
					this.inventory.removeAll(InventoryType.EXECUTABLE);
					this.inventory.removeAll(InventoryType.LATENT);
					this.inventory.removeAll(InventoryType.UNACCEPTED);
					this.inventory.removeAll(InventoryType.UNCOMMITTED);
					this.inventory.removeAll(InventoryType.UNEXECUTED);
				}
			}
		}
		while(miningBlock == null);

		PendingBlock candidateBlock = new PendingBlock(this.context, miningBlock.getHeader(), this.inventory);
		candidateBlock.getHeader().sign(this.context.getNode().getKeyPair());
		this.context.getMetaData().increment("ledger.proposals.generated");
		
		return candidateBlock;
	}
	
}
