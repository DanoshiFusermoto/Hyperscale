package org.radix.hyperscale.ledger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import org.eclipse.collections.api.factory.Maps;
import org.radix.hyperscale.Constants;
import org.radix.hyperscale.Context;
import org.radix.hyperscale.collections.Bloom;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.crypto.Identity;
import org.radix.hyperscale.exceptions.ValidationException;
import org.radix.hyperscale.ledger.AtomStatus.State;
import org.radix.hyperscale.ledger.BlockHeader.InventoryType;
import org.radix.hyperscale.ledger.exceptions.LockException;
import org.radix.hyperscale.ledger.exceptions.PrimitiveLockException;
import org.radix.hyperscale.ledger.exceptions.PrimitiveUnlockException;
import org.radix.hyperscale.ledger.exceptions.StateLockException;
import org.radix.hyperscale.ledger.exceptions.StateUnlockException;
import org.radix.hyperscale.ledger.primitives.Atom;
import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;
import org.radix.hyperscale.network.messages.Message;
import org.radix.hyperscale.serialization.SerializationException;
import org.radix.hyperscale.utils.Numbers;

import com.google.common.annotations.VisibleForTesting;

@VisibleForTesting
public class PendingBranch
{
	private static final Logger stateAccumulatorLog = Logging.getLogger("stateaccumulator");

	public enum Type
	{
		NONE, FORK, MERGE
	}
	
	private static final Logger blocksLog = Logging.getLogger("blocks");

	private final Context context;
	private final long 	id;
	private final Type	type;
	private final Map<Hash, Boolean> meta;
    private final StateAccumulator accumulator;
	private final LinkedList<PendingBlock> blocks;  // TODO ensure supers are updated whenever this is modified!
	
	private BlockHeader root;

	PendingBranch(final Context context, final Type type, final BlockHeader root, final StateAccumulator accumulator)
	{
		this.context = Objects.requireNonNull(context, "Context is null");
		this.type = Objects.requireNonNull(type, "Type is null");
		this.id = ThreadLocalRandom.current().nextLong();
		this.blocks = new LinkedList<PendingBlock>();
		this.meta = Maps.mutable.empty();
		this.root = Objects.requireNonNull(root, "Root is null");
		this.accumulator = Objects.requireNonNull(accumulator, "Accumulator is null");
	}

	PendingBranch(final Context context, final BlockHeader root, final StateAccumulator accumulator, final Collection<PendingBlock> pendingBlocks) throws LockException, IOException, ValidationException
	{
		this(context, Type.NONE, root, accumulator);

		for (PendingBlock pendingBlock : pendingBlocks)
		{
			add(pendingBlock);
			if (pendingBlock.isConstructed() && isApplied(pendingBlock.getHeader().getPrevious()))
				apply(pendingBlock);
		}
	}
		
	PendingBranch(final Context context, final Type type, final BlockHeader root, final StateAccumulator accumulator, final PendingBlock pendingBlock) throws ValidationException, IOException, LockException
	{
		this(context, type, root, accumulator);

		add(pendingBlock);
		if (pendingBlock.isConstructed() && isApplied(pendingBlock.getHeader().getPrevious()))
			apply(pendingBlock);
	}
	
	PendingBranch(final Context context, final Type type, final BlockHeader root, final StateAccumulator accumulator, final Collection<PendingBlock> pendingBlocks) throws ValidationException, LockException, IOException
	{
		this(context, type, root, accumulator);
		
		for (PendingBlock pendingBlock : pendingBlocks)
		{
			add(pendingBlock);
			if (pendingBlock.isConstructed() && isApplied(pendingBlock.getHeader().getPrevious()))
				apply(pendingBlock);
		}
	}
	
	long getID()
	{
		return this.id;
	}
	
	@Override
	public int hashCode()
	{
		return (int) this.id;
	}
	
	@Override
	public boolean equals(Object other)
	{
		if (other == null)
			return false;
		
		if (other == this)
			return true;
		
		if (other instanceof PendingBranch pendingBranch)
		{
			if (pendingBranch.id == this.id)
				return true;
		}
		
		return false;
	}
	
	Type getType()
	{
		return this.type;
	}

	void add(final PendingBlock block) throws IOException, ValidationException
	{
		if (Objects.requireNonNull(block, "Block is null").getHeader() == null)
			throw new IllegalStateException("Block "+block.getHash()+" does not have a header");
	
		synchronized(this)
		{
			try
			{
				if (this.meta.containsKey(block.getHash()))
					throw new ValidationException(block, "Block "+block.getHash()+" is already added to branch "+toString());
				
				if (block.getHeight() <= this.root.getHeight())
					throw new ValidationException(block, "Block is "+block.getHash()+" is before branch root "+this.root.getHash());
				
				if (this.blocks.isEmpty() == false && this.blocks.getLast().getHash().equals(block.getHeader().getPrevious()) == false)
					throw new IllegalStateException("Block "+block.getHash()+" does not attach to end of branch "+toString());
		
				if (this.blocks.isEmpty() == true && this.root.getHash().equals(block.getHeader().getPrevious()) == false)
					throw new IllegalStateException("Block "+block.getHash()+" does not attach to branch root "+toString());

				this.blocks.add(block);
				this.meta.put(block.getHash(), Boolean.FALSE);
				
				if (block.isUnbranched())
					block.setInBranch(this);
		
				if (blocksLog.hasLevel(Logging.DEBUG))
					blocksLog.debug(context.getName()+": Added block "+block.getHeader()+" to branch "+this.type+" "+this.root);
				
				return;
			}
			catch (Throwable t)
			{
				// Record the exception thrown in this pending block
				block.thrown(t);
				
				this.blocks.remove(block);
				meta.remove(block.getHeader().getHash());
				
				throw new ValidationException(block, "Failed to add block "+block.getHeight()+"@"+block.getHash()+" to branch "+this, t);
			}
		}
	}
	
	private void apply(final PendingBlock pendingBlock) throws LockException, ValidationException, SerializationException
	{
		Objects.requireNonNull(pendingBlock, "Pending block is null");
		
		if (pendingBlock.isConstructed() == false)
			throw new IllegalStateException("Can not apply an un-constructed block "+pendingBlock.getHeader());
		
		if (Block.MAX_BLOCK_SIZE != Integer.MAX_VALUE)
		{
			int blockSize = pendingBlock.getBlock().getSize();
			if (blockSize > Message.MAX_MESSAGE_SIZE)
				throw new ValidationException(this, "Block "+pendingBlock.getBlock()+" is too large "+blockSize+" bytes");
		}

		synchronized(this)
		{
			if (pendingBlock.getHeader().getPrevious().equals(this.root.getHash()) == false)
			{
				if (contains(pendingBlock.getHeader().getPrevious()) == false)
					throw new IllegalStateException("Can not apply block "+pendingBlock.getHeader()+" when previous absent");

				if (isApplied(pendingBlock.getHeader().getPrevious()) == false)
					throw new IllegalStateException("Can not apply block "+pendingBlock.getHeader()+" when previous is not applied");
			}
			
			if (pendingBlock.isApplied() == false)
			{
				for (PendingAtom pendingAtom : pendingBlock.<PendingAtom>get(InventoryType.ACCEPTED))
				{
					if (pendingAtom.getStatus().after(State.PREPARED)) 
						throw new ValidationException(pendingAtom, "Accepted atom "+pendingAtom.getAtom()+" has invalid status "+pendingAtom.getStatus());
							
					InventoryType referenceType = references(pendingAtom.getHash(), pendingBlock, BlockHeader.ACCEPT_VERIFY_FILTER);
					if (referenceType != null)
						throw new ValidationException(pendingBlock, "Accepted atom "+pendingAtom.getAtom()+" is "+referenceType+" in branch");
				}
				
				for (PendingAtom pendingAtom : pendingBlock.<PendingAtom>get(InventoryType.UNACCEPTED))
				{
					if (pendingAtom.getStatus().after(State.PREPARED)) 
						throw new ValidationException(pendingAtom, "Unaccepted atom "+pendingAtom.getAtom()+" has invalid status "+pendingAtom.getStatus());
							
					InventoryType referenceType = references(pendingAtom.getHash(), pendingBlock, BlockHeader.UNACCEPT_VERIFY_FILTER);
					if (referenceType != null)
						throw new ValidationException(pendingBlock, "Unaccepted atom "+pendingAtom.getAtom()+" is "+referenceType+" in branch");
				}
				
				for (PendingAtom pendingAtom : pendingBlock.<PendingAtom>get(InventoryType.EXECUTABLE))
				{
					if (pendingAtom.getStatus().after(State.PROVISIONED)) 
						throw new ValidationException(pendingAtom, "Executable atom "+pendingAtom.getAtom()+" has invalid status "+pendingAtom.getStatus());
							
					if (pendingAtom.isExecuteSignalled()) 
						throw new ValidationException(pendingAtom, "Executable atom "+pendingAtom.getAtom()+" is already execute signalled");

					if (pendingAtom.getStatus().after(State.PREPARED) == false && 
						references(pendingAtom.getHash(), pendingBlock, InventoryType.ACCEPTED) == null) 
						throw new ValidationException(pendingAtom, "Executable atom "+pendingAtom.getAtom()+" is not ACCEPTED");

					InventoryType referenceType = references(pendingAtom.getHash(), pendingBlock, BlockHeader.EXECUTE_VERIFY_FILTER);
					if (referenceType != null)
						throw new ValidationException(pendingBlock, "Executable atom "+pendingAtom.getAtom()+" is "+referenceType+" in branch");
				}
				
				for (PendingAtom pendingAtom : pendingBlock.<PendingAtom>get(InventoryType.LATENT))
				{
					if (pendingAtom.getStatus().after(State.PROVISIONED)) 
						throw new ValidationException(pendingAtom, "Latent atom "+pendingAtom.getAtom()+" has invalid status "+pendingAtom.getStatus());
							
					if (pendingAtom.isExecuteLatentSignalled()) 
						throw new ValidationException(pendingAtom, "Latent atom "+pendingAtom.getAtom()+" is already latent execute signalled");

					if (pendingAtom.getStatus().after(State.PREPARED) == false && 
						references(pendingAtom.getHash(), pendingBlock, InventoryType.ACCEPTED) == null) 
						throw new ValidationException(pendingAtom, "Latent atom "+pendingAtom.getAtom()+" is not ACCEPTED");

					InventoryType referenceType = references(pendingAtom.getHash(), pendingBlock, BlockHeader.LATENT_VERIFY_FILTER);
					if (referenceType != null)
					{
						references(pendingAtom.getHash(), pendingBlock, BlockHeader.LATENT_VERIFY_FILTER);
						throw new ValidationException(pendingBlock, "Latent atom "+pendingAtom.getAtom()+" is "+referenceType+" in branch");
					}
				}

				for (PendingAtom pendingAtom : pendingBlock.<PendingAtom>get(InventoryType.UNEXECUTED))
				{
					if (pendingAtom.getStatus().after(State.EXECUTING)) 
						throw new ValidationException(pendingAtom, "Unexecuted atom "+pendingAtom.getAtom()+" has invalid status "+pendingAtom.getStatus());

					if (pendingAtom.getStatus().after(State.PREPARED) == false && 
						references(pendingAtom.getHash(), pendingBlock, InventoryType.ACCEPTED) == null) 
						throw new ValidationException(pendingAtom, "Unexecuted atom "+pendingAtom.getAtom()+" is not ACCEPTED");

					InventoryType referenceType = references(pendingAtom.getHash(), pendingBlock, BlockHeader.UNEXECUTED_VERIFY_FILTER);
					if (referenceType != null)
						throw new ValidationException(pendingBlock, "Unexecuted atom "+pendingAtom.getAtom()+" is "+referenceType+" in branch");
				}
				
				for (PendingAtom pendingAtom : pendingBlock.<PendingAtom>get(InventoryType.COMMITTED))
				{
					if (pendingAtom.getStatus().after(State.FINALIZED)) 
						throw new ValidationException(pendingAtom, "Commit atom "+pendingAtom.getAtom()+" has invalid status "+pendingAtom.getStatus());
							
					if (pendingAtom.getStatus().after(State.PREPARED) == false && 
						references(pendingAtom.getHash(), pendingBlock, InventoryType.ACCEPTED) == null) 
						throw new ValidationException(pendingAtom, "Commit atom "+pendingAtom.getAtom()+" is not ACCEPTED");

					InventoryType referenceType = references(pendingAtom.getHash(), pendingBlock, BlockHeader.COMMIT_VERIFY_FILTER);
					if (referenceType != null)
						throw new ValidationException(pendingBlock, "Commit atom "+pendingAtom.getAtom()+" is "+referenceType+" in branch");
				}

				for (PendingAtom pendingAtom : pendingBlock.<PendingAtom>get(InventoryType.UNCOMMITTED))
				{
					if (pendingAtom.getStatus().after(State.FINALIZED)) 
						throw new ValidationException(pendingAtom, "Uncommitted atom "+pendingAtom.getAtom()+" has invalid status "+pendingAtom.getStatus());
							
					if (pendingAtom.getStatus().after(State.PREPARED) == false && 
						references(pendingAtom.getHash(), pendingBlock, InventoryType.ACCEPTED) == null) 
						throw new ValidationException(pendingAtom, "Uncommitted atom "+pendingAtom.getAtom()+" is not ACCEPTED");

					InventoryType referenceType = references(pendingAtom.getHash(), pendingBlock, BlockHeader.UNCOMMITTED_VERIFY_FILTER);
					if (referenceType != null)
						throw new ValidationException(pendingBlock, "Uncommitted atom "+pendingAtom.getAtom()+" is "+referenceType+" in branch");
				}

				for (Hash item : pendingBlock.getHeader().getInventory(InventoryType.PACKAGES))
				{
					if (references(item, pendingBlock, InventoryType.PACKAGES) != null)
						throw new ValidationException(pendingBlock, "Package "+item+" is already in branch");
				}
			
				unlockable(pendingBlock.get(InventoryType.COMMITTED));
				unlockable(pendingBlock.get(InventoryType.UNEXECUTED));
				unlockable(pendingBlock.get(InventoryType.UNCOMMITTED));
				lockable(pendingBlock.get(InventoryType.ACCEPTED));
				
				pendingBlock.setApplied();
			}

			this.meta.put(pendingBlock.getHash(), Boolean.TRUE);
			
			if (blocksLog.hasLevel(Logging.DEBUG))
				blocksLog.debug(context.getName()+": Applied block "+pendingBlock+" to "+this.type+" "+this.root+" -> "+this.accumulator.locked().size()+" locked in accumulator "+this.accumulator.locked().stream().map(StateAddress::getHash).reduce((a, b) -> Hash.hash(a, b)));
		}
	}
	
	void update() throws LockException, ValidationException, SerializationException
	{
		synchronized(this)
		{
			// Apply any pending proposals referenced by this branch that have not yet been applied
			final Iterator<PendingBlock> blocksIterator = this.blocks.iterator();
			while(blocksIterator.hasNext())
			{
				final PendingBlock vertex = blocksIterator.next();

				if (vertex.isConstructed() == false)
					continue;

				if (isApplied(vertex.getHash()) == false)
				{
					try
					{
						if(isApplied(vertex.getHeader().getPrevious()))
							apply(vertex);
						else
							break;
					}
					catch (Throwable t)
					{
						// Record the exception thrown in this pending block
						vertex.thrown(t);
						
						// Rest of the branch can not be applied if a StateLockException is thrown here so truncate it
						// TODO should this be somewhere else?
						
						blocksIterator.remove();
						meta.remove(vertex.getHeader().getHash());
						while(blocksIterator.hasNext())
						{
							meta.remove(blocksIterator.next().getHeader().getHash());
							blocksIterator.remove();
						}			
						
						throw new ValidationException(vertex, "Failed to add block "+vertex.getHeight()+"@"+vertex.getHash()+" to branch "+this, t);
					}
				}
			}
		}
	}

	boolean contains(final Hash block)
	{
		Objects.requireNonNull(block, "Block is null");
		Hash.notZero(block, "Block hash is ZERO");
		
		synchronized(this)
		{
			return this.meta.containsKey(block);
		}
	}

	boolean contains(final PendingBlock pendingBlock)
	{
		Objects.requireNonNull(pendingBlock, "Pending block is null");

		synchronized(this)
		{
			return this.meta.containsKey(pendingBlock.getHash());
		}
	}

	InventoryType references(final Hash item, final InventoryType ... types)
	{
		return references(item, null, types);
	}
	
	InventoryType references(final Hash item, final PendingBlock excludeTo, final InventoryType ... types)
	{
		Objects.requireNonNull(type, "Inventory type is null");
		Objects.requireNonNull(item, "Item hash is null");
		Hash.notZero(item, "Item hash is ZERO");
		
		synchronized(this)
		{
			for (int b = 0 ; b < this.blocks.size() ; b++)
			{
				final PendingBlock pendingBlock = this.blocks.get(b);
				if (excludeTo != null && pendingBlock.getHeight() >= excludeTo.getHeight())
					continue;
				
				final InventoryType referenceType = pendingBlock.references(item, types);
				if (referenceType != null)
					return referenceType;
			}
			
			return null;
		}
	}

	boolean isCanonical()
	{
		synchronized(this)
		{
			if (this.blocks.isEmpty())
				return true;
			
			PendingBlock current = null;
			final Iterator<PendingBlock> blockIterator = this.blocks.descendingIterator();
			while(blockIterator.hasNext())
			{
				final PendingBlock previous = blockIterator.next();
				if (current != null)
				{
					if (previous.getHash().equals(current.getHeader().getPrevious()) == false)
						return false;
					
					current = previous;
				}
				
				current = previous;
				if (current.getHeader().getPrevious().equals(this.root.getHash()))
					return true;
			}
			
			return false;
		}
	}

	boolean isMergable(final Collection<PendingBlock> blocks)
	{
		Objects.requireNonNull(blocks, "Pending blocks is null");
		Numbers.isZero(blocks.size(), "Pending blocks is empty");

		PendingBlock last = null;
		for (final PendingBlock block : blocks)
		{
			if (last == null || last.getHeight() < block.getHeight())
				last = block;
		}

		synchronized(this)
		{
			if (this.blocks.contains(last))
				return false;
			
			final Hash branchTip; 
			if (this.blocks.isEmpty())
				branchTip = this.root.getHash();
			else
				branchTip = this.blocks.getLast().getHash();
			
			boolean mergable = false;
			for (final PendingBlock block : blocks)
			{
				if (block.getHeader() == null)
					throw new IllegalStateException("Block "+block.getHash()+" does not have a header");
				
				if (branchTip.equals(block.getHeader().getPrevious()) == false)
					continue;
				
				mergable = true;
				break;
			}
			
			return mergable;
		}
	}
	
	void merge(final Collection<PendingBlock> blocks) throws IOException, ValidationException, LockException
	{
		Objects.requireNonNull(blocks, "Pending blocks is null");
		Numbers.isZero(blocks.size(), "Pending blocks is empty");

		final List<PendingBlock> sortedBlocks = new ArrayList<PendingBlock>(blocks);
		Collections.sort(sortedBlocks, (pb1, pb2) -> {
			if (pb1.getHeight() < pb2.getHeight())
				return -1;
				
			if (pb1.getHeight() > pb2.getHeight())
				return 1;

			return 0;
		});

		final List<PendingBlock> mergeBlocks = new ArrayList<PendingBlock>(blocks.size());
		synchronized(this)
		{
			// Determine the blocks to actually merge onto this branch
			Hash previous; 
			if (this.blocks.isEmpty())
				previous = this.root.getHash();
			else
				previous = this.blocks.getLast().getHash();

			for (int i = 0 ; i < sortedBlocks.size() ; i++)
			{
				final PendingBlock sortedBlock = sortedBlocks.get(i);
				if (sortedBlock.getHeader() == null)
					throw new IllegalStateException("Block "+sortedBlock.getHash()+" does not have a header");
				
				if (sortedBlock.getHeight() <= Block.toHeight(previous))
					continue;
				else if (this.meta.containsKey(sortedBlock.getHash()))
				{
					previous = sortedBlock.getHash();
					continue;
				}
				else if (sortedBlock.getHeader().getPrevious().equals(previous))
				{
					mergeBlocks.add(sortedBlock);
					previous = sortedBlock.getHash();
				}
				else
					throw new IllegalStateException("Merge block "+sortedBlock+" does not attach to previous "+previous+" in branch "+this);
			}
			
			if (mergeBlocks.isEmpty() == false)
			{
				if (blocksLog.hasLevel(Logging.DEBUG))
					blocksLog.debug(this.context.getName()+": Merging branch "+this.blocks+" with "+blocks);
				
				for (PendingBlock mergeBlock : mergeBlocks)
				{
					if (blocksLog.hasLevel(Logging.DEBUG))
						blocksLog.debug(this.context.getName()+": Adding merge block "+mergeBlock);

					add(mergeBlock);
				}
			}
		}
	}

	boolean isFork(final Collection<PendingBlock> blocks)
	{
		Objects.requireNonNull(blocks, "Pending blocks is null");
		Numbers.isZero(blocks.size(), "Pending blocks is empty");

		final List<PendingBlock> sortedBlocks = new ArrayList<PendingBlock>(blocks);
		Collections.sort(sortedBlocks, (pb1, pb2) -> {
			if (pb1.getHeight() < pb2.getHeight())
				return -1;
				
			if (pb1.getHeight() > pb2.getHeight())
				return 1;

			return 0;
		});

		synchronized(this)
		{
			for (final PendingBlock sortedBlock : sortedBlocks)
			{
				if (sortedBlock.getHeader() == null)
					throw new IllegalStateException("Block "+sortedBlock.getHash()+" does not have a header");

				for (final PendingBlock vertex : this.blocks)
				{
					if (vertex.getHash().equals(sortedBlock.getHeader().getPrevious()))
					{
						if (vertex.equals(this.blocks.getLast()) == false)
							return true;
						else
							return false;
					}
				}
			}
			
			return false;
		}
	}

	PendingBranch fork(final Collection<PendingBlock> blocks) throws LockException, IOException, ValidationException
	{
		Objects.requireNonNull(blocks, "Pending blocks is null");
		Numbers.isZero(blocks.size(), "Pending blocks is empty");

		final List<PendingBlock> sortedBlocks = new ArrayList<PendingBlock>(blocks);
		Collections.sort(sortedBlocks, (pb1, pb2) -> {
			if (pb1.getHeight() < pb2.getHeight())
				return -1;
				
			if (pb1.getHeight() > pb2.getHeight())
				return 1;

			return 0;
		});

		synchronized(this)
		{
			PendingBlock forkBlock = null;
			PendingBranch forkBranch = null;
			for (final PendingBlock sortedBlock : sortedBlocks)
			{
				if (sortedBlock.getHeader() == null)
					throw new IllegalStateException("Block "+sortedBlock.getHash()+" does not have a header");

				for (final PendingBlock vertex : this.blocks)
				{
					if (vertex.getHash().equals(sortedBlock.getHeader().getPrevious()))
					{
						if (vertex.equals(this.blocks.getLast()) == false)
						{
							forkBlock = vertex;
							break;
						}
					}
				}
					
				if (forkBlock != null)
					break;
			}

			if (forkBlock != null)
			{
				final List<PendingBlock> forkBlocks = new ArrayList<PendingBlock>();
				for (final PendingBlock vertex : this.blocks)
				{
					forkBlocks.add(vertex);
					if (vertex.equals(forkBlock))
						break;
				}
				
				for (final PendingBlock sortedBlock : sortedBlocks)
				{
					if (sortedBlock.getHeight() <= forkBlock.getHeight())
						continue;
					
					forkBlocks.add(sortedBlock);
				}
				
				forkBranch = new PendingBranch(this.context, Type.FORK, this.root, this.accumulator, forkBlocks);
			}
			
			return forkBranch;
		}
	}

	boolean intersects(final PendingBlock block)
	{
		if (Objects.requireNonNull(block, "Block is null").getHeader() == null)
			throw new IllegalStateException("Block "+block.getHash()+" does not have a header");

		synchronized(this)
		{
			if (this.meta.containsKey(block.getHeader().getPrevious()))
				return true;
			
			return false;
		}
	}
	
	boolean intersects(final PendingBranch branch)
	{
		Objects.requireNonNull(branch, "Pending branch is null");
		Numbers.isZero(branch.blocks.size(), "Pending blocks is empty");

		final List<PendingBlock> sortedBlocks;
		synchronized(branch)
		{
			sortedBlocks = new ArrayList<PendingBlock>(branch.blocks);
		}
		
		Collections.sort(sortedBlocks, (pb1, pb2) -> {
			if (pb1.getHeight() < pb2.getHeight())
				return -1;
				
			if (pb1.getHeight() > pb2.getHeight())
				return 1;

			return 0;
		});

		synchronized(this)
		{
			for (final PendingBlock sortedBlock : sortedBlocks)
				if (this.meta.containsKey(sortedBlock.getHeader().getPrevious()))
					return true;
			
			return false;
		}
	}
	
	boolean intersects(final Collection<PendingBlock> blocks)
	{
		Objects.requireNonNull(blocks, "Pending blocks is null");
		Numbers.isZero(blocks.size(), "Pending blocks is empty");

		final List<PendingBlock> sortedBlocks = new ArrayList<PendingBlock>(blocks);
		Collections.sort(sortedBlocks, (pb1, pb2) -> {
			if (pb1.getHeight() < pb2.getHeight())
				return -1;
				
			if (pb1.getHeight() > pb2.getHeight())
				return 1;

			return 0;
		});

		synchronized(this)
		{
			for (final PendingBlock sortedBlock : sortedBlocks)
				if (this.meta.containsKey(sortedBlock.getHeader().getPrevious()))
					return true;
			
			return false;
		}
	}

	/**
	 * Trims the branch to the block header (inclusive)
	 * 
	 * @param header
	 */
	Collection<PendingBlock> trimTo(final BlockHeader header)
	{
		Objects.requireNonNull(header, "Block is null");

		final List<PendingBlock> trimmed = new ArrayList<PendingBlock>();
		synchronized(this)
		{
			final Iterator<PendingBlock> vertexIterator = this.blocks.iterator();
			while(vertexIterator.hasNext())
			{
				final PendingBlock vertex = vertexIterator.next();
				if (vertex.getHeader().getHeight() <= header.getHeight())
				{
					remove(vertex);

					if (this.meta.remove(vertex.getHash()).equals(Boolean.FALSE))
						blocksLog.warn(this.context.getName()+": Removing unapplied block "+vertex+" from "+this.root+" "+this.blocks.getLast().getHeader());
					
					vertexIterator.remove();
					trimmed.add(vertex);
					this.root = vertex.getHeader();
				}
			}
		}

		return trimmed;
	}

	/**
	 * Trims the branch from the block header (inclusive)
	 * 
	 * @param header
	 */
	Collection<PendingBlock> trimFrom(final BlockHeader header)
	{
		Objects.requireNonNull(header, "Block is null");

		final List<PendingBlock> trimmed = new ArrayList<PendingBlock>();
		synchronized(this)
		{
			final Iterator<PendingBlock> vertexIterator = this.blocks.iterator();
			while(vertexIterator.hasNext())
			{
				final PendingBlock vertex = vertexIterator.next();
				if (vertex.getHeader().getHeight() >= header.getHeight())
				{
					remove(vertex);

					if (this.meta.remove(vertex.getHash()).equals(Boolean.FALSE))
						blocksLog.warn(this.context.getName()+": Removing unapplied block "+vertex+" from "+this.root+" "+this.blocks.getLast().getHeader());
					
					vertexIterator.remove();
					trimmed.add(vertex);
				}
			}
		}

		return trimmed;
	}

	List<PendingBlock> committed(final PendingBlock block)
	{
		if (Objects.requireNonNull(block, "Block is null").getHeader() == null)
			throw new IllegalStateException("Block "+block.getHash()+" does not have a header");

		final LinkedList<PendingBlock> committed = new LinkedList<PendingBlock>();
		synchronized(this)
		{
			final Iterator<PendingBlock> vertexIterator = this.blocks.iterator();
			while(vertexIterator.hasNext())
			{
				final PendingBlock vertex = vertexIterator.next();
				if (vertex.isConstructed() == false)
					throw new IllegalStateException("Unconstructed proposal "+vertex.getHeader()+" found when commiting to "+block.getHeader());
				
				committed.add(vertex);
				
				remove(vertex);

				this.meta.remove(vertex.getHash());
				vertexIterator.remove();
				
				if (vertex.getHash().equals(block.getHash()))
					break;
			}
			
			if (committed.isEmpty() == false)
			{
				if (committed.getLast().equals(block) == false)
					blocksLog.warn(this.context.getName()+": Committed partial branch "+this.root+" -> "+committed.getLast().getHeader());

				this.root = committed.getLast().getHeader();
			}
		}

		return committed;
	}
	
	private void remove(final PendingBlock pendingBlock)
	{
	}
	
	PendingBlock commitable(int superCount)
	{
	    synchronized(this)
	    {
	        // Check if we have enough super blocks to satisfy consensus rules
	    	//
			// Blocks to be committed require at least one "confirming" super block higher than it, thus there will always be at least one super block in a pending branch.
			// The required quantity of super blocks in a branch is defined by 'superCount' and may be larger than 2 depending on certain conditions.
	        final LinkedList<PendingBlock> supers = supers();
	        if (supers.size() < Math.max(superCount, Constants.MIN_COMMIT_SUPERS))
	            return null;

	        if (blocksLog.hasLevel(Logging.DEBUG))
	            blocksLog.debug(this.context.getName()+": Found commit branch "+this+" with supers "+supers);

	        // Find the highest constructed super that is NOT the last super
	        PendingBlock highestConstructedSuper = null;
	        for (final PendingBlock superBlock : supers) 
	        {
	            // Break on the last super
	            if (supers.getLast().equals(superBlock))
	                break;
	                
	            // Also break on a non-constructed supers
	            if (superBlock.isConstructed() == false)
	                break;
	                
	            highestConstructedSuper = superBlock;
	        }
	        
	        if (highestConstructedSuper == null)
	            return null;

	        // Check if all blocks up to this super are constructed
	        boolean allConstructed = true;
	        PendingBlock lastConstructedSuper = null;
	        
	        for (final PendingBlock vertex : this.blocks) 
	        {
	            // If we've reached our target super, we're done
	            if (highestConstructedSuper.equals(vertex))
	                break;
	                
	            // If we hit a non-constructed block, we can't commit to our target
	            if (vertex.isConstructed() == false) 
	            {
	                allConstructed = false;
	                break;
	            }
	            
	            // Keep track of the last constructed super seen
	            if (supers.contains(vertex) && vertex.isConstructed())
	                lastConstructedSuper = vertex;
	        }
	        
	        // If all blocks up to our target are constructed, return the target
	        // Otherwise, return the last constructed super we found
	        PendingBlock commitable = allConstructed ? highestConstructedSuper : lastConstructedSuper;

	        if (commitable != null && blocksLog.hasLevel(Logging.INFO))
	            blocksLog.info(this.context.getName()+": Found commitable block "+commitable+" in branch "+this);

	        return commitable;
	    }
	}	

	LinkedList<PendingBlock> supers()
	{
		final LinkedList<PendingBlock> supers = new LinkedList<PendingBlock>();
		synchronized(this)
		{	
			final Iterator<PendingBlock> vertexIterator = this.blocks.iterator();
			while(vertexIterator.hasNext())
			{
				final PendingBlock vertex = vertexIterator.next();
				if (vertex.isSuper())
					supers.add(vertex);
			}
		}
		return supers;
	}
	
	private boolean isApplied(final Hash vertex)
	{
		synchronized(this)
		{
			if (vertex.equals(this.root.getHash()))
				return true;
			
			return this.meta.getOrDefault(vertex, Boolean.FALSE).equals(Boolean.TRUE);
		}
	}

	private void lockable(final Collection<PendingAtom> pendingAtoms) throws LockException
	{
		Objects.requireNonNull(pendingAtoms, "Pending atoms is null");
		
		if (pendingAtoms.isEmpty())
			return;
		
		synchronized(this)
		{
			for (PendingAtom pendingAtom : pendingAtoms)
				lockable(pendingAtom);
		}
	}
	
	void lockable(final PendingAtom pendingAtom) throws LockException
	{
		Objects.requireNonNull(pendingAtom, "Pending atom is null");
		
		if (stateAccumulatorLog.hasLevel(Logging.DEBUG))
			stateAccumulatorLog.debug(this.context.getName()+": Locking state in "+pendingAtom);

		lockable(pendingAtom, pendingAtom.getStateAddresses(StateLockMode.WRITE));
	}
	
	void lockable(final PendingAtom pendingAtom, final Collection<StateAddress> stateAddresses) throws LockException
	{
		Objects.requireNonNull(pendingAtom, "Pending atom is null");
		Objects.requireNonNull(stateAddresses, "State addresses is null");
			
		final List<StateAddress> stateAddressesCopy = new ArrayList<>(stateAddresses);
		synchronized(this)
		{
			for (int i = this.blocks.size()-1; i >= 0; i--) 
			{
                final PendingBlock pendingBlock = this.blocks.get(i);
				if (isApplied(pendingBlock.getHeader().getHash()) == false)
					continue;
					
				if(pendingBlock.isLocked(pendingAtom.getAddress()))
					throw new PrimitiveLockException(Atom.class, pendingAtom.getHash());

				Iterator<StateAddress> stateAddressesIterator = stateAddressesCopy.iterator();
				while(stateAddressesIterator.hasNext())
				{
					final StateAddress stateAddress = stateAddressesIterator.next();
					if (pendingBlock.isUnlocked(stateAddress)) 
					{
						stateAddressesIterator.remove();
						continue;
					}

					if (pendingBlock.isLocked(stateAddress)) 
						throw new StateLockException(stateAddress, pendingAtom.getHash(), pendingBlock.getLockedBy(stateAddress).getHash());
				}

				if (stateAddressesCopy.isEmpty())
					break;
			}
			
			if(this.accumulator.isLocked(pendingAtom.getAddress()))
				throw new PrimitiveLockException(Atom.class, pendingAtom.getHash());

			this.accumulator.lockable(stateAddressesCopy, pendingAtom);
		}
		
		if (stateAccumulatorLog.hasLevel(Logging.DEBUG))
		{
			for (StateAddress stateAddress : stateAddresses)
				stateAccumulatorLog.debug(this.context.getName()+": Lockable state address "+stateAddress+" via "+pendingAtom);
		}
	}
	
	void lockable(final PendingAtom pendingAtom, final StateAddress stateAddress) throws LockException
	{
		Objects.requireNonNull(pendingAtom, "Pending atom is null");
		Objects.requireNonNull(stateAddress, "State address is null");
			
		synchronized(this)
		{
			boolean checkAccumulator = true;
			for (int i = this.blocks.size()-1; i >= 0; i--) 
			{
                final PendingBlock pendingBlock = this.blocks.get(i);
				if (isApplied(pendingBlock.getHeader().getHash()) == false)
					continue;
					
				if(pendingBlock.isLocked(pendingAtom.getAddress()))
					throw new PrimitiveLockException(Atom.class, pendingAtom.getHash());

				if (pendingBlock.isUnlocked(stateAddress)) 
				{
					checkAccumulator = false;
					continue;
				}

				if (pendingBlock.isLocked(stateAddress)) 
					throw new StateLockException(stateAddress, pendingAtom.getHash(), pendingBlock.getLockedBy(stateAddress).getHash());
			}
			
			if(this.accumulator.isLocked(pendingAtom.getAddress()))
				throw new PrimitiveLockException(Atom.class, pendingAtom.getHash());

			if (checkAccumulator)
			{
				if (this.accumulator.isLocked(stateAddress)) 
					throw new StateLockException(stateAddress, pendingAtom.getHash(), this.accumulator.getLockedBy(stateAddress).getHash());
			}
		}
		
		if (stateAccumulatorLog.hasLevel(Logging.DEBUG))
			stateAccumulatorLog.debug(this.context.getName()+": Lockable state address "+stateAddress+" via "+pendingAtom);
	}

	void unlockable(final Collection<PendingAtom> pendingAtoms) throws LockException
	{
		Objects.requireNonNull(pendingAtoms, "Pending atoms is null");
		
		if (pendingAtoms.isEmpty())
			return;
		
		synchronized(this)
		{
			for (PendingAtom pendingAtom : pendingAtoms)
				unlockable(pendingAtom);
		}
	}

	void unlockable(final PendingAtom pendingAtom) throws LockException
	{
		Objects.requireNonNull(pendingAtom, "Pending atom is null");
		
		if (stateAccumulatorLog.hasLevel(Logging.DEBUG))
			stateAccumulatorLog.debug(this.context.getName()+": Unlocking state in "+pendingAtom);

		unlockable(pendingAtom, pendingAtom.getStateAddresses(StateLockMode.WRITE));
	}
			
	void unlockable(final PendingAtom pendingAtom, final Collection<StateAddress> stateAddresses) throws LockException
	{
		Objects.requireNonNull(pendingAtom, "Pending atom is null");
		Objects.requireNonNull(pendingAtom, "State addresses is null");
		
		final List<StateAddress> stateAddressesCopy = new ArrayList<>(stateAddresses);
		synchronized(this)
		{
			for (int i = this.blocks.size()-1; i >= 0; i--) 
			{
                final PendingBlock pendingBlock = this.blocks.get(i);
				if (isApplied(pendingBlock.getHeader().getHash()) == false)
					continue;
					
				if(pendingBlock.isUnlocked(pendingAtom.getAddress()))
					throw new PrimitiveUnlockException(Atom.class, pendingAtom.getHash());

				final Iterator<StateAddress> stateAddressesIterator = stateAddressesCopy.iterator();
				while(stateAddressesIterator.hasNext())
				{
					final StateAddress stateAddress = stateAddressesIterator.next();
					if (pendingBlock.isLocked(stateAddress)) 
					{
						stateAddressesIterator.remove();
						continue;
					}

					if (pendingBlock.isUnlocked(stateAddress)) 
						throw new StateUnlockException(stateAddress, pendingAtom.getHash());
				}
				
				if (stateAddressesCopy.isEmpty())
					break;
			}
			
			if(this.accumulator.isLocked(pendingAtom.getAddress()) == false)
				throw new PrimitiveUnlockException(Atom.class, pendingAtom.getHash());

			this.accumulator.unlockable(stateAddressesCopy, pendingAtom);
		}
		
		if (stateAccumulatorLog.hasLevel(Logging.DEBUG))
		{
			for (final StateAddress stateAddress : stateAddresses)
				stateAccumulatorLog.debug(this.context.getName()+": Unlockable state address "+stateAddress+" via "+pendingAtom);
		}
	}

	BlockHeader getRoot()
	{
		synchronized(this)
		{
			return this.root;
		}
	}
	
	public StateAccumulator getStateAccumulator() 
	{
		synchronized(this)
		{
			return this.accumulator;
		}
	}


	public LinkedList<PendingBlock> getBlocks()
	{
		synchronized(this)
		{
			return new LinkedList<PendingBlock>(this.blocks);
		}
	}

	LinkedList<PendingBlock> getBlocksTo(Hash block)
	{
		synchronized(this)
		{
			final LinkedList<PendingBlock> blocks = new LinkedList<PendingBlock>();
			for (final PendingBlock pendingBlock : this.blocks)
			{
				blocks.add(pendingBlock);
				if (pendingBlock.getHash().equals(block))
					return blocks;
			}
			
			return new LinkedList<PendingBlock>();
		}
	}

	boolean isEmpty()
	{
		synchronized(this)
		{
			return this.blocks.isEmpty();
		}
	}
	
	public PendingBlock get(final Hash block)
	{
		Objects.requireNonNull(block, "Block hash is null");
		Hash.notZero(block, "Block hash is ZERO");
		
		synchronized(this)
		{
			for (final PendingBlock vertex : this.blocks)
				if (vertex.getHash().equals(block))
					return vertex;
			
			return null;
		}
	}


	PendingBlock getLow()
	{
		synchronized(this)
		{
			return this.blocks.getFirst();
		}
	}

	PendingBlock getHigh()
	{
		synchronized(this)
		{
			return this.blocks.getLast();
		}
	}
	
	@Override
	public String toString()
	{
		return this.root.toString()+" / "+getBlocks().stream().map(pb -> pb.getHash().toString()).collect(Collectors.joining(" -> "));
	}

	public int size()
	{
		synchronized(this)
		{
			return this.blocks.size();
		}
	}
	
	// Vote power and weights //
	long getVotePower(final long height, final Identity identity) throws IOException
	{
		return getVotePower(height, identity, true);
	}

	long getVotePower(final long height, final Identity identity, boolean matured) throws IOException
	{
		Objects.requireNonNull(identity, "Identity is null");
		Numbers.isNegative(height, "Height is negative");
		
		final Epoch epoch = Epoch.from(this.root);
		return this.context.getLedger().getValidatorHandler().getVotePower(epoch, identity);

		// TODO Liveness recovery piece goes here // REMOVED FOR OPEN SOURCE
	}

	public long getVotePower(final long height, final Bloom owners) throws IOException
	{
		Objects.requireNonNull(owners, "Identities is null");

		final Epoch epoch = Epoch.from(this.root);
		return this.context.getLedger().getValidatorHandler().getVotePower(epoch, owners);
		
		// TODO Liveness recovery piece goes here // REMOVED FOR OPEN SOURCE
	}

	long getVotePowerThreshold(final long height) throws IOException
	{
		Numbers.isNegative(height, "Height is negative");
		return twoFPlusOne(getTotalVotePower(height));
	}

	long getTotalVotePower(final long height) throws IOException
	{
		Numbers.isNegative(height, "Height is negative");
		
		final Epoch epoch = Epoch.from(this.root);
		final int numShardGroups = this.context.getLedger().numShardGroups(epoch);
		final ShardGroupID localShardGroupID = ShardMapper.toShardGroup(this.context.getNode().getIdentity(), numShardGroups); 
		return this.context.getLedger().getValidatorHandler().getTotalVotePower(epoch, localShardGroupID);
		
		// TODO Liveness recovery piece goes here // REMOVED FOR OPEN SOURCE
	}
	
	private long twoFPlusOne(final long power)
	{
		Numbers.isNegative(power, "Power is negative");

		long F = Math.max(1, power / 3);
		long TWOF = F * 2;
		long T = Math.min(power, TWOF + 1);
		return T;
	}
	
	PendingBlock getBlockAtHeight(final long height)
	{
		Numbers.isNegative(height, "Height is negative");
		
		synchronized(this)
		{
			for (final PendingBlock vertex : this.blocks)
			{
				if (vertex.getHeight() == height)
					return vertex;
			}

			return null;
		}
	}

	/**
	 * Returns a {@link BlockHeader} which represents the highest fully qualified build head in this branch up to the specified progress round. 
	 * <br><br>
	 * NOTE Does not consider commit blocks that may be after the build header. 
	 * @return BlockHeader
	 */
	BlockHeader getBuildable(final ProgressRound round)
	{
		Objects.requireNonNull(round, "Progress round is null");

		synchronized(this)
		{
			if (this.root.getHeight() >= round.clock())
				return null;
			
			BlockHeader buildableHead = this.root;
			for (final PendingBlock vertex : this.blocks)
			{
				if (vertex.getHeight() < round.clock() && vertex.isConstructed() == true && isApplied(vertex.getHash()) == true && vertex.getHeight() > buildableHead.getHeight())
					buildableHead = vertex.getHeader();
				else
					break;
			}
			
			return buildableHead;
		}
	}
	
	public boolean isBuildable(final BlockHeader header)
	{
		synchronized(this)
		{
			if (this.root.getHash().equals(header.getHash()))
				return true;
			
			for (final PendingBlock vertex : this.blocks)
			{
				if (vertex.getHash().equals(header.getHash()) && vertex.isConstructed() == true && isApplied(vertex.getHash()) == true)
					return true;
			}
			
			return false;
		}
	}
	
	public boolean isBuildable() 
	{
		synchronized(this)
		{
			if (isConstructed() == false || isPrepared() == false || isCanonical() == false)
				return false;
			
			return true;
		}
	}

	public boolean isConstructed()
	{
		synchronized(this)
		{
			for (final PendingBlock vertex : this.blocks)
			{
				if (vertex.getBlock() == null)
					return false;
			}
			return true;
		}
	}
	
	public boolean isPrepared()
	{
		synchronized(this)
		{
			for (final PendingBlock vertex : this.blocks)
			{
				if (isApplied(vertex.getHash()) == false)
					return false;
			}
			return true;
		}
	}
}
