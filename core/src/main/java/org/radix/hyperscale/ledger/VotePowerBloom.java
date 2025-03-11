package org.radix.hyperscale.ledger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Objects;

import org.radix.hyperscale.collections.Bloom;
import org.radix.hyperscale.common.BasicObject;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.crypto.Identity;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.SerializerId2;
import org.radix.hyperscale.serialization.DsonOutput.Output;
import org.radix.hyperscale.utils.Numbers;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("ledger.vote_power_bloom")
public final class VotePowerBloom extends BasicObject
{
	private static final int MAX_VOTE_POWER_SHIFTS = 24;
	private static final double VOTE_POWER_PROBABILITY = 0.000001;
	
	@JsonProperty("block")
	@DsonOutput(Output.ALL)
    private Hash block;

    @JsonProperty("shard_group")
	@DsonOutput(Output.ALL)
    private ShardGroupID shardGroupID;

	@JsonProperty("bloom")
	@DsonOutput(Output.ALL)
    private Bloom bloom;

	@JsonProperty("total_power")
	@DsonOutput(Output.ALL)
    private long totalPower;
	
	@SuppressWarnings("unused")
	private VotePowerBloom()
	{
		// FOR SERIALIZER 
	}
	
	private VotePowerBloom(final Hash block, final ShardGroupID shardGroupID, final long totalPower, Bloom bloom)
	{
		this.block = Objects.requireNonNull(block, "Block hash is null");
		this.bloom = Objects.requireNonNull(bloom, "Bloom is null");

		Numbers.isNegative(totalPower, "Total power is negative");
		this.totalPower = totalPower;
		
		Objects.requireNonNull(shardGroupID, "Shard group ID is null");
		this.shardGroupID = shardGroupID;
	}

	public VotePowerBloom(final Hash block, final ShardGroupID shardGroupID, final int numIdentities)
	{
		this.block = Objects.requireNonNull(block, "Block hash is null");
		this.bloom = new Bloom(VOTE_POWER_PROBABILITY, numIdentities*MAX_VOTE_POWER_SHIFTS);
		this.totalPower = 0l;
		
		Objects.requireNonNull(shardGroupID, "Shard group ID is null");
		this.shardGroupID = shardGroupID;
	}
	
	void add(final Identity identity, final long power)
	{
		Objects.requireNonNull(identity, "Identity is null");
		Numbers.isNegative(power, "Power is negative");

		if (power == 0)
			return;
		
		byte[] identityBytes = identity.toByteArray();
		byte[] bloomHashBytes = new byte[identityBytes.length+1];
		System.arraycopy(identityBytes, 0, bloomHashBytes, 1, identityBytes.length);

		for (int shift = 0 ; shift < MAX_VOTE_POWER_SHIFTS ; shift++)
		{
			bloomHashBytes[0] = (byte) shift;
			if (((power >> shift) & 1) == 1)
				this.bloom.add(bloomHashBytes);
		}
		
		this.totalPower += power;
	}
	
	public long power(final Identity identity)
	{
		Objects.requireNonNull(identity, "Identity is null");
		
		int power = 0;
		byte[] identityBytes = identity.toByteArray();
		byte[] bloomHashBytes = new byte[identityBytes.length+1];
		System.arraycopy(identityBytes, 0, bloomHashBytes, 1, identityBytes.length);
		
		for (int shift = 0 ; shift < MAX_VOTE_POWER_SHIFTS ; shift++)
		{
			bloomHashBytes[0] = (byte) shift;
			if (this.bloom.contains(bloomHashBytes));
				power += (1 << shift);
		}
		
		return power;
	}
	
	public Hash getBlock()
	{
		return this.block;
	}

	public ShardGroupID getShardGroupID()
	{
		return this.shardGroupID;
	}

	public long getTotalPower()
	{
		return this.totalPower;
	}

	public int count()
	{
		return this.bloom.count();
	}
	
    public static VotePowerBloom from(byte[] bytes) throws IOException
    {
    	ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    	DataInputStream dis = new DataInputStream(bais);
    	byte[] hashBytes = new byte[Hash.BYTES];
    	dis.read(hashBytes);
    	Hash block = Hash.from(hashBytes);
    	ShardGroupID shardGroupID = ShardGroupID.from(dis.readInt());
    	long totalPower = dis.readLong();
    	
    	byte[] bloomBytes = new byte[dis.readInt()];
    	dis.read(bloomBytes);
    	Bloom bloom = Bloom.from(bloomBytes);
        return new VotePowerBloom(block, shardGroupID, totalPower, bloom); 
    }
	
	public byte[] toByteArray() throws IOException
	{
	  	ByteArrayOutputStream baos = new ByteArrayOutputStream();
	   	DataOutputStream dos = new DataOutputStream(baos);
	   	dos.write(this.block.toByteArray());
    	dos.writeInt(this.shardGroupID.intValue());
    	dos.writeLong(this.totalPower);
    	byte[] bytes = this.bloom.toByteArray();
    	dos.writeInt(bytes.length);
    	dos.write(bytes);
    	return baos.toByteArray();
    }
}
