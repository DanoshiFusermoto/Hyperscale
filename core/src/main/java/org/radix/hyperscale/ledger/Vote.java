package org.radix.hyperscale.ledger;

import java.util.Objects;

import org.radix.hyperscale.common.ExtendedObject;
import org.radix.hyperscale.crypto.CryptoException;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.crypto.KeyPair;
import org.radix.hyperscale.crypto.PublicKey;
import org.radix.hyperscale.crypto.Signature;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.DsonOutput.Output;
import org.radix.hyperscale.utils.Numbers;

import com.fasterxml.jackson.annotation.JsonProperty;

abstract class Vote<KP extends KeyPair<?, K, S>, K extends PublicKey<S>, S extends Signature> extends ExtendedObject 
{
	@JsonProperty("object")
	@DsonOutput(Output.ALL)
	private Hash object;

	@JsonProperty("decision")
	@DsonOutput(Output.ALL)
	private CommitDecision decision;

	@JsonProperty("owner")
	@DsonOutput(Output.ALL)
	private K owner;

	@JsonProperty("signature")
	@DsonOutput(value = {Output.API, Output.WIRE, Output.PERSIST})
	private S signature;
	
	private volatile transient long weight = -1;
	
	Vote()
	{
		// For serializer
	}
	
	Vote(final Hash object, final CommitDecision decision)
	{
		this.object = Objects.requireNonNull(object, "Object is null");
		Hash.notZero(object, "Object hash is ZERO");
		
		// TODO check object is serializable
		
		this.decision = Objects.requireNonNull(decision, "Decision is null");
	}

	protected Vote(final Hash object, final CommitDecision decision, final K owner)
	{
		this.object = Objects.requireNonNull(object, "Object is null");
		Hash.notZero(object, "Object hash is ZERO");
		
		// TODO check object is serializable
		
		this.owner = Objects.requireNonNull(owner, "Owner is null");
		this.decision = Objects.requireNonNull(decision, "Decision is null");
	}

	protected Vote(final Hash object, final CommitDecision decision, final K owner, final long weight)
	{
		this.object = Objects.requireNonNull(object, "Object is null");
		Hash.notZero(object, "Object hash is ZERO");
		
		// TODO check object is serializable
		
		this.owner = Objects.requireNonNull(owner, "Owner is null");
		this.decision = Objects.requireNonNull(decision, "Decision is null");
		this.weight = Numbers.isNegative(weight, "Weight is negative");
	}

/*	protected Vote(final Hash object, final CommitDecision decision, final K owner, final S signature)
	{
		this.object = Objects.requireNonNull(object, "Object is null");
		Hash.notZero(object, "Object hash is ZERO");
		this.owner = Objects.requireNonNull(owner, "Owner is null");
		this.signature = Objects.requireNonNull(signature, "Signature is null");
		this.decision = Objects.requireNonNull(decision, "Decision is null");
	}*/
	
	protected Vote(final Hash object, final CommitDecision decision, final K owner, final S signature, final long weight)
	{
		this.object = Objects.requireNonNull(object, "Object is null");
		Hash.notZero(object, "Object hash is ZERO");
		this.owner = Objects.requireNonNull(owner, "Owner is null");
		this.signature = Objects.requireNonNull(signature, "Signature is null");
		this.decision = Objects.requireNonNull(decision, "Decision is null");
		this.weight = Numbers.isNegative(weight, "Weight is negative");
	}

	@Override
	public int hashCode() 
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + this.object.hashCode();
		result = prime * result + this.decision.hashCode();
		result = prime * result + this.owner.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object object)
	{
		if (object == null)
			return false;

		if (object == this)
			return true;

		if (object instanceof Vote vote)
		{
			if (vote.decision.equals(this.decision) == false)
				return false;

			if (vote.object.equals(this.object) == false)
				return false;
			
			if (vote.owner.equals(this.owner) == false)
				return false;

			return true;
		}
		
		return false;
	}

	final Hash getObject()
	{
		return this.object;
	}

	public final CommitDecision getDecision()
	{
		return this.decision;
	}

	public final K getOwner()
	{
		return this.owner;
	}
	
	public final void sign(final KP key) throws CryptoException
	{
		Objects.requireNonNull(key, "Key pair is null");
		
		synchronized(this)
		{
			if (this.signature != null)
				throw new IllegalStateException("Vote "+getClass()+" is already signed "+this);
	
			if (key.getPublicKey().equals(getOwner()) == false)
				throw new CryptoException("Attempting to sign with key that doesn't match owner");
			
			this.signature = key.getPrivateKey().sign(getObject());
		}
	}

	public final synchronized boolean verify(final K key) throws CryptoException
	{
		Objects.requireNonNull(key, "Public key is null");
		
		synchronized(this)
		{
			if (this.signature == null)
				throw new CryptoException("Signature is not present");
			
			if (getOwner() == null)
				return false;
	
			if (key.equals(getOwner()) == false)
				return false;
		}
		
		return key.verify(getObject(), this.signature);
	}
	
	boolean requiresSignature()
	{
		return true;
	}
	
	public final S getSignature()
	{
		synchronized(this)
		{
			return this.signature;
		}
	}
	
	final long getWeight()
	{
		synchronized(this)
		{
			if (this.weight == -1)
				throw new IllegalStateException("Vote weight has not been set");
			
			return this.weight;
		}
	}
	
	final void setWeight(final long weight)
	{
		Numbers.isNegative(weight, "Vote weight is negative");
		
		synchronized(this)
		{
			if (this.weight != -1)
				throw new IllegalStateException("Vote weight has already been set");

			this.weight = weight;
		}
	}

	@Override
	public String toString()
	{
		return super.toString()+" "+this.getObject()+" <- "+this.owner;
	}
}
