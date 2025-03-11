package org.radix.hyperscale.network.messages;

import java.util.Objects;

import org.radix.hyperscale.crypto.CryptoException;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.crypto.ed25519.EDKeyPair;
import org.radix.hyperscale.crypto.ed25519.EDPublicKey;
import org.radix.hyperscale.crypto.ed25519.EDSignature;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.SerializerId2;
import org.radix.hyperscale.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

@SerializerId2("godix.kill")
public class KillMessage extends Message
{
	@JsonProperty("nonce")
	@DsonOutput(Output.ALL)
	private long nonce;

	@JsonProperty("bootstraps")
	@DsonOutput(Output.ALL)
	private boolean bootstraps;

	@JsonProperty("signature")
	@DsonOutput(value = {Output.API, Output.WIRE, Output.PERSIST})
	private EDSignature signature;
	
	@SuppressWarnings("unused")
	private KillMessage()
	{
		super();
	}

	public KillMessage(final boolean bootstraps, final EDKeyPair godixKey) throws CryptoException
	{
		super();

		Objects.requireNonNull(godixKey, "Godix key pair is null");
		
		this.bootstraps = bootstraps;
		
		Hash hash = getHash();
		this.signature = godixKey.getPrivateKey().sign(hash);
	}
	
	public boolean includeBoostraps()
	{
		return this.bootstraps;
	}
	
	public boolean verify(final EDPublicKey godixPublicKey) throws CryptoException
	{
		Objects.requireNonNull(godixPublicKey, "Godix public key is null");
		
		Hash hash = getHash();
		return godixPublicKey.verify(hash, this.signature);
	}
}
