package org.radix.hyperscale.serialization.mapper;

import java.io.IOException;

import org.radix.hyperscale.crypto.merkle.MerkleProof;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;

public class JacksonCborMerkleProofDeserializer extends StdDeserializer<MerkleProof> 
{
	private static final long serialVersionUID = -1555027261207368739L;

	JacksonCborMerkleProofDeserializer() 
	{
		this(null);
	}

	JacksonCborMerkleProofDeserializer(Class<MerkleProof> t) 
	{
		super(t);
	}

	@Override
	public MerkleProof deserialize(JsonParser p, DeserializationContext ctxt) throws IOException 
	{
		byte[] bytes = p.getBinaryValue();
		if (bytes == null || bytes.length == 0 || bytes[0] != JacksonCodecConstants.MERKLE_PROOF_VALUE)
			throw new InvalidFormatException(p, "Expecting " + JacksonCodecConstants.MERKLE_PROOF_VALUE, bytes[0], this.handledType());
		
		return MerkleProof.from(bytes, 1);
	}
}