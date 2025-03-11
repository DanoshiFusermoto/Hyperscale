package org.radix.hyperscale.crypto.bls12381;

import java.util.List;
import java.util.Objects;

import org.miracl.core.BLS12381.BLS;
import org.radix.hyperscale.crypto.bls12381.group.AtePairing;
import org.radix.hyperscale.crypto.bls12381.group.G1Point;
import org.radix.hyperscale.crypto.bls12381.group.G2Point;
import org.radix.hyperscale.crypto.bls12381.group.GTPoint;
import org.radix.hyperscale.utils.Numbers;

/**
 * Forked from https://github.com/ConsenSys/mikuli/tree/master/src/main/java/net/consensys/mikuli/crypto
 * 
 * Modified for use with Cassandra as internal code not a dependency
 * 
 * Original repo source has no license headers.
 */
public final class BLS12381 
{
	public static final boolean SKIP_SIGNING = Boolean.getBoolean("BLS.skip_signing");
	public static final boolean SKIP_VERIFICATION = Boolean.getBoolean("BLS.skip_verification");
	
	static
	{
		if (SKIP_SIGNING)
			System.err.println("Warning: BLS signing is being skipped");
		if (SKIP_VERIFICATION)
			System.err.println("Warning: BLS verification is being skipped");
	}

	/**
	 * Generates a SignatureAndPublicKey.
	 *
	 * @param keyPair The public and private key pair, not null
	 * @param message The message to sign, not null
	 * @return The SignatureAndPublicKey, not null
	 */
	public static BLSSignature sign(final BLSPrivateKey privateKey, final byte[] message) 
	{
		Objects.requireNonNull(privateKey, "Private key is null");
		Objects.requireNonNull(message, "Message to sign is null");
		Numbers.isZero(message.length, "Message is empty");

		if (SKIP_SIGNING)
			return BLSSignature.NULL;

		G1Point hashInGroup1 = hashFunction(message);
		/*
		 * The signature is hash point in G1 multiplied by the private key.
		 */
		G1Point sig = privateKey.sign(hashInGroup1);
		return new BLSSignature(sig);
	}

	/**
	 * Verifies the given BLS signature against the message bytes using the public key.
	 * 
	 * @param publicKey The public key, not null
	 * @param signature The signature, not null
	 * @param message The message data to verify, not null
	 * 
	 * @return True if the verification is successful.
	 */
	public static boolean verify(final BLSPublicKey publicKey, final BLSSignature signature, final byte[] message) 
	{
		Objects.requireNonNull(publicKey, "Public key is null");
		Objects.requireNonNull(signature, "Signature is null");
		Objects.requireNonNull(message, "Message to verify is null");
		Numbers.isZero(message.length, "Message is empty");
		
		if (SKIP_VERIFICATION)
			return true;
		
		G1Point hashInGroup1 = hashFunction(message);
		G2Point g2GeneratorNeg = BLSCurveParameters.g2Generator().neg();
		GTPoint e = AtePairing.pair2(hashInGroup1, publicKey.g2Point(), signature.g1Point(), g2GeneratorNeg);
        return e.isunity();
	}

	/**
	 * Aggregates list of PublicKey pairs
	 * 
	 * @param publicKeyList The list of public keys to aggregate, not null
	 * @return PublicKey The public key, not null
	 * @throws IllegalArgumentException if parameter list is empty
	 */
	public static BLSPublicKey aggregatePublicKey(final List<BLSPublicKey> publicKeys) 
	{
		Objects.requireNonNull(publicKeys, "Public key list is null");
		Numbers.isZero(publicKeys.size(), "Public key list is empty");
		
		if (SKIP_VERIFICATION)
			return BLSPublicKey.NULL;
		
		if (publicKeys.size() == 1)
			return publicKeys.get(0);
		
		return publicKeys.get(0).combine(publicKeys, 1, publicKeys.size());
	}

	/**
	 * Aggregates list of Signature pairs
	 * 
	 * @param signatureList The list of signatures to aggregate, not null
	 * @throws IllegalArgumentException if parameter list is empty
	 * @return Signature, not null
	 */
	public static BLSSignature aggregateSignatures(final List<BLSSignature> signatures) 
	{
		Objects.requireNonNull(signatures, "Signatures is null");
		Numbers.isZero(signatures.size(), "Signatures is empty");
		
		if (SKIP_SIGNING)
			return BLSSignature.NULL;

		if (signatures.size() == 1)
			return signatures.get(0);
		
		return signatures.get(0).combine(signatures, 1, signatures.size());
	}

	private static G1Point hashFunction(byte[] message) 
	{
//		byte[] hashByte = MPIN.HASH_ID(ECP.SHA256, message, CONFIG_BIG.MODBYTES);
		return new G1Point(BLS.bls_hash_to_point(message));
	}
	
	private BLS12381() {} 
}
