package org.radix.hyperscale.crypto.bls12381;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import org.radix.hyperscale.crypto.CryptoException;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.crypto.Signature.VerificationResult;

/** Tests for the BLS12381 signature scheme implementation. */
public class BLS12381Test {
  /** Tests key pair generation. */
  @Test
  public void testKeyPairGeneration() throws CryptoException {
    // Generate a key pair
    BLSKeyPair keyPair = new BLSKeyPair();

    // Check that the keys are not null
    assertNotNull("Private key should not be null", keyPair.getPrivateKey());
    assertNotNull("Public key should not be null", keyPair.getPublicKey());

    // Verify that the derived public key is valid
    BLSPrivateKey privateKey = keyPair.getPrivateKey();
    BLSPublicKey publicKey = keyPair.getPublicKey();

    // Check the key pair association
    String message = "Hello, BLS12381!";
    Hash digest = Hash.hash(message);
    BLSSignature signature = privateKey.sign(digest);
    signature.setVerified(VerificationResult.UNVERIFIED);

    assertTrue(
        "Signature verification should succeed with the corresponding public key",
        publicKey.verify(digest, signature));
  }

  /** Tests signing and verification. */
  @Test
  public void testSignAndVerify() throws CryptoException {
    // Generate a key pair
    BLSKeyPair keyPair = new BLSKeyPair();

    // Sign a message
    String message = "Hello, BLS12381!";
    Hash digest = Hash.hash(message);
    BLSSignature signature = keyPair.getPrivateKey().sign(digest);
    signature.setVerified(VerificationResult.UNVERIFIED);

    // Verify the signature
    boolean result = keyPair.getPublicKey().verify(digest, signature);
    assertTrue("Signature verification should succeed", result);
    signature.setVerified(VerificationResult.UNVERIFIED);

    // Verify with a different message
    String differentMessage = "Different message";
    Hash differentDigest = Hash.hash(differentMessage);
    result = keyPair.getPublicKey().verify(differentDigest, signature);
    assertFalse("Signature verification should fail with a different message", result);
    signature.setVerified(VerificationResult.UNVERIFIED);

    // Generate another key pair
    BLSKeyPair anotherKeyPair = new BLSKeyPair();

    // Verify with a different public key
    result = anotherKeyPair.getPublicKey().verify(digest, signature);
    assertFalse("Signature verification should fail with a different public key", result);
  }

  /** Tests signature aggregation. */
  @Test
  public void testSignatureAggregation() throws CryptoException {
    // Generate multiple key pairs
    int numKeys = 5;
    List<BLSKeyPair> keyPairs = new ArrayList<>();
    List<BLSPublicKey> publicKeys = new ArrayList<>();
    List<BLSSignature> signatures = new ArrayList<>();

    for (int i = 0; i < numKeys; i++) {
      keyPairs.add(new BLSKeyPair());
      publicKeys.add(keyPairs.get(i).getPublicKey());
    }

    // Sign the same message with all private keys
    String message = "Message to be signed by multiple keys";
    Hash digest = Hash.hash(message);

    for (int i = 0; i < numKeys; i++) {
      BLSSignature signature = keyPairs.get(i).getPrivateKey().sign(digest);
      signature.setVerified(VerificationResult.UNVERIFIED);
      signatures.add(signature);
    }

    // Aggregate the signatures
    BLSSignature aggregatedSignature = BLS12381.aggregateSignatures(signatures);

    // Aggregate the public keys
    BLSPublicKey aggregatedPublicKey = BLS12381.aggregatePublicKey(publicKeys);

    // Verify the aggregated signature with the aggregated public key
    boolean result = aggregatedPublicKey.verify(digest, aggregatedSignature);
    assertTrue(
        "Aggregated signature verification should succeed with aggregated public key", result);
    aggregatedSignature.setVerified(VerificationResult.UNVERIFIED);

    // Verify individual signatures with the corresponding public keys
    for (int i = 0; i < numKeys; i++) {
      BLSSignature signature = signatures.get(i);
      result = publicKeys.get(i).verify(digest, signature);
      assertTrue("Individual signature verification should succeed", result);
      signature.setVerified(VerificationResult.UNVERIFIED);
    }
  }

  /** Tests aggregation of signatures from different messages. */
  @Test
  public void testDifferentMessageAggregation() throws CryptoException {
    // Generate multiple key pairs
    int numKeys = 3;
    List<BLSKeyPair> keyPairs = new ArrayList<>();
    List<BLSPublicKey> publicKeys = new ArrayList<>();
    List<BLSSignature> signatures = new ArrayList<>();

    for (int i = 0; i < numKeys; i++) {
      keyPairs.add(new BLSKeyPair());
      publicKeys.add(keyPairs.get(i).getPublicKey());
    }

    // Sign different messages with different keys
    String[] messages = new String[numKeys];
    for (int i = 0; i < numKeys; i++) {
      messages[i] = "Message " + i;
      Hash digest = Hash.hash(messages[i]);
      BLSSignature signature = keyPairs.get(i).getPrivateKey().sign(digest);
      signature.setVerified(VerificationResult.UNVERIFIED);
      signatures.add(signature);
    }

    // Aggregate the signatures
    BLSSignature aggregatedSignature = BLS12381.aggregateSignatures(signatures);

    // Note: For different messages, we cannot simply verify with the aggregated public key
    // The test here shows that individual verifications still work
    for (int i = 0; i < numKeys; i++) {
      Hash digest = Hash.hash(messages[i]);
      BLSSignature signature = signatures.get(i);
      boolean result = publicKeys.get(i).verify(digest, signature);
      assertTrue("Individual signature verification should succeed", result);
      signature.setVerified(VerificationResult.UNVERIFIED);
    }
  }

  /** Tests signature combination methods. */
  @Test
  public void testSignatureCombination() throws CryptoException {
    // Generate key pairs
    BLSKeyPair keyPair1 = new BLSKeyPair();
    BLSKeyPair keyPair2 = new BLSKeyPair();

    // Sign the same message
    String message = "Test message for combination";
    Hash digest = Hash.hash(message);
    BLSSignature signature1 = keyPair1.getPrivateKey().sign(digest);
    signature1.setVerified(VerificationResult.UNVERIFIED);
    BLSSignature signature2 = keyPair2.getPrivateKey().sign(digest);
    signature2.setVerified(VerificationResult.UNVERIFIED);

    // Combine signatures using the combine method
    BLSSignature combinedSignature = signature1.combine(signature2);

    // Create a list and use the list-based combine method
    List<BLSSignature> signatureList = new ArrayList<>();
    signatureList.add(signature1);
    signatureList.add(signature2);

    BLSSignature aggregatedSignature = BLS12381.aggregateSignatures(signatureList);

    // The combined signature should be the same as the aggregated signature
    assertArrayEquals(
        "Combined signature should be equal to aggregated signature",
        combinedSignature.toByteArray(),
        aggregatedSignature.toByteArray());

    // Combine public keys
    BLSPublicKey publicKey1 = keyPair1.getPublicKey();
    BLSPublicKey publicKey2 = keyPair2.getPublicKey();

    BLSPublicKey combinedPublicKey = publicKey1.combine(publicKey2);

    // Create a list and use the list-based combine method
    List<BLSPublicKey> publicKeyList = new ArrayList<>();
    publicKeyList.add(publicKey1);
    publicKeyList.add(publicKey2);

    BLSPublicKey aggregatedPublicKey = BLS12381.aggregatePublicKey(publicKeyList);

    // The combined public key should be the same as the aggregated public key
    assertArrayEquals(
        "Combined public key should be equal to aggregated public key",
        combinedPublicKey.toByteArray(),
        aggregatedPublicKey.toByteArray());

    // Verify with the combined public key
    boolean result = combinedPublicKey.verify(digest, combinedSignature);
    assertTrue("Verification with combined keys should succeed", result);
  }

  /** Tests signature reduction. */
  @Test
  public void testSignatureReduction() throws CryptoException {
    // Generate key pairs
    BLSKeyPair keyPair1 = new BLSKeyPair();
    BLSKeyPair keyPair2 = new BLSKeyPair();

    // Sign the same message
    String message = "Test message for reduction";
    Hash digest = Hash.hash(message);
    BLSSignature signature1 = keyPair1.getPrivateKey().sign(digest);
    signature1.setVerified(VerificationResult.UNVERIFIED);
    BLSSignature signature2 = keyPair2.getPrivateKey().sign(digest);
    signature2.setVerified(VerificationResult.UNVERIFIED);

    // Combine signatures
    BLSSignature combinedSignature = signature1.combine(signature2);

    // Reduce the combined signature by removing signature2
    BLSSignature reducedSignature = combinedSignature.reduce(signature2);

    // The reduced signature should be equal to signature1
    assertArrayEquals(
        "Reduced signature should be equal to signature1",
        reducedSignature.toByteArray(),
        signature1.toByteArray());
  }

  /** Tests serialization/deserialization of keys and signatures. */
  @Test
  public void testSerialization() throws CryptoException {
    // Generate a key pair
    BLSKeyPair keyPair = new BLSKeyPair();

    // Get byte representations
    byte[] privateKeyBytes = keyPair.getPrivateKey().toByteArray();
    byte[] publicKeyBytes = keyPair.getPublicKey().toByteArray();

    // Recreate from bytes
    BLSPrivateKey recreatedPrivateKey = BLSPrivateKey.from(privateKeyBytes);
    BLSPublicKey recreatedPublicKey = BLSPublicKey.from(publicKeyBytes);

    // Sign a message with both original and recreated private keys
    String message = "Test serialization";
    Hash digest = Hash.hash(message);
    BLSSignature originalSignature = keyPair.getPrivateKey().sign(digest);
    BLSSignature recreatedSignature = recreatedPrivateKey.sign(digest);

    // Get byte representations of signatures
    byte[] originalSignatureBytes = originalSignature.toByteArray();
    byte[] recreatedSignatureBytes = recreatedSignature.toByteArray();

    // Recreate signatures from bytes
    BLSSignature deserializedOriginalSignature = BLSSignature.from(originalSignatureBytes);
    BLSSignature deserializedRecreatedSignature = BLSSignature.from(recreatedSignatureBytes);

    // Verify all permutations
    assertTrue(
        "Original public key should verify original signature",
        keyPair.getPublicKey().verify(digest, originalSignature));

    assertTrue(
        "Recreated public key should verify original signature",
        recreatedPublicKey.verify(digest, originalSignature));

    assertTrue(
        "Original public key should verify recreated signature",
        keyPair.getPublicKey().verify(digest, recreatedSignature));

    assertTrue(
        "Recreated public key should verify recreated signature",
        recreatedPublicKey.verify(digest, recreatedSignature));

    assertTrue(
        "Original public key should verify deserialized original signature",
        keyPair.getPublicKey().verify(digest, deserializedOriginalSignature));

    assertTrue(
        "Original public key should verify deserialized recreated signature",
        keyPair.getPublicKey().verify(digest, deserializedRecreatedSignature));
  }
}
