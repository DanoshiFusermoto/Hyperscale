package org.radix.hyperscale.crypto.ed25519;

import static org.junit.Assert.*;

import java.util.Arrays;

import org.junit.Test;
import org.miracl.core.RAND;
import org.radix.hyperscale.crypto.CryptoException;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.crypto.Signature.VerificationResult;
import org.radix.hyperscale.crypto.ed25519.ED25519;
import org.radix.hyperscale.crypto.ed25519.EDKeyPair;
import org.radix.hyperscale.crypto.ed25519.EDSignature;
import org.radix.hyperscale.utils.Bytes;

/**
 * Tests for the ED25519 signature scheme implementation.
 */
public class ED25519Test 
{
    
    // Known test vectors for ED25519 from RFC 8032
    // Format: {private key, public key, message, signature}
    // Source: https://datatracker.ietf.org/doc/html/rfc8032#section-7.1
    private static final String[][] TEST_VECTORS = 
    {
        // Test 1
        {
            "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60",
            "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a",
            "",
            "e5564300c360ac729086e2cc806e828a84877f1eb8e5d974d873e06522490155" +
            "5fb8821590a33bacc61e39701cf9b46bd25bf5f0595bbe24655141438e7a100b"
        },
        // Test 2
        {
            "4ccd089b28ff96da9db6c346ec114e0f5b8a319f35aba624da8cf6ed4fb8a6fb",
            "3d4017c3e843895a92b70aa74d1b7ebc9c982ccf2ec4968cc0cd55f12af4660c",
            "72",
            "92a009a9f0d4cab8720e820b5f642540a2b27b5416503f8fb3762223ebdb69da" +
            "085ac1e43e15996e458f3613d0f11d8c387b2eaeb4302aeeb00d291612bb0c00"
        },
        // Test 3
        {
            "c5aa8df43f9f837bedb7442f31dcb7b166d38535076f094b85ce3a2e0b4458f7",
            "fc51cd8e6218a1a38da47ed00230f0580816ed13ba3303ac5deb911548908025",
            "af82",
            "6291d657deec24024827e69c3abe01a30ce548a284743a445e3680d7db5ac3ac" +
            "18ff9b538d16f290ae67f760984dc6594a7c15e9716ed28dc027beceea1ec40a"
        }
    };
    
    /**
     * Tests key pair generation.
     */
    @Test
    public void testKeyPairGeneration() throws CryptoException 
    {
        // Generate a key pair
        EDKeyPair keyPair = new EDKeyPair();
        
        // Check that the keys are not null
        assertNotNull("Private key should not be null", keyPair.getPrivateKey());
        assertNotNull("Public key should not be null", keyPair.getPublicKey());
        
        // Check that the keys have the correct length
        assertEquals("Private key should be " + ED25519.PRIVATE_KEY_SIZE + " bytes", 
                ED25519.PRIVATE_KEY_SIZE, keyPair.getPrivateKey().toByteArray().length);
        assertEquals("Public key should be " + ED25519.PUBLIC_KEY_SIZE + " bytes", 
                ED25519.PUBLIC_KEY_SIZE, keyPair.getPublicKey().toByteArray().length);
        
        // Check that the public key can be derived from the private key
        byte[] derivedPublicKey = ED25519.derivePublicKey(keyPair.getPrivateKey().toByteArray());
        assertArrayEquals("Derived public key should match the one from the key pair", 
                keyPair.getPublicKey().toByteArray(), derivedPublicKey);
    }
    
    /**
     * Tests the sign and verify operations.
     */
    @Test
    public void testSignAndVerify() throws CryptoException 
    {
        // Generate a key pair
        EDKeyPair keyPair = new EDKeyPair();
        
        // Create a test message
        Hash digest = Hash.hash("Hello, ED25519!");
        
        // Sign the message
        EDSignature signature = keyPair.getPrivateKey().sign(digest.toByteArray());
        signature.setVerified(VerificationResult.UNVERIFIED);
        
        // Verify the signature
        boolean result = keyPair.getPublicKey().verify(digest.toByteArray(), signature);
        signature.setVerified(VerificationResult.UNVERIFIED);
        
        // Check that the verification succeeded
        assertTrue("Signature verification should succeed", result);
        
        // Modify the message
        Hash tamperedDigest = Hash.hash("Hello, ED25519?");
        
        // Verify the signature with the tampered message
        result = keyPair.getPublicKey().verify(tamperedDigest.toByteArray(), signature);
        
        // Check that the verification failed
        assertFalse("Signature verification should fail with tampered message", result);
    }
    
    /**
     * Tests the known test vectors from RFC 8032.
     */
    @Test
    public void testVectors() throws CryptoException 
    {
        for (int i = 0; i < TEST_VECTORS.length; i++) 
        {
            String[] vector = TEST_VECTORS[i];
            byte[] privateKey = Bytes.fromHexString(vector[0]);
            byte[] publicKey = Bytes.fromHexString(vector[1]);
            byte[] message = Bytes.fromHexString(vector[2]);
            byte[] signature = Bytes.fromHexString(vector[3]);
            
            // Test 1: Derive public key from private key
            byte[] derivedPublicKey = ED25519.derivePublicKey(privateKey);
            assertArrayEquals("Test vector " + (i + 1) + ": Derived public key should match expected public key", publicKey, derivedPublicKey);
            
            // Test 2: Sign message with private key
            byte[] generatedSignature = ED25519.sign(privateKey, message);
            assertArrayEquals("Test vector " + (i + 1) + ": Generated signature should match expected signature", signature, generatedSignature);
            
            // Test 3: Verify signature with public key
            boolean verifyResult = ED25519.verify(publicKey, message, signature);
            assertTrue("Test vector " + (i + 1) + ": Signature verification should succeed", verifyResult);
        }
    }
    
    /**
     * Tests a large number of random messages to ensure consistency.
     */
    @Test
    public void testRandomMessages() throws CryptoException 
    {
        // Generate a key pair
        EDKeyPair keyPair = new EDKeyPair();
        
        // Create a random number generator
        RAND rng = new RAND();
        byte[] seed = new byte[128];
        for (int i = 0; i < seed.length; i++)
            seed[i] = (byte)i;
        rng.seed(seed.length, seed);
        
        // Test with 100 random messages
        for (int i = 0; i < 100; i++) 
        {
            // Generate a random message
            byte[] message = new byte[64];
            rng.seed(64, message);
            Hash digest = Hash.hash(message);

            // Sign the message
            EDSignature signature = keyPair.getPrivateKey().sign(digest);
            signature.setVerified(VerificationResult.UNVERIFIED);

            // Verify the signature
            boolean result = keyPair.getPublicKey().verify(digest, signature);
            signature.setVerified(VerificationResult.UNVERIFIED);
            
            // Check that the verification succeeded
            assertTrue("Signature verification should succeed for random message " + i, result);
            
            // Modify the message
            byte[] tamperedMessage = Arrays.copyOf(message, message.length);
            tamperedMessage[0] ^= 0x01; // Flip a bit
            Hash tamperedDigest = Hash.hash(tamperedMessage);
            
            // Verify the signature with the tampered message
            result = keyPair.getPublicKey().verify(tamperedDigest, signature);
            
            // Check that the verification failed
            assertFalse("Signature verification should fail for tampered random message " + i, result);
        }
    }
    
    /**
     * Tests the pre-hashed signing and verification.
     */
    @Test
    public void testPrehashedSigning() throws CryptoException 
    {
        // Generate a key pair
        EDKeyPair keyPair = new EDKeyPair();
        
        // Create a test message and its hash
        byte[] message = "Hello, ED25519!".getBytes();
        byte[] messageHash = sha512(message);
        
        // Sign the pre-hashed message
        byte[] signature = ED25519.signPrehashed(keyPair.getPrivateKey().toByteArray(), messageHash);
        
        // Verify the signature with the pre-hashed message
        boolean result = ED25519.verifyPrehashed(keyPair.getPublicKey().toByteArray(), messageHash, signature);
        
        // Check that the verification succeeded
        assertTrue("Signature verification with pre-hashed message should succeed", result);
    }
    
    /**
     * Helper method to compute SHA-512 hash.
     */
    private byte[] sha512(byte[] input) 
    {
        org.miracl.core.HASH512 hash = new org.miracl.core.HASH512();
        for (byte b : input)
            hash.process(b);
        return hash.hash();
    }
}