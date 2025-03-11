package org.radix.hyperscale.crypto.ed25519;

import org.miracl.core.RAND;
import org.radix.hyperscale.crypto.CryptoException;
import org.radix.hyperscale.crypto.Signature.VerificationResult;
import org.radix.hyperscale.crypto.ed25519.EDKeyPair;
import org.radix.hyperscale.crypto.ed25519.EDSignature;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * A benchmark for ED25519 operations using MIRACL Core.
 */
public class ED25519Benchmark {
    
    // Number of messages to process
    private static final int NUM_MESSAGES = 10_000;
    
    // Number of key pairs to generate
    private static final int NUM_KEYS = 100;
    
    // Message size in bytes
    private static final int MESSAGE_SIZE = 64;
    
    public static void main(String[] args) throws CryptoException {
        System.out.println("ED25519 Benchmark using MIRACL Core");
        System.out.println("==================================");
        System.out.println("Number of key pairs: " + NUM_KEYS);
        System.out.println("Number of messages: " + NUM_MESSAGES);
        System.out.println("Message size: " + MESSAGE_SIZE + " bytes");
        System.out.println();
        
        LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(10));
        
        RAND rng = initializeRng();
        
        // Generate key pairs
        long startKeyGen = System.nanoTime();
        List<EDKeyPair> keyPairs = generateKeyPairs(rng, NUM_KEYS);
        long endKeyGen = System.nanoTime();
        
        // Generate messages
        List<byte[]> messages = generateMessages(rng, NUM_MESSAGES, MESSAGE_SIZE);
        
        // Perform signing benchmark
        long startSigning = System.nanoTime();
        List<EDSignature> signatures = signMessages(messages, keyPairs);
        long endSigning = System.nanoTime();
        
        // Perform verification benchmark
        long startVerification = System.nanoTime();
        boolean allVerified = verifySignatures(messages, signatures, keyPairs);
        long endVerification = System.nanoTime();
        
        // Print results
        double keyGenTimeMs = TimeUnit.NANOSECONDS.toMillis(endKeyGen - startKeyGen);
        double signingTimeMs = TimeUnit.NANOSECONDS.toMillis(endSigning - startSigning);
        double verificationTimeMs = TimeUnit.NANOSECONDS.toMillis(endVerification - startVerification);
        
        System.out.println("Results:");
        System.out.println("---------");
        System.out.printf("Key Generation: %.2f ms total, %.2f ms per key%n", 
                keyGenTimeMs, keyGenTimeMs / NUM_KEYS);
        
        System.out.printf("Signing: %.2f ms total, %.2f µs per message%n", 
                signingTimeMs, signingTimeMs * 1000 / NUM_MESSAGES);
        
        System.out.printf("Verification: %.2f ms total, %.2f µs per message%n", 
                verificationTimeMs, verificationTimeMs * 1000 / NUM_MESSAGES);
        
        System.out.println("All signatures verified: " + allVerified);
        
        // Calculate operations per second
        double signingsPerSecond = NUM_MESSAGES / (signingTimeMs / 1000);
        double verificationsPerSecond = NUM_MESSAGES / (verificationTimeMs / 1000);
        
        System.out.println();
        System.out.println("Performance:");
        System.out.println("-----------");
        System.out.printf("Signings per second: %.0f%n", signingsPerSecond);
        System.out.printf("Verifications per second: %.0f%n", verificationsPerSecond);
    }
    
    /**
     * Initialize the random number generator.
     */
    private static RAND initializeRng() {
        RAND rng = new RAND();
        byte[] seed = new byte[128];
        // Use a deterministic seed for reproducible benchmarks
        for (int i = 0; i < seed.length; i++) {
            seed[i] = (byte)i;
        }
        rng.seed(seed.length, seed);
        return rng;
    }
    
    /**
     * Generate multiple key pairs.
     * @throws CryptoException 
     */
    private static List<EDKeyPair> generateKeyPairs(RAND rng, int count) throws CryptoException {
        List<EDKeyPair> keyPairs = new ArrayList<>(count);
        
        for (int i = 0; i < count; i++) {
            keyPairs.add(new EDKeyPair());
        }
        
        return keyPairs;
    }
    
    /**
     * Generate random messages for benchmarking.
     */
    private static List<byte[]> generateMessages(RAND rng, int count, int size) {
        List<byte[]> messages = new ArrayList<>(count);
        
        for (int i = 0; i < count; i++) {
            byte[] message = new byte[size];
            rng.seed(size, message);
            messages.add(message);
        }
        
        return messages;
    }
    
    /**
     * Sign a list of messages.
     * @throws CryptoException 
     */
    private static List<EDSignature> signMessages(List<byte[]> messages, List<EDKeyPair> keyPairs) throws CryptoException {
        List<EDSignature> signatures = new ArrayList<>(messages.size());
        
        for (int i = 0; i < messages.size(); i++) {
            // Select a key pair based on message index, cycling through the available key pairs
            EDKeyPair keyPair = keyPairs.get(i % keyPairs.size());
            EDSignature signature = keyPair.sign(messages.get(i));
            signature.setVerified(VerificationResult.UNVERIFIED);
            signatures.add(signature);
        }
        
        return signatures;
    }
    
    /**
     * Verify a list of signatures.
     * @throws CryptoException 
     */
    private static boolean verifySignatures(List<byte[]> messages, List<EDSignature> signatures, List<EDKeyPair> keyPairs) throws CryptoException 
    {
        boolean allValid = true;
        
        for (int i = 0; i < messages.size(); i++) {
            EDKeyPair keyPair = keyPairs.get(i % keyPairs.size());
            boolean valid = keyPair.getPublicKey().verify(messages.get(i), signatures.get(i));
            if (!valid) {
                allValid = false;
                System.err.println("Verification failed for message " + i);
                break;
            }
        }
        
        return allValid;
    }
}