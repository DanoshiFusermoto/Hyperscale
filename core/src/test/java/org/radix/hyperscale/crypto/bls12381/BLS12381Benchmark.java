package org.radix.hyperscale.crypto.bls12381;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.radix.hyperscale.crypto.CryptoException;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.crypto.Signature.VerificationResult;

/**
 * A benchmark for BLS12381 operations.
 */
public class BLS12381Benchmark {
    
    // Number of messages to process
    private static final int NUM_MESSAGES = 10_000;
    
    // Number of key pairs to generate
    private static final int NUM_KEYS = 100;
    
    // Message size in bytes
    private static final int MESSAGE_SIZE = 64;
    
    // Number of keys to aggregate
    private static final int AGGREGATE_KEYS = 10;
    
    public static void main(String[] args) 
    {
        System.out.println("BLS12381 Benchmark");
        System.out.println("=================");
        System.out.println("Number of key pairs: " + NUM_KEYS);
        System.out.println("Number of messages: " + NUM_MESSAGES);
        System.out.println("Message size: " + MESSAGE_SIZE + " bytes");
        System.out.println("Number of keys/signatures to aggregate: " + AGGREGATE_KEYS);
        System.out.println();
        
        try 
        {
        	Thread.sleep(10000);
        	
            // Generate key pairs
            long startKeyGen = System.nanoTime();
            List<BLSKeyPair> keyPairs = generateKeyPairs(NUM_KEYS);
            long endKeyGen = System.nanoTime();
            
            // Extract public keys
            List<BLSPublicKey> publicKeys = new ArrayList<>();
            for (BLSKeyPair keyPair : keyPairs)
                publicKeys.add(keyPair.getPublicKey());
            
            // Generate messages
            List<byte[]> digests = generateDigests(NUM_MESSAGES, MESSAGE_SIZE);
            
            // Perform signing benchmark
            long startSigning = System.nanoTime();
            List<BLSSignature> signatures = signMessages(digests, keyPairs);
            long endSigning = System.nanoTime();
            
            // Perform verification benchmark
            long startVerification = System.nanoTime();
            boolean allVerified = verifySignatures(digests, signatures, keyPairs);
            long endVerification = System.nanoTime();
            
            // Perform aggregation benchmark
            long startAggregationBenchmark = System.nanoTime();
            boolean allAggVerified = runAggregationBenchmark(keyPairs, NUM_MESSAGES / AGGREGATE_KEYS);
            long endAggregationBenchmark = System.nanoTime();
            
            // Print results
            double keyGenTimeMs = TimeUnit.NANOSECONDS.toMillis(endKeyGen - startKeyGen);
            double signingTimeMs = TimeUnit.NANOSECONDS.toMillis(endSigning - startSigning);
            double verificationTimeMs = TimeUnit.NANOSECONDS.toMillis(endVerification - startVerification);
            double aggregationTimeMs = TimeUnit.NANOSECONDS.toMillis(endAggregationBenchmark - startAggregationBenchmark);
            
            System.out.println("Results:");
            System.out.println("---------");
            System.out.printf("Key Generation: %.2f ms total, %.2f ms per key%n", keyGenTimeMs, keyGenTimeMs / NUM_KEYS);
            
            System.out.printf("Signing: %.2f ms total, %.2f µs per message%n", signingTimeMs, signingTimeMs * 1000 / NUM_MESSAGES);
            
            System.out.printf("Verification: %.2f ms total, %.2f µs per message%n", verificationTimeMs, verificationTimeMs * 1000 / NUM_MESSAGES);
            
            System.out.printf("Aggregation (signing, aggregating, verifying): %.2f ms total, %.2f ms per batch%n", aggregationTimeMs, aggregationTimeMs / (NUM_MESSAGES / AGGREGATE_KEYS));
            
            System.out.println("All individual signatures verified: " + allVerified);
            System.out.println("All aggregated signatures verified: " + allAggVerified);
            
            // Calculate operations per second
            double signingsPerSecond = NUM_MESSAGES / (signingTimeMs / 1000);
            double verificationsPerSecond = NUM_MESSAGES / (verificationTimeMs / 1000);
            double aggregationsPerSecond = (NUM_MESSAGES / AGGREGATE_KEYS) / (aggregationTimeMs / 1000);
            
            System.out.println();
            System.out.println("Performance:");
            System.out.println("-----------");
            System.out.printf("Signings per second: %.0f%n", signingsPerSecond);
            System.out.printf("Verifications per second: %.0f%n", verificationsPerSecond);
            System.out.printf("Aggregated operations per second: %.0f%n", aggregationsPerSecond);
            
        } 
        catch (Exception e)
        {
            System.err.println("Error during benchmark: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * Generate multiple key pairs.
     */
    private static List<BLSKeyPair> generateKeyPairs(int count) 
    {
        List<BLSKeyPair> keyPairs = new ArrayList<>(count);
        for (int i = 0; i < count; i++)
            keyPairs.add(new BLSKeyPair());
        
        return keyPairs;
    }
    
    /**
     * Generate random messages for benchmarking.
     */
    private static List<byte[]> generateDigests(int count, int size) 
    {
        List<byte[]> digests = new ArrayList<>(count);
        for (int i = 0; i < count; i++) 
        {
            byte[] message = new byte[size];
            // Fill with pseudo-random data based on i
            for (int j = 0; j < size; j++)
                message[j] = (byte)((i * j) % 256);

            digests.add(Hash.hash(message).toByteArray());
        }
        
        return digests;
    }
    
    /**
     * Sign a list of messages.
     */
    private static List<BLSSignature> signMessages(List<byte[]> digests, List<BLSKeyPair> keyPairs) throws CryptoException 
    {
        List<BLSSignature> signatures = new ArrayList<>(digests.size());
        for (int i = 0; i < digests.size(); i++) 
        {
            // Select a key pair based on message index, cycling through the available key pairs
            BLSKeyPair keyPair = keyPairs.get(i % keyPairs.size());
            BLSSignature signature = keyPair.getPrivateKey().sign(digests.get(i));
            signature.setVerified(VerificationResult.UNVERIFIED);
            signatures.add(signature);
        }
        
        return signatures;
    }
    
    /**
     * Verify a list of signatures.
     */
    private static boolean verifySignatures(List<byte[]> digests, List<BLSSignature> signatures, List<BLSKeyPair> keyPairs) 
    {
        boolean allValid = true;
        
        for (int i = 0; i < digests.size(); i++) 
        {
            BLSKeyPair keyPair = keyPairs.get(i % keyPairs.size());
            boolean valid = keyPair.getPublicKey().verify(digests.get(i), signatures.get(i));
            if (valid == false) 
            {
                allValid = false;
                System.err.println("Verification failed for message " + i);
                break;
            }
        }
        
        return allValid;
    }
    
    private static boolean runAggregationBenchmark(List<BLSKeyPair> keyPairs, int numBatches) throws CryptoException 
    {
        boolean allValid = true;
        
        for (int batchIdx = 0; batchIdx < numBatches; batchIdx++) 
        {
            // Generate a unique message for this batch
            byte[] message = new byte[MESSAGE_SIZE];
            for (int j = 0; j < MESSAGE_SIZE; j++)
                message[j] = (byte)((batchIdx * j) % 256);
            Hash digest = Hash.hash(message);
            
            // Have multiple keys sign the same message
            List<BLSSignature> batchSignatures = new ArrayList<>();
            List<BLSPublicKey> batchPublicKeys = new ArrayList<>();
            
            for (int i = 0; i < AGGREGATE_KEYS; i++) {
                int keyIdx = (batchIdx * AGGREGATE_KEYS + i) % keyPairs.size();
                BLSKeyPair keyPair = keyPairs.get(keyIdx);
                
                // Each key signs the same batch message
                BLSSignature signature = keyPair.getPrivateKey().sign(digest);
                batchSignatures.add(signature);
                batchPublicKeys.add(keyPair.getPublicKey());
            }
            
            // Aggregate the signatures and public keys
            BLSSignature aggregatedSignature = BLS12381.aggregateSignatures(batchSignatures);
            BLSPublicKey aggregatedPublicKey = BLS12381.aggregatePublicKey(batchPublicKeys);
            
            // Verify the aggregated signature
            boolean valid = aggregatedPublicKey.verify(digest, aggregatedSignature);
            if (valid == false) 
            {
                allValid = false;
                System.err.println("Aggregated verification failed for batch " + batchIdx);
                break;
            }
        }
        
        return allValid;
    }
}