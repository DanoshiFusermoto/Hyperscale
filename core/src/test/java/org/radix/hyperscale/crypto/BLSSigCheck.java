package org.radix.hyperscale.crypto;

import java.util.ArrayList;
import java.util.List;

import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.crypto.Signature.VerificationResult;
import org.radix.hyperscale.crypto.bls12381.BLS12381;
import org.radix.hyperscale.crypto.bls12381.BLSPublicKey;
import org.radix.hyperscale.crypto.bls12381.BLSSignature;
import org.radix.hyperscale.utils.Base58;

public class BLSSigCheck
{
    public static void main(String[] args) throws Exception 
    {
    	String merkle = "e6ea104a1b8504d26b5762f32cbbf52dbd536cba6d0afc5b55b006811570f309";
    	Hash merkleHash = Hash.from(merkle);
    	
    	BLSPublicKey pubKey1 = BLSPublicKey.from("AVFU4uZZbZsumYpuyWExsBs8xnyTtd1fnFtZLSFkwGtcVEpvL5ex9cNtrA1op3zak23UeNHU9n5YVx1MBN46vhzcfhowH9dBwEH1FJkMJyjr6gvpEfPYoHwcithzhSPBCGMjZukdQaA27LhXkXPbhkLjczkYQPNGoUnkKBnhqoHvhFQr8MSTqFQ49WxHhKrHH6xVWGchYK7cufgxRmzF2eRd4PmfAYgYo2FKqxQXQeHGCL1svGk7sVQvx1PdAiVPmb6aUf");
    	BLSPublicKey pubKey2 = BLSPublicKey.from("6nbgKDVfBX27C6nyVmWh3V6fHxMuTDy1apw4avWRC48f8epqQvr3X21E81QQstQvGCYp8Kbr9TDb9okw5gFe5VvDuJhWwiq6px7awnNYsFL8oSaRzW8brCd9j6oWtXyZ6JCBxj1NmAVj86Engi3SALM18c8CHD4SA3W1rKfUS5gFNN6C29eLZk4jYNYDKXyvGAEvSJVjvEUdVhKm2vgj4FNhVkHCPfHuR692J7FAs3tK9QhZwbJzE7SxXxVCrR6pHHEBr5");
    	List<BLSPublicKey> pubkeys = new ArrayList<>();
    	pubkeys.add(pubKey1);
    	pubkeys.add(pubKey2);
    	BLSPublicKey aggPubCheck = BLS12381.aggregatePublicKey(pubkeys);
    	
    	BLSSignature sig1= BLSSignature.from(Base58.fromBase58("Ko6mgSEmhnPiqc4XqRETBJ63tLUXGK8y7ok36gVow31CyEwGRaZm6GySAvpi2EpfZW"));
    	BLSSignature sig2= BLSSignature.from(Base58.fromBase58("VzDoNSFJ7ozaHSbrHMr9KKt1ocQ9f56s1RMc99fY9q83WkUsAsjgmKGkrvPAeFd8vJ"));
    	List<BLSSignature> sigs = new ArrayList<>();
    	sigs.add(sig1);
    	sigs.add(sig2);
    	BLSSignature aggSigCheck = BLS12381.aggregateSignatures(sigs);

//    	Thread.sleep(10000);
    	int ITERATIONS = 10000;
        long start= System.nanoTime();
        for (int i = 0; i < ITERATIONS ; i++) 
        {
        	aggSigCheck.setVerified(VerificationResult.UNVERIFIED);
        	if (aggPubCheck.verify(merkleHash, aggSigCheck) == false)
        		System.out.println("FAILED");
        }
        long duration = System.nanoTime() - start;        
        System.out.printf("Verify: %.2f operations/sec%n", ITERATIONS / (duration / 1_000_000_000.0));
    }
}
