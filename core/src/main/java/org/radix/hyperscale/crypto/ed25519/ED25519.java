package org.radix.hyperscale.crypto.ed25519;

import java.security.SecureRandom;
import org.miracl.core.ED25519.EDDSA;
import org.miracl.core.RAND;

/**
 * A simplified wrapper for ED25519 signature operations using MIRACL Core. Provides easy-to-use
 * methods for key generation, signing, and verification.
 */
public class ED25519 {
  // TODO MEH! //
  private static SecureRandom secRand = new SecureRandom();
  private static RAND rand = new RAND();

  static {
    byte[] secRandSeed = secRand.generateSeed(128);
    rand.seed(secRandSeed.length, secRandSeed);
  }

  // Key and signature sizes in bytes
  public static final int PUBLIC_KEY_SIZE = 32;
  public static final int PRIVATE_KEY_SIZE = 32;
  public static final int SIGNATURE_SIZE = 64;

  /**
   * Generates an ED25519 key pair using the pre-configured RAND.
   *
   * @return A KeyPair containing the private and public keys
   */
  public static byte[][] generateKeyPair() {
    return generateKeyPair(rand);
  }

  /**
   * Generates an ED25519 key pair.
   *
   * @param rng Secure random number generator
   * @return A KeyPair containing the private and public keys
   */
  public static byte[][] generateKeyPair(RAND rng) {
    byte[][] keyPair = new byte[2][];
    keyPair[0] = new byte[PRIVATE_KEY_SIZE];
    keyPair[1] = new byte[PUBLIC_KEY_SIZE];

    EDDSA.KEY_PAIR_GENERATE(rng, keyPair[0], keyPair[1]);

    return keyPair;
  }

  /**
   * Signs a message using ED25519.
   *
   * @param privateKey The private key (32 bytes)
   * @param message The message to sign
   * @return The 64-byte signature
   */
  public static byte[] sign(byte[] privateKey, byte[] message) {
    byte[] signature = new byte[SIGNATURE_SIZE];

    // Sign without prehashing and with no context
    EDDSA.SIGNATURE(false, privateKey, null, message, signature);

    return signature;
  }

  /**
   * Signs a message using ED25519.
   *
   * @param privateKey The private key (32 bytes)
   * @param publicKey The private key (32 bytes)
   * @param message The message to sign
   * @return The 64-byte signature
   */
  public static byte[] sign(byte[] privateKey, byte[] publicKey, byte[] message) {
    byte[] signature = new byte[SIGNATURE_SIZE];

    // Sign without prehashing and with no context
    EDDSA.SIGNATURE(false, privateKey, publicKey, null, message, signature);

    return signature;
  }

  /**
   * Signs a pre-hashed message using ED25519.
   *
   * @param privateKey The private key (32 bytes)
   * @param prehashed The pre-hashed message
   * @return The 64-byte signature
   */
  public static byte[] signPrehashed(byte[] privateKey, byte[] prehashed) {
    byte[] signature = new byte[SIGNATURE_SIZE];

    // Sign with prehashing and no context
    EDDSA.SIGNATURE(true, privateKey, null, prehashed, signature);

    return signature;
  }

  /**
   * Signs a pre-hashed message using ED25519.
   *
   * @param privateKey The private key (32 bytes)
   * @param publicKey The private key (32 bytes)
   * @param prehashed The pre-hashed message
   * @return The 64-byte signature
   */
  public static byte[] signPrehashed(byte[] privateKey, byte[] publicKey, byte[] prehashed) {
    byte[] signature = new byte[SIGNATURE_SIZE];

    // Sign with prehashing and no context
    EDDSA.SIGNATURE(true, privateKey, publicKey, null, prehashed, signature);

    return signature;
  }

  /**
   * Verifies an ED25519 signature.
   *
   * @param publicKey The public key (32 bytes)
   * @param message The message that was signed
   * @param signature The signature to verify
   * @return true if the signature is valid, false otherwise
   */
  public static boolean verify(byte[] publicKey, byte[] message, byte[] signature) {
    // Verify without prehashing and with no context
    return EDDSA.VERIFY(false, publicKey, null, message, signature);
  }

  /**
   * Verifies an ED25519 signature for a pre-hashed message.
   *
   * @param publicKey The public key (32 bytes)
   * @param prehashed The pre-hashed message
   * @param signature The signature to verify
   * @return true if the signature is valid, false otherwise
   */
  public static boolean verifyPrehashed(byte[] publicKey, byte[] prehashed, byte[] signature) {
    // Verify with prehashing and no context
    return EDDSA.VERIFY(true, publicKey, null, prehashed, signature);
  }

  /**
   * Derives a public key from a private key.
   *
   * @param privateKey The private key
   * @return The corresponding public key
   */
  public static byte[] derivePublicKey(byte[] privateKey) {
    byte[] publicKey = new byte[PUBLIC_KEY_SIZE];
    EDDSA.KEY_PAIR_GENERATE(null, privateKey, publicKey);
    return publicKey;
  }
}
