package org.radix.hyperscale.crypto.bls12381.group;

/**
 * Forked from https://github.com/ConsenSys/mikuli/tree/master/src/main/java/net/consensys/mikuli/crypto
 * 
 * Modified for use with Cassandra as internal code not a dependency
 * 
 * Original repo source has no license headers.
 */
public interface Group<G> 
{
  G add(G g);

  G mul(Scalar scalar);
}
