package org.radix.hyperscale.collections;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.util.BitSet;
import java.util.Collection;
import java.util.Objects;

import org.apache.commons.lang3.ArrayUtils;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.Serializable;
import org.radix.hyperscale.serialization.SerializerId2;
import org.radix.hyperscale.serialization.DsonOutput.Output;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Implementation of a Bloom-filter, as described here:
 * http://en.wikipedia.org/wiki/Bloom_filter
 *
 * For updates and bugfixes, see http://github.com/magnuss/java-bloomfilter
 *
 * Inspired by the SimpleBloomFilter-class written by Ian Clarke. This
 * implementation provides a more evenly distributed Hash-function by
 * using a proper digest instead of the Java RNG. Many of the changes
 * were proposed in comments in his blog:
 * http://blog.locut.us/2008/01/12/a-decent-stand-alone-java-bloom-filter-implementation/
 *
 * @param <E> Object type that is to be inserted into the Bloom filter, e.g. String or Integer.
 */
@SerializerId2("collections.bloom")
public class Bloom extends Serializable
{
    static final Charset CHARSET = StandardCharsets.UTF_8; // encoding used for storing hash values as strings
    static final String HASHNAME = "MD5"; // MD5 gives good enough accuracy in most circumstances. Change to SHA1 if it's needed
    private static final ThreadLocal<MessageDigest> digester = ThreadLocal.withInitial(() -> getDigester(HASHNAME, null));
    private static MessageDigest getDigester(String algorithm, String provider) 
	{
		try 
		{
			return (provider == null) ? MessageDigest.getInstance(algorithm) : MessageDigest.getInstance(algorithm, provider);
		} catch (NoSuchProviderException e) {
			throw new IllegalArgumentException("No such provider for: " + algorithm, e);
		} catch (NoSuchAlgorithmException e) {
			throw new IllegalArgumentException("No such algorithm: " + algorithm, e);
		}
	}
	
    public static Bloom from(byte[] bytes) throws IOException
    {
    	ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    	DataInputStream dis = new DataInputStream(bais);
    	int bitSetSize = dis.readInt();
    	int expectedNumberOfFilterElements = dis.readInt();
    	int numberOfAddedElements = dis.readInt();
    	int numBitSetBytes = dis.readInt();
    	byte[] bitSetBytes = new byte[numBitSetBytes];
    	dis.read(bitSetBytes);
    	BitSet bitSet = BitSet.valueOf(bitSetBytes);
        return new Bloom(bitSetSize, expectedNumberOfFilterElements, numberOfAddedElements, bitSet); 
    }

	//Parsed with a getter / setter
	private BitSet 	bitset;

	@JsonProperty("numbits")
	@DsonOutput(Output.ALL)
    private int bitSetSize;

	@JsonProperty("expected")
//	@DsonOutput(Output.ALL)
	@DsonOutput(value = {Output.HASH, Output.WIRE, Output.PERSIST})
    private int expectedNumberOfFilterElements; // expected (maximum) number of elements to be added
	
	@JsonProperty("added")
//	@DsonOutput(Output.ALL)
	@DsonOutput(value = {Output.HASH, Output.WIRE, Output.PERSIST})
    private int numberOfAddedElements; // number of elements actually added to the Bloom filter
	
	@JsonProperty("k")
	@DsonOutput(Output.ALL)
    private int k; // number of hash functions

    public Bloom() 
    {
    	super();
    }
    
	/**
      * Constructs an empty Bloom filter. The total length of the Bloom filter will be
      * c*n.
      *
      * @param c is the number of bits used per element.
      * @param n is the expected number of elements the filter will contain.
      * @param k is the number of hash functions used.
      */
    public Bloom(final double c, final int n, final int k) 
    {
      this.expectedNumberOfFilterElements = n;
      this.k = k;
      this.bitSetSize = (int)Math.ceil(c * n);
      this.numberOfAddedElements = 0;
      this.bitset = new BitSet(this.bitSetSize);
    }

    /**
     * Constructs an empty Bloom filter. The optimal number of hash functions (k) is estimated from the total size of the Bloom
     * and the number of expected elements.
     *
     * @param bitSetSize defines how many bits should be used in total for the filter.
     * @param expectedNumberOElements defines the maximum number of elements the filter is expected to contain.
     */
    public Bloom(final int bitSetSize, final int expectedNumberOElements) 
    {
        this(bitSetSize / (double)expectedNumberOElements,
             expectedNumberOElements,
             (int) Math.round((bitSetSize / (double)expectedNumberOElements) * Math.log(2.0)));
    }

    /**
     * Constructs an empty Bloom filter with a given false positive probability. The number of bits per
     * element and the number of hash functions is estimated
     * to match the false positive probability.
     *
     * @param falsePositiveProbability is the desired false positive probability.
     * @param expectedNumberOfElements is the expected number of elements in the Bloom filter.
     */
    public Bloom(final double falsePositiveProbability, final int expectedNumberOfElements) 
    {
        this(Math.ceil(-(Math.log(falsePositiveProbability) / Math.log(2))) / Math.log(2), // c = k / ln(2)
             expectedNumberOfElements,
             (int)Math.ceil(-(Math.log(falsePositiveProbability) / Math.log(2)))); // k = ceil(-log_2(false prob.))
    }

    /**
     * Constructs a new Bloom filter containing the provided collection with a given false positive probability. The number of bits per
     * element and the number of hash functions is estimated to match the false positive probability.
     *
     * @param falsePositiveProbability is the desired false positive probability.
     * @param elements is the collection of elements to add to the Bloom filter.
     */
    public Bloom(final double falsePositiveProbability, final Collection<byte[]> elements) 
    {
        this(Math.ceil(-(Math.log(falsePositiveProbability) / Math.log(2))) / Math.log(2), // c = k / ln(2)
        	 elements.size(),
             (int)Math.ceil(-(Math.log(falsePositiveProbability) / Math.log(2)))); // k = ceil(-log_2(false prob.))
        
        for (byte[] element : elements)
        	add(element);
    }

    /**
     * Construct a new Bloom filter based on existing Bloom filter data.
     *
     * @param bitSetSize defines how many bits should be used for the filter.
     * @param expectedNumberOfFilterElements defines the maximum number of elements the filter is expected to contain.
     * @param numberOfAddedElements specifies how many elements have been inserted into the <code>filterData</code> BitSet.
     * @param filterData a BitSet representing an existing Bloom filter.
     */
    public Bloom(final int bitSetSize, final int expectedNumberOfFilterElements, final int numberOfAddedElements, final BitSet filterData) 
    {
        this(bitSetSize, expectedNumberOfFilterElements);
        this.bitset = (BitSet) filterData.clone();
        this.numberOfAddedElements = numberOfAddedElements;
    }
    
	public Bloom(final Bloom bloom)
	{
		this(Objects.requireNonNull(bloom, "Bloom to copy is null").bitSetSize, bloom.expectedNumberOfFilterElements, bloom.numberOfAddedElements, bloom.bitset);
	}

	@JsonProperty("bits")
	@DsonOutput(Output.ALL)
	private byte[] getDSONBitset() 
	{
		return this.bitset.toByteArray();
	}

	@JsonProperty("bits")
	private void setDSONBitset(byte[] bytes) 
	{
		this.bitset = BitSet.valueOf(bytes);
	}
	
    /**
     * Generates digests based on the contents of an array of bytes and splits the result into 4-byte int's and store them in an array. The
     * digest function is called until the required number of int's are produced. For each call to digest a salt
     * is prepended to the data. The salt is increased by 1 for each call.
     *
     * @param data specifies input data.
     * @param hashes number of hashes/int's to produce.
     * @return array of int-sized hashes
     */
    private int[] createHashes(byte[] data, int hashes) 
    {
        int[] result = new int[hashes];

        int k = 0;
        byte salt = 0;
        MessageDigest digester = Bloom.digester.get();
        digester.reset();
        while (k < hashes) 
        {
            byte[] digest;
            digester.update(salt);
            digest = digester.digest(data);                
            salt++;
        
            for (int i = 0; i < digest.length/4 && k < hashes; i++) 
            {
                int h = 0;
                for (int j = (i*4); j < (i*4)+4; j++) 
                {
                    h <<= 8;
                    h |= ((int) digest[j]) & 0xFF;
                }
                result[k] = h;
                k++;
            }
        }
        return result;
    }

    /**
     * Compares the contents of two instances to see if they are equal.
     *
     * @param obj is the object to compare to.
     * @return True if the contents of the objects are equal.
     */
    @Override
    public boolean equals(Object obj) 
    {
        if (obj == null)
            return false;

        if (getClass() != obj.getClass())
            return false;
        
    	synchronized(this)
        {
	        final Bloom other = (Bloom) obj;        
	        if (this.expectedNumberOfFilterElements != other.expectedNumberOfFilterElements)
	            return false;
	
	        if (this.k != other.k)
	            return false;
	        
	        if (this.bitSetSize != other.bitSetSize)
	            return false;
	        
	        if (this.bitset != other.bitset && (this.bitset == null || !this.bitset.equals(other.bitset)))
	            return false;
        }
       
        return true;
    }

    /**
     * Calculates a hash code for this class.
     * @return hash code representing the contents of an instance of this class.
     */
    @Override
    public int hashCode() 
    {
    	synchronized(this)
        {
	        int hash = 7;
	        hash = 61 * hash + (this.bitset != null ? this.bitset.hashCode() : 0);
	        hash = 61 * hash + this.expectedNumberOfFilterElements;
	        hash = 61 * hash + this.bitSetSize;
	        hash = 61 * hash + this.k;
	        return hash;
        }
    }


    /**
     * Calculates the expected probability of false positives based on
     * the number of expected filter elements and the size of the Bloom filter.
     * <br /><br />
     * The value returned by this method is the <i>expected</i> rate of false
     * positives, assuming the number of inserted elements equals the number of
     * expected elements. If the number of elements in the Bloom filter is less
     * than the expected value, the true probability of false positives will be lower.
     *
     * @return expected probability of false positives.
     */
    public double expectedFalsePositiveProbability() 
    {
        return getFalsePositiveProbability(expectedNumberOfFilterElements);
    }

    /**
     * Calculate the probability of a false positive given the specified
     * number of inserted elements.
     *
     * @param numberOfElements number of inserted elements.
     * @return probability of a false positive.
     */
    public double getFalsePositiveProbability(double numberOfElements) 
    {
        // (1 - e^(-k * n / m)) ^ k
        return Math.pow((1 - Math.exp(-k * (double) numberOfElements / (double) bitSetSize)), k);
    }

    /**
     * Get the current probability of a false positive. The probability is calculated from
     * the size of the Bloom filter and the current number of elements added to it.
     *
     * @return probability of false positives.
     */
    public double getFalsePositiveProbability() 
    {
        return getFalsePositiveProbability(numberOfAddedElements);
    }


    /**
     * Returns the value chosen for K.<br />
     * <br />
     * K is the optimal number of hash functions based on the size
     * of the Bloom filter and the expected number of inserted elements.
     *
     * @return optimal k.
     */
    public int getK() 
    {
        return k;
    }

    /**
     * Sets all bits to false in the Bloom filter.
     */
    public void clear() 
    {
    	synchronized(this)
    	{
			bitset.clear();
			numberOfAddedElements = 0;
        }
    }

    /**
     * Adds an array of bytes to the Bloom filter.
     *
     * @param bytes array of bytes to add to the Bloom filter.
     */
    public void add(byte[] bytes) 
    {
    	int[] hashes = createHashes(bytes, k);

    	synchronized(this)
    	{
			for (int hash : hashes)
				bitset.set(Math.abs(hash % bitSetSize), true);
       
			numberOfAddedElements ++;
        }
    }

    /**
     * Returns true if the array of bytes could have been inserted into the Bloom filter.
     * Use getFalsePositiveProbability() to calculate the probability of this
     * being correct.
     *
     * @param bytes array of bytes to check.
     * @return true if the array could have been inserted into the Bloom filter.
     */
    public boolean contains(byte[] bytes) 
    {
        int[] hashes = createHashes(bytes, k);
        
    	synchronized(this)
    	{
	        for (int hash : hashes) 
	        {
	            if (!bitset.get(Math.abs(hash % bitSetSize)))
	                return false;
	        }
        }
		
        return true;
    }

    /**
     * Read a single bit from the Bloom filter.
     * @param bit the bit to read.
     * @return true if the bit is set, false if it is not.
     */
    public boolean getBit(int bit) 
    {
    	synchronized(this)
    	{
			return bitset.get(bit);
		}
    }

    /**
     * Set a single bit in the Bloom filter.
     * @param bit is the bit to set.
     * @param value If true, the bit is set. If false, the bit is cleared.
     */
    public void setBit(int bit, boolean value) 
    {
    	synchronized(this)
    	{
			bitset.set(bit, value);
        }
    }

    /**
     * Return the bit set used to store the Bloom filter.
     * @return bit set representing the Bloom filter.
     */
    public BitSet getBitSet() {
        return bitset;
    }

    /**
     * Returns the number of bits in the Bloom filter. Use count() to retrieve
     * the number of inserted elements.
     *
     * @return the size of the bitset used by the Bloom filter.
     */
    public int size() {
        return this.bitSetSize;
    }

    /**
     * Returns the number of elements added to the Bloom filter after it
     * was constructed or after clear() was called.
     *
     * @return number of elements added to the Bloom filter.
     */
    public int count() {
        return this.numberOfAddedElements;
    }

    /**
     * Returns the expected number of elements to be inserted into the filter.
     * This value is the same value as the one passed to the constructor.
     *
     * @return expected number of elements.
     */
    public int getExpectedNumberOfElements() {
        return expectedNumberOfFilterElements;
    }

    /**
     * Get expected number of bits per element when the Bloom filter is full. This value is set by the constructor
     * when the Bloom filter is created. See also getBitsPerElement().
     *
     * @return expected number of bits per element.
     */
    public double getExpectedBitsPerElement() {
    	return this.bitSetSize / (double)this.expectedNumberOfFilterElements;
    }

    /**
     * Get actual number of bits per element based on the number of elements that have currently been inserted and the length
     * of the Bloom filter. See also getExpectedBitsPerElement().
     *
     * @return number of bits per element.
     */
    public double getBitsPerElement() {
        return this.bitSetSize / (double)numberOfAddedElements;
    }
    
    public byte[] toByteArray()
    {
    	byte[] bitSetBytes = this.bitset.toByteArray();
    	ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES+Integer.BYTES+Integer.BYTES+Integer.BYTES+bitSetBytes.length);
    	buffer.putInt(this.bitSetSize);
    	buffer.putInt(this.expectedNumberOfFilterElements);
    	buffer.putInt(this.numberOfAddedElements);
    	buffer.putInt(bitSetBytes.length);
    	buffer.put(bitSetBytes);
    	return ArrayUtils.clone(buffer.array());
    }
}