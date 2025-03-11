package org.radix.hyperscale.serialization;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.nio.charset.StandardCharsets;

import org.radix.hyperscale.utils.UInt128;

/**
 * Collection of Serialization-related utilities
 */
public class SerializationUtils {
	private static final HashFunction murmur3_128 = Hashing.murmur3_128();

	private SerializationUtils() {
		throw new IllegalStateException("Cannot instantiate.");
	}

	public static UInt128 stringToNumericID(String id) {
		HashCode h = murmur3_128.hashBytes(id.getBytes(StandardCharsets.UTF_8));
		return UInt128.from(h.asBytes());
	}
}
