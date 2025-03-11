package org.radix.hyperscale.serialization.mapper;

/**
 * Constants for DSON protocol encoded in CBOR/JSON.
 */
public final class JacksonCodecConstants {
	private JacksonCodecConstants() {
		throw new IllegalStateException("Can't construct");
	}

	// Encodings for CBOR mappings
	public static final byte BYTES_VALUE = 0x01;
	public static final byte HASH_VALUE  = 0x03;
	public static final byte ADDR_VALUE  = 0x04;
	public static final byte U20_VALUE   = 0x05; // 0x20 byte = 256 bit unsigned int
	public static final byte RRI_VALUE   = 0x06;
	public static final byte U30_VALUE   = 0x07; // 0x30 byte = 384 bit unsigned int
	public static final byte U10_VALUE   = 0x09; // 0x10 byte = 128 bit unsigned int
	public static final byte PUB_KEY_VALUE   = 0x10;
	public static final byte IDENTITY_VALUE   = 0x11;
	public static final byte EC_SIGNATURE_VALUE = 0x12;
	public static final byte BLS_SIGNATURE_VALUE = 0x13;
	public static final byte MERKLE_PROOF_VALUE = 0x20;
	public static final byte SHARD_ID_VALUE = 0x30;
	public static final byte SHARD_GROUP_ID_VALUE = 0x31;
	public static final byte STATE_ADDRESS_VALUE = 0x40;

	// Type tag prefixes used in strings for JSON mappings
	public static final int STR_VALUE_LEN     = 3;
	public static final String BYTE_STR_VALUE = ":b:";
	public static final String HASH_STR_VALUE = ":h:";
	public static final String U256_STR_VALUE  = ":u:"; // 0x20 byte = 256 bit unsigned int
	public static final String PUB_KEY_STR_VALUE  = ":k:";
	public static final String EC_SIGNATURE_STR_VALUE  = ":e:";
	public static final String BLS_SIGNATURE_STR_VALUE  = ":l:";
	public static final String IDENTITY_STR_VALUE  = ":i:";
	public static final String MERKLE_PROOF_STR_VALUE = ":m:";
	public static final String SHARD_ID_STR_VALUE = ":s:";
	public static final String STATE_ADDRESS_STR_VALUE = ":a:";
	public static final String SHARD_GROUP_ID_STR_VALUE = ":g:";
}
