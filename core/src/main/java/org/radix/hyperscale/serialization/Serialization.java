package org.radix.hyperscale.serialization;

import static org.radix.hyperscale.serialization.mapper.DsonFieldFilter.filterProviderFor;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.mutable.MutableLong;
import org.json.JSONObject;
import org.radix.hyperscale.collections.ByteBufferPool;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.crypto.Identity;
import org.radix.hyperscale.crypto.merkle.MerkleProof;
import org.radix.hyperscale.ledger.ShardGroupID;
import org.radix.hyperscale.ledger.ShardID;
import org.radix.hyperscale.serialization.DsonOutput.Output;
import org.radix.hyperscale.serialization.core.ClasspathScanningSerializationPolicy;
import org.radix.hyperscale.serialization.core.ClasspathScanningSerializerIds;
import org.radix.hyperscale.serialization.mapper.JacksonCborMapper;
import org.radix.hyperscale.serialization.mapper.JacksonCodecConstants;
import org.radix.hyperscale.serialization.mapper.JacksonJsonMapper;
import org.radix.hyperscale.utils.Bytes;
import org.radix.hyperscale.utils.UInt256;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ser.FilterProvider;
import com.google.common.annotations.VisibleForTesting;

/**
 * Serialization class that handles conversion to/from DSON and JSON.
 */
public class Serialization 
{
	private static final int SERIALIZATION_BUFFER_SIZE = 1<<20;
	private static final ThreadLocal<ByteBuffer> serializationBuffer = ThreadLocal.withInitial(() -> ByteBuffer.allocate(SERIALIZATION_BUFFER_SIZE));
	private static final ThreadLocal<ByteBufferOutputStream> serializationBufferOutputStream = ThreadLocal.withInitial(() -> new ByteBufferOutputStream(serializationBuffer.get(), true));
	public static final ByteBufferPool bufferPool = new ByteBufferPool("Serialization", 1<<27, ByteBufferPool.MIN_BUFFER_SIZE, 1<<18, true);

	public static boolean DEBUG_STATISTICS = Boolean.getBoolean("debug");
	
	private static Serialization instance;
	
	static 
	{
		try {
			instance = Serialization.create(ClasspathScanningSerializerIds.create(), ClasspathScanningSerializationPolicy.create());
		} 
		catch (SerializationException e) 
		{
			throw new RuntimeException(e);
		}
	}

	public static Serialization getInstance() 
	{
		return instance;
	}

	/**
	 * Create a new instance of {@link Serialization} with the specified IDs and policy.
	 *
	 * @param idLookup The {@link SerializerIds} to use for class and property lookup.
	 * @param policy The {@link SerializationPolicy} to use for determining serialization outputs.
	 * @return A new instance of {@link Serialization}.
	 * @throws SerializationException 
	 */
	public static Serialization create(SerializerIds idLookup, SerializationPolicy policy) throws SerializationException 
	{
		return new Serialization(idLookup, policy);
	}
	
	private final Map<Output, JacksonCborMapper> dsonMappers;
	private final Map<Output, JacksonJsonMapper> jsonMappers;

	private final SerializerIds idLookup;

	private final FilterProvider allProvider;
	private final FilterProvider noneProvider;
	
	private final Map<String, Entry<MutableLong, MutableLong>> statistics;

	// Constructor set up to be dependency injection capable at some future date
	@VisibleForTesting
	Serialization(SerializerIds idLookup, SerializationPolicy policy) throws SerializationException 
	{
		this.idLookup = idLookup;

		EnumSet<Output> availableOutputs = EnumSet.allOf(Output.class);
		availableOutputs.remove(Output.ALL);
		availableOutputs.remove(Output.NONE);

		this.noneProvider = filterProviderFor(Collections.emptyMap());
		Map<Class<?>, Set<String>> allFields =  availableOutputs.stream()
													.flatMap(output -> policy.getIncludedFields(output).entrySet().stream())
													.collect(Collectors.groupingBy(Map.Entry::getKey, flatMapping(e -> e.getValue().stream(), Collectors.toSet())));
		this.allProvider = filterProviderFor(allFields);

		this.dsonMappers = new EnumMap<Output, JacksonCborMapper>(Output.class);
		for (Output e : availableOutputs)
			dsonMappers.put(e, JacksonCborMapper.create(e, idLookup, filterProviderFor(policy.getIncludedFields(e))));
		dsonMappers.put(Output.NONE, JacksonCborMapper.create(Output.NONE, idLookup, this.noneProvider));
		dsonMappers.put(Output.ALL, JacksonCborMapper.create(Output.ALL, idLookup, this.allProvider));

		this.jsonMappers = new EnumMap<Output, JacksonJsonMapper>(Output.class);
		for (Output e : availableOutputs)
			jsonMappers.put(e, JacksonJsonMapper.create(idLookup, filterProviderFor(policy.getIncludedFields(e))));
		this.jsonMappers.put(Output.NONE, JacksonJsonMapper.create(idLookup, this.noneProvider));
		this.jsonMappers.put(Output.ALL, JacksonJsonMapper.create(idLookup, this.allProvider));
		
		this.statistics = new ConcurrentHashMap<String, Entry<MutableLong, MutableLong>>(64);
	}
	
	public Collection<Entry<String, Entry<Long, Long>>> statistics()
	{
		final List<Entry<String, Entry<Long, Long>>> statistics = new ArrayList<Entry<String, Entry<Long, Long>>>();
		this.statistics.forEach((n, e) -> statistics.add(new AbstractMap.SimpleEntry<String, Entry<Long, Long>>(n, new AbstractMap.SimpleEntry<Long, Long>(e.getKey().getValue(), e.getValue().getValue()))));

		Collections.sort(statistics, (o1, o2) -> o1.getKey().compareToIgnoreCase(o2.getKey()));
		
		return statistics;
	}

	/**
	 * Convert the specified object to DSON encoded bytes for the specified
	 * output mode.
	 *
	 * @param o The object to serialize
	 * @param output The output mode to serialize for
	 * @return The serialized object as a DSON byte array
	 * @throws SerializationException if something goes wrong with serialization
	 */
	public byte[] toDson(final Object object, final DsonOutput.Output output) throws SerializationException 
	{
		Objects.requireNonNull(object, "Object is null");
		Objects.requireNonNull(output, "Output is null");
		
		try 
		{
			// Is cached?
			if (object instanceof Serializable serializable)
			{
				final ByteBuffer cachedOutput = serializable.getCachedDsonOutput();
				if (cachedOutput == null)
					serializable.onSerialize(output);
				else if(output.equals(Output.WIRE) || output.equals(Output.PERSIST))
				{
					statistics("output:cached", 0);
					return Arrays.copyOfRange(cachedOutput.array(), 0, cachedOutput.limit());
				}
			}

			final ByteBufferOutputStream serializationBufferOutputStream = Serialization.serializationBufferOutputStream.get().clear();
			dsonMapper(output).writeValue(serializationBufferOutputStream, object);
			statistics("output:bytes", 0);

			return Arrays.copyOfRange(serializationBufferOutputStream.getByteBuffer().array(), 0, serializationBufferOutputStream.getByteBuffer().position());
		} 
		catch (IOException ex) 
		{
			throw new SerializationException("Error serializing to DSON", ex);
		}
	}
	
	/**
	 * Convert the specified object to DSON encoded bytes for the specified
	 * output mode.
	 *
	 * @param o The object to serialize
	 * @param output The output mode to serialize for
	 * @return The size of the serialized object in bytes
	 * @throws SerializationException if something goes wrong with serialization
	 */
	public int toDsonSize(final Object object, final DsonOutput.Output output) throws SerializationException 
	{
		Objects.requireNonNull(object, "Object is null");
		Objects.requireNonNull(output, "Output is null");
		
		try 
		{
			// Is cached?
			if (object instanceof Serializable serializable)
			{
				final ByteBuffer cachedOutput = serializable.getCachedDsonOutput();
				if (cachedOutput == null)
					serializable.onSerialize(output);
				else if(output.equals(Output.WIRE) || output.equals(Output.PERSIST))
				{
					statistics("output:cached", 0);
					return cachedOutput.limit();
				}
			}

			final ByteBufferOutputStream serializationBufferOutputStream = Serialization.serializationBufferOutputStream.get().clear();
			dsonMapper(output).writeValue(serializationBufferOutputStream, object);
			statistics("output:bytes", 0);

			return serializationBufferOutputStream.getByteBuffer().position();
		} 
		catch (IOException ex) 
		{
			throw new SerializationException("Error serializing to DSON", ex);
		}
	}

	/**
	 * Convert the specified object to DSON encoded bytes for the specified
	 * output mode.
	 *
	 * @param o The object to serialize
	 * @param output The output mode to serialize for
	 * @return The serialized object as a DSON byte array
	 * @throws SerializationException if something goes wrong with serialization
	 */
	public void toDson(final OutputStream stream, final Object object, final DsonOutput.Output output) throws SerializationException 
	{
		Objects.requireNonNull(object, "Object is null");
		Objects.requireNonNull(output, "Output is null");
		
		try 
		{
			// Is cached?
			if (object instanceof Serializable serializable)
			{
				final ByteBuffer cachedOutput = serializable.getCachedDsonOutput();
				if (cachedOutput == null)
					serializable.onSerialize(output);
				else if(output.equals(Output.WIRE) || output.equals(Output.PERSIST))
				{
					stream.write(cachedOutput.array(), 0, cachedOutput.limit());
					statistics("output:cached", 0);
					return;
				}
			}

			dsonMapper(output).writeValue(stream, object);
			statistics("output:stream", 0);
		} 
		catch (IOException ex) 
		{
			throw new SerializationException("Error serializing to DSON", ex);
		}
	}
	
	/**
	 * Convert the specified object to a hash digest.
	 *
	 * @param o The object to serialize
	 * @return The hash object containing the digest
	 * @throws SerializationException if something goes wrong with serialization
	 */
	public Hash toHash(final Object object) throws SerializationException 
	{
		Objects.requireNonNull(object, "Object is null");
		
		try 
		{
			if (object instanceof Serializable serializable)
				serializable.onSerialize(Output.HASH);
			
			final ByteBufferOutputStream serializationBufferOutputStream = Serialization.serializationBufferOutputStream.get().clear();
			dsonMapper(Output.HASH).writeValue(serializationBufferOutputStream, object);
			statistics("output:hash", 0);
			
			return Hash.hash(serializationBufferOutputStream.getByteBuffer().array(), 0, serializationBufferOutputStream.getByteBuffer().position());
		} 
		catch (IOException ex) 
		{
			throw new SerializationException("Error serializing to hash", ex);
		}
	}

	
	public long statistics(String clazz, int length)
	{
		Entry<MutableLong, MutableLong> entry = this.statistics.computeIfAbsent(clazz, k -> new AbstractMap.SimpleEntry<>(new MutableLong(0), new MutableLong(0)));
		synchronized(entry)
		{
			entry.getKey().increment();
			entry.getValue().add(length);
		}
		return entry.getValue().longValue();
	}
	
	public long statistics(String clazz, int length, int reducedLength)
	{
		Entry<MutableLong, MutableLong> entry = this.statistics.computeIfAbsent(clazz, k -> new AbstractMap.SimpleEntry<>(new MutableLong(0), new MutableLong(0)));
		synchronized(entry)
		{
			entry.getKey().increment();
			entry.getValue().subtract((long)length-reducedLength);
		}
		return entry.getValue().longValue();
	}

	/**
	 * Convert the specified DSON encoded byte array to an instance of the
	 * specified class.
	 *
	 * @param bytes The DSON encoded object to deserialize
	 * @param valueType The class of the object to deserialize
	 * @param compressed Whether the object to deserialize is compressed
	 * @return The deserialized object
	 * @throws SerializationException if something goes wrong with serialization
	 */
	public <T> T fromDson(byte[] bytes, Class<T> valueType) throws SerializationException 
	{
		try 
		{
			T deserialized = dsonMapper(Output.ALL).readValue(bytes, valueType);
			if (deserialized instanceof Serializable serializable)
//			{
//				serializable.setCachedOutput(bytes.clone());
				serializable.onDeserialized();
//			}

			return deserialized;
		} 
		catch (IOException ex) 
		{
			ex.printStackTrace();
			throw new SerializationException("Error converting from DSON", ex);
		}
	}

	/**
	 * Convert the specified DSON encoded byte array to an instance of the
	 * specified class.
	 *
	 * @param bytes The DSON encoded object to deserialize
	 * @param valueType The class of the object to deserialize
	 * @param compressed Whether the object to deserialize is compressed
	 * @return The deserialized object
	 * @throws SerializationException if something goes wrong with serialization
	 */
	public <T> T fromDson(byte[] bytes, int offset, int length, Class<T> valueType) throws SerializationException 
	{
		try 
		{
			T deserialized = dsonMapper(Output.ALL).readValue(bytes, offset, length, valueType);
			if (deserialized instanceof Serializable serializable)
//			{
//				serializable.setCachedOutput(Arrays.copyOfRange(bytes, offset, offset+length));
				serializable.onDeserialized();
//			}
			
			return deserialized;
		} 
		catch (IOException ex) 
		{
			throw new SerializationException("Error converting from DSON", ex);
		}
	}

	/**
	 * Convert the specified object to a JSON encoded string for the specified
	 * output mode.
	 *
	 * @param o The object to serialize
	 * @param output The output mode to serialize for
	 * @return The serialized object as a JSON string
	 * @throws SerializationException if something goes wrong with serialization
	 */
	public String toJson(Object object, DsonOutput.Output output) throws SerializationException 
	{
		try 
		{
			if (object instanceof Serializable serializable)
				serializable.onSerialize(output);

			return jsonMapper(output).writeValueAsString(object);
		} 
		catch (JsonProcessingException ex) 
		{
			throw new SerializationException("Error converting to JSON", ex);
		}
	}

	/**
	 * Convert the specified object to a {@link JSONObject} encoded string for the specified
	 * output mode.
	 *
	 * @param o The object to serialize
	 * @param output The output mode to serialize for
	 * @return The serialized object as a JSON string
	 */
	public JSONObject toJsonObject(Object object, DsonOutput.Output output) 
	{
		if (object instanceof Serializable serializable)
			serializable.onSerialize(output);
		
		return jsonMapper(output).convertValue(object, JSONObject.class);
	}


	/**
	 * Convert the specified DSON encoded byte array to an instance of the
	 * specified class.
	 *
	 * @param bytes The DSON encoded object to deserialize
	 * @param valueType The class of the object to deserialize
	 * @return The deserialized object
	 * @throws SerializationException if something goes wrong with serialization
	 */
	public <T> T fromDson(byte[] bytes, DsonJavaType valueType) throws SerializationException 
	{
		try 
		{
			T deserialized = dsonMapper(Output.ALL).readValue(bytes, valueType.javaType());
			if (deserialized instanceof Serializable serializable)
				serializable.onDeserialized();
			return deserialized;
		} 
		catch (IOException ex) 
		{
			throw new SerializationException("Error converting from DSON", ex);
		}
	}

	/**
	 * Convert the specified JSON encoded string to an instance of the
	 * specified class.
	 *
	 * @param json The JSON encoded object to deserialize
	 * @param valueType The class of the object to deserialize
	 * @return The deserialized object
	 * @throws SerializationException if something goes wrong with serialization
	 */
	public <T> T fromJson(String json, Class<T> valueType) throws SerializationException 
	{
		try 
		{
			T deserialized = jsonMapper(Output.ALL).readValue(json, valueType);
			if (deserialized instanceof Serializable serializable)
				serializable.onDeserialized();
			return deserialized;
		} 
		catch (IOException ex) 
		{
			throw new SerializationException("Error converting from JSON", ex);
		}
	}

	/**
	 * Convert the specified JSON encoded string to an instance of the
	 * specified class.
	 *
	 * @param json The JSON encoded object to deserialize
	 * @param valueType The class of the object to deserialize
	 * @return The deserialized object
	 * @throws SerializationException if something goes wrong with serialization
	 */
	public <T> T fromJson(String json, JsonJavaType valueType) throws SerializationException
	{
		try 
		{
			T deserialized = jsonMapper(Output.ALL).readValue(json, valueType.javaType());
			if (deserialized instanceof Serializable serializable)
				serializable.onDeserialized();

			return deserialized;
		} 
		catch (IOException ex)
		{
			throw new SerializationException("Error converting from JSON", ex);
		}
	}

	/**
	 * Convert the specified JSONObject to an instance of the
	 * specified class.
	 *
	 * @param json The {@link JSONObject} object to convert
	 * @param valueType The class of the object to convert to
	 * @return The converted object
	 */
	public <T> T fromJsonObject(JSONObject json, Class<T> valueType) {
		return jsonMapper(Output.ALL).convertValue(json, valueType);
	}
	
	@SuppressWarnings("unchecked")
	public <T> T fromTypedString(String value) throws SerializationException
	{
		if (value.startsWith(JacksonCodecConstants.BYTE_STR_VALUE))
			return (T) Bytes.fromBase64String(value.substring(JacksonCodecConstants.STR_VALUE_LEN));
		else if (value.startsWith(JacksonCodecConstants.HASH_STR_VALUE))
			return (T) Hash.from(value, JacksonCodecConstants.STR_VALUE_LEN);
		else if (value.startsWith(JacksonCodecConstants.U256_STR_VALUE))
			return (T) UInt256.from(value, JacksonCodecConstants.STR_VALUE_LEN);
		else if (value.startsWith(JacksonCodecConstants.MERKLE_PROOF_STR_VALUE))
			return (T) MerkleProof.from(value.substring(JacksonCodecConstants.STR_VALUE_LEN));
		else if (value.startsWith(JacksonCodecConstants.SHARD_ID_STR_VALUE))
			return (T) ShardID.from(value, JacksonCodecConstants.STR_VALUE_LEN);
		else if (value.startsWith(JacksonCodecConstants.SHARD_GROUP_ID_STR_VALUE))
			return (T) ShardGroupID.from(value.substring(JacksonCodecConstants.STR_VALUE_LEN));
		else if (value.startsWith(JacksonCodecConstants.IDENTITY_STR_VALUE))
		{
			try
			{
				return (T) Identity.from(value, JacksonCodecConstants.STR_VALUE_LEN);
			}
			catch (Exception cex)
			{
				throw new SerializationException(cex);
			}
		}
		else if (value.startsWith("["))
			return (T) fromJson(value, ArrayList.class);
		else if (value.startsWith("{"))
			return (T) fromJson(value, Object.class);
		else
		{
			try
			{
				return (T) Long.valueOf(value);
			}
			catch (NumberFormatException nfex) {}
			
			return (T) value;
		}
	}

	/**
	 * Return a collection type for use with the {@link #fromDson(byte[], DsonJavaType)} method.
	 *
	 * @param collectionClass The collection class to deserialize.
	 * @param elementClass The collection element type.
	 * @return The deserialized collection.
	 */
	@SuppressWarnings("rawtypes")
	public DsonJavaType dsonCollectionType(Class<? extends Collection> collectionClass, Class<?> elementClass) 
	{
		JavaType type = dsonMappers.get(Output.ALL).getTypeFactory().constructCollectionType(collectionClass, elementClass);
		return new DsonJavaType(type);
	}

	/**
	 * Return a collection type for use with the {@link #fromJson(String, JsonJavaType)} method.
	 *
	 * @param collectionClass The collection class to deserialize.
	 * @param elementClass The collection element type.
	 * @return The deserialized collection.
	 */
	@SuppressWarnings("rawtypes")
	public JsonJavaType jsonCollectionType(Class<? extends Collection> collectionClass, Class<?> elementClass) 
	{
		JavaType type = jsonMappers.get(Output.ALL).getTypeFactory().constructCollectionType(collectionClass, elementClass);
		return new JsonJavaType(type);
	}

	/**
	 * Retrieve serializer ID from class.
	 *
	 * @param cls The class to look up the ID for
	 * @return The serializer ID, or {@code null} if no serializer for the specified class.
	 */
	public String getIdForClass(Class<?> cls) 
	{
		return idLookup.getIdForClass(cls);
	}

	/**
	 * Retrieve class given a serializer ID.
	 *
	 * @param id The ID to look up the class for
	 * @return The class, or {@code null} if serializer ID unknown.
	 */
	public Class<?> getClassForId(String id) 
	{
		return idLookup.getClassForId(id);
	}

	private JacksonCborMapper dsonMapper(Output output) 
	{
		return dsonMappers.get(output);
	}

	private JacksonJsonMapper jsonMapper(Output output) 
	{
		return jsonMappers.get(output);
	}

	private static <T, U, A, R> Collector<T, ?, R> flatMapping(Function<? super T, ? extends Stream<? extends U>> mapper, Collector<? super U, A, R> downstream) 
	{
		BiConsumer<A, ? super U> acc = downstream.accumulator();
		return Collector.of(downstream.supplier(), (a, t) -> {
			try (Stream<? extends U> s = mapper.apply(t)) 
			{
				if (s != null)
					s.forEachOrdered(u -> acc.accept(a, u));
			}
		}, downstream.combiner(), downstream.finisher(), downstream.characteristics().toArray(new Collector.Characteristics[0]));
	}
}
