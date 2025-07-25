package org.radix.hyperscale.serialization.mapper;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.function.Function;

import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.crypto.Identity;
import org.radix.hyperscale.crypto.Signature;
import org.radix.hyperscale.crypto.bls12381.BLSSignature;
import org.radix.hyperscale.crypto.ed25519.EDSignature;
import org.radix.hyperscale.crypto.merkle.MerkleProof;
import org.radix.hyperscale.ledger.ShardGroupID;
import org.radix.hyperscale.ledger.ShardID;
import org.radix.hyperscale.ledger.StateAddress;
import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;
import org.radix.hyperscale.serialization.ByteBufferOutputStream;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.Polymorphic;
import org.radix.hyperscale.serialization.Serializable;
import org.radix.hyperscale.serialization.Serialization;
import org.radix.hyperscale.serialization.SerializerConstants;
import org.radix.hyperscale.serialization.SerializerDummy;
import org.radix.hyperscale.serialization.SerializerIds;
import org.radix.hyperscale.utils.UInt128;
import org.radix.hyperscale.utils.UInt256;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.util.ByteArrayBuilder;
import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.DeserializationConfig;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.BeanDeserializerModifier;
import com.fasterxml.jackson.databind.deser.std.DelegatingDeserializer;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.deser.std.UntypedObjectDeserializer;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.BinaryNode;
import com.fasterxml.jackson.databind.ser.BeanSerializerModifier;
import com.fasterxml.jackson.databind.ser.FilterProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.fasterxml.jackson.datatype.eclipsecollections.EclipseCollectionsModule;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsonorg.JsonOrgModule;

/**
 * A Jackson {@link ObjectMapper} that will serialize and deserialize
 * to the subset of <a href="http://cbor.io/">CBOR</a> that DSON uses.
 */
public class JacksonCborMapper extends ObjectMapper 
{
	private static final Logger serializerlog = Logging.getLogger("serializer");

	private static final long serialVersionUID = 4917479892309630214L;
	
	private final DsonOutput.Output dsonOutput;

	private JacksonCborMapper(DsonOutput.Output dsonOutput) 
	{
		super(createCborFactory());
		
		this.dsonOutput = dsonOutput;
	}
	
	DsonOutput.Output getDsonOutput()
	{
		return this.dsonOutput;
	}

	private static JsonFactory createCborFactory() 
	{
		return new RadixCBORFactory();
	}
	
	@Override
	public byte[] writeValueAsBytes(Object value) throws JsonProcessingException
	{
        ByteArrayBuilder bb = new ByteArrayBuilder(_jsonFactory._getBufferRecycler(), 1000);
        try 
        {
            _configAndWriteValue(_jsonFactory.createJsonGenerator(bb, JsonEncoding.UTF8), value);
        } 
        catch (JsonProcessingException e) 
        {
            throw e;
        } 
        catch (IOException e) 
        {
            throw JsonMappingException.fromUnexpectedIOE(e);
        }
        
        byte[] result = bb.toByteArray();
        bb.release();
        return result;
	}

	private static final JsonDeserializer<Object> untypedDeserializer = new JsonDeserializer<Object>() 
	{
		@Override
		public Object deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException 
		{
	        final JsonNode object;
	        final JsonToken token = jp.getCurrentToken();
			if (token.isStructStart())
			{
		        object = jp.readValueAsTree();
		        final JsonNode typeNameNodeOrNull = object.get(SerializerConstants.SERIALIZER_TYPE_NAME);
		        if (typeNameNodeOrNull != null) 
		        {
					final ObjectCodec codec = jp.getCodec();
		            final String typeName = typeNameNodeOrNull.asText();
		            final Class<?> typeClass = Serialization.getInstance().getClassForId(typeName);
		            return object.traverse(codec).readValueAs(typeClass);
		        }
		        else if (object.isArray())
		        {
					final ObjectCodec codec = jp.getCodec();
		        	return object.traverse(codec).readValueAs(ArrayList.class);
		        }
		        else 
		        {
		        	final ObjectCodec codec = jp.getCodec();
	                Map<Object, Object> resultMap = new TreeMap<>();
	                StandardKeyDeserializer keyDeserializer = new StandardKeyDeserializer();
	                Iterator<Entry<String, JsonNode>> iterator = object.fields();
	                while(iterator.hasNext())
	                {
	                	Entry<String, JsonNode> entry = iterator.next();
	                    try 
	                    {
	                        // Try to deserialize the key based on its format
	                        Object key = keyDeserializer.deserializeKey(entry.getKey(), ctxt);
	                        // Deserialize the value using the codec
	                        Object value = entry.getValue().traverse(codec).readValueAs(Object.class);
	                        resultMap.put(key, value);
	                    } 
	                    catch (IOException e) 
	                    {
	                        throw new RuntimeException("Failed to deserialize map entry", e);
	                    }
	                }

	                return resultMap;
		        }
			}
			else if (token.equals(JsonToken.VALUE_EMBEDDED_OBJECT))
			{
				object = jp.readValueAs(BinaryNode.class);

				byte[] bytes = object.binaryValue();
				if (bytes[0] == JacksonCodecConstants.HASH_VALUE)
					return Hash.from(bytes, 1);
				else if (bytes[0] == JacksonCodecConstants.U20_VALUE)
					return UInt256.from(bytes, 1);
				else if (bytes[0] == JacksonCodecConstants.MERKLE_PROOF_VALUE)
					return MerkleProof.from(bytes, 1);
				else if (bytes[0] == JacksonCodecConstants.SHARD_ID_VALUE)
					return ShardID.from(bytes, 1);
				else if (bytes[0] == JacksonCodecConstants.STATE_ADDRESS_VALUE)
					return StateAddress.from(bytes, 1);
				else if (bytes[0] == JacksonCodecConstants.SHARD_GROUP_ID_VALUE)
					return ShardGroupID.from(bytes, 1);
				else if (bytes[0] == JacksonCodecConstants.IDENTITY_VALUE)
				{
					try 
					{
						return Identity.from(bytes, 1);
					} 
					catch (Exception cex) 
					{
						throw new IOException(cex);
					}
				}
				else if (bytes[0] == JacksonCodecConstants.BYTES_VALUE)
					return Arrays.copyOfRange(bytes, 1, bytes.length);
				else
					return bytes;
			}
			else
			{
		        object = jp.readValueAsTree();
		        
		        if (object.isBoolean())
		        	return object.booleanValue();
		        else if (object.isLong())
		        	return object.longValue();
		        else if (object.isInt())
		        	return object.intValue();
		        else if (object.isFloat())
		        	return object.floatValue();
		        else if (object.isDouble())
		        	return object.doubleValue();
		        else if (object.isTextual())
		        	return object.asText();
		        else
		        	throw new IllegalArgumentException();
			}
		}
	};


	/**
	 * Create an {@link ObjectMapper} that will serialize to/from
	 * CBOR encoded DSON.
	 *
	 * @param idLookup A {@link SerializerIds} used to perform serializer ID lookup
	 * @param filterProvider A {@link FilterProvider} to use for filtering serialized fields
	 * @return A freshly created {@link JacksonCborMapper}
	 */
	public static JacksonCborMapper create(DsonOutput.Output output, SerializerIds idLookup, FilterProvider filterProvider) 
	{
		SimpleModule cborModule = new SimpleModule();

		cborModule.addSerializer(SerializerDummy.class, new JacksonSerializerDummySerializer(idLookup));
		cborModule.addSerializer(String.class, new JacksonJsonStringSerializer());
		cborModule.addSerializer(ShardGroupID.class, new JacksonCborShardGroupIDSerializer());
		cborModule.addSerializer(StateAddress.class, new JacksonCborStateAddressSerializer());
		cborModule.addSerializer(Identity.class, new JacksonCborIdentitySerializer());
		cborModule.addSerializer(MerkleProof.class, new JacksonCborMerkleProofSerializer());
		
		cborModule.addSerializer(Hash.class, new JacksonCborObjectBytesSerializer<>(
			Hash.class,
			JacksonCodecConstants.HASH_VALUE,
			Hash::toByteArray
		));
		cborModule.addSerializer(byte[].class, new JacksonCborObjectBytesSerializer<>(
			byte[].class,
			JacksonCodecConstants.BYTES_VALUE,
			Function.identity()
		));
		cborModule.addSerializer(UInt128.class, new JacksonCborObjectBytesSerializer<>(
			UInt128.class,
			JacksonCodecConstants.U10_VALUE,
			UInt128::toByteArray
		));
		cborModule.addSerializer(UInt256.class, new JacksonCborObjectBytesSerializer<>(
			UInt256.class,
			JacksonCodecConstants.U20_VALUE,
			UInt256::toByteArray
		));
		cborModule.addSerializer(ShardID.class, new JacksonCborObjectBytesSerializer<>(
			ShardID.class,
			JacksonCodecConstants.SHARD_ID_VALUE,
			ShardID::toByteArray
		));
		
		cborModule.addSerializer(EDSignature.class, new JacksonCborObjectBytesSerializer<>(
			EDSignature.class,
			JacksonCodecConstants.EC_SIGNATURE_VALUE,
			EDSignature::toByteArray
		));

		cborModule.addSerializer(BLSSignature.class, new JacksonCborObjectBytesSerializer<>(
			BLSSignature.class,
			JacksonCodecConstants.BLS_SIGNATURE_VALUE,
			BLSSignature::toByteArray
		));

		cborModule.addKeySerializer(Hash.class, new StandardKeySerializer());
		cborModule.addKeySerializer(String.class, new StandardKeySerializer());
		cborModule.addKeySerializer(ShardID.class, new StandardKeySerializer());
		cborModule.addKeySerializer(ShardGroupID.class, new StandardKeySerializer());
		cborModule.addKeySerializer(Identity.class, new StandardKeySerializer());
		cborModule.addKeySerializer(UInt256.class, new StandardKeySerializer());

		cborModule.addDeserializer(SerializerDummy.class, new JacksonSerializerDummyDeserializer());
		cborModule.addDeserializer(String.class, new JacksonJsonStringDeserializer());
		cborModule.addDeserializer(Identity.class, new JacksonCborIdentityDeserializer());
		cborModule.addDeserializer(ShardGroupID.class, new JacksonCborShardGroupIDDeserializer());
		cborModule.addDeserializer(MerkleProof.class, new JacksonCborMerkleProofDeserializer());
		cborModule.addDeserializer(Hash.class, new JacksonCborHashDeserializer());
		cborModule.addDeserializer(ShardID.class, new JacksonCborShardIDDeserializer());
		cborModule.addDeserializer(UInt256.class, new JacksonCborUInt256Deserializer());
		cborModule.addDeserializer(StateAddress.class, new JacksonCborStateAddressDeserializer());

		cborModule.addDeserializer(byte[].class, new JacksonCborObjectBytesDeserializer<>(
			byte[].class,
			JacksonCodecConstants.BYTES_VALUE,
			Function.identity()
		));
		cborModule.addDeserializer(UInt128.class, new JacksonCborObjectBytesDeserializer<>(
			UInt128.class,
			JacksonCodecConstants.U10_VALUE,
			UInt128::from
		));
		
		cborModule.addDeserializer(EDSignature.class, new JacksonCborObjectBytesDeserializer<>(
			EDSignature.class,
			JacksonCodecConstants.EC_SIGNATURE_VALUE,
			EDSignature::from
		));

		cborModule.addDeserializer(BLSSignature.class, new JacksonCborObjectBytesDeserializer<>(
			BLSSignature.class,
			JacksonCodecConstants.BLS_SIGNATURE_VALUE,
			BLSSignature::from
		));

		// Special case for abstract signature references
		cborModule.addDeserializer(Signature.class, new StdDeserializer<>(Signature.class) 
		{
			@Override
			public Signature deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JacksonException
			{
				byte[] bytes = p.getBinaryValue();
				if (bytes == null || bytes.length == 0)
					throw new InvalidFormatException(p, "Expecting signature bytes but null or empty", bytes, handledType());
				
				if (bytes[0] == JacksonCodecConstants.EC_SIGNATURE_VALUE)
					return EDSignature.from(bytes, 1);
				else if (bytes[0] == JacksonCodecConstants.BLS_SIGNATURE_VALUE)
					return BLSSignature.from(bytes, 1);
				else
					throw new InvalidFormatException(p, "Unexpected prefix "+bytes[0], bytes, handledType());
			}
		});

		cborModule.addKeyDeserializer(Hash.class, new StandardKeyDeserializer());
		cborModule.addKeyDeserializer(String.class, new StandardKeyDeserializer());
		cborModule.addKeyDeserializer(ShardID.class, new StandardKeyDeserializer());
		cborModule.addKeyDeserializer(ShardGroupID.class, new StandardKeyDeserializer());
		cborModule.addKeyDeserializer(Identity.class, new StandardKeyDeserializer());
		cborModule.addKeyDeserializer(UInt256.class, new StandardKeyDeserializer());
		
		// Modifier for untyped objects
		cborModule.setDeserializerModifier(new BeanDeserializerModifier() 
		{
			@Override
			@SuppressWarnings("rawtypes")
			// Special modifier for Enum values to remove :str: leadin from front
			public JsonDeserializer<Enum> modifyEnumDeserializer(DeserializationConfig config, final JavaType type, BeanDescription beanDesc, final JsonDeserializer<?> deserializer) 
			{
				return new JsonDeserializer<Enum>() 
				{
					@Override
					@SuppressWarnings("unchecked")
					public Enum deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException 
					{
						Class<? extends Enum> rawClass = (Class<Enum<?>>) type.getRawClass();
						return Enum.valueOf(rawClass, jp.getValueAsString());
					}
				};
			}

			@Override
			public JsonDeserializer<?> modifyDeserializer(DeserializationConfig config, BeanDescription beanDesc, JsonDeserializer<?> deserializer) 
			{
				if ((deserializer instanceof UntypedObjectDeserializer) == false)
				{
					if (beanDesc.getType().isPrimitive())
						return deserializer;

					if (Polymorphic.class.isAssignableFrom(beanDesc.getBeanClass()))
						return deserializer;
					
					if (beanDesc.getType().isInterface() == false && beanDesc.getType().isAbstract() == false)
						return new DeserializerWithTrigger(deserializer);
				}
				
				return untypedDeserializer;
			}
		});
		
		// Modifier for extracting byte array output of Serializable objects
		cborModule.setSerializerModifier(new BeanSerializerModifier() {

			@Override
			@SuppressWarnings("unchecked")
			public JsonSerializer<?> modifySerializer(SerializationConfig config, BeanDescription beanDesc, JsonSerializer<?> serializer) 
			{
				Class<?> clazz = beanDesc.getType().getRawClass();
				if (clazz != null && Serializable.class.isAssignableFrom(clazz))
					return new SerializerCachedOutput((Class<Serializable>)clazz, serializer);
				
				return super.modifySerializer(config, beanDesc, serializer);
			}
		});

		JacksonCborMapper mapper = new JacksonCborMapper(output);
		mapper.registerModule(cborModule);
	    mapper.registerModule(new JsonOrgModule());
	    mapper.registerModule(new GuavaModule());
	    mapper.registerModule(new EclipseCollectionsModule());
	    mapper.registerModule(new Jdk8Module());
	    
		mapper.configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true);
		mapper.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
		mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		mapper.setVisibility(mapper.getSerializationConfig().getDefaultVisibilityChecker()
                .withFieldVisibility(JsonAutoDetect.Visibility.NONE)
                .withGetterVisibility(JsonAutoDetect.Visibility.NONE)
                .withSetterVisibility(JsonAutoDetect.Visibility.NONE)
                .withCreatorVisibility(JsonAutoDetect.Visibility.PUBLIC_ONLY));
		mapper.setSerializationInclusion(Include.NON_NULL);
		mapper.setFilterProvider(filterProvider);
		mapper.setAnnotationIntrospector(new DsonFilteringIntrospector());
	    mapper.setDefaultTyping(new DsonTypeResolverBuilder(idLookup));
		return mapper;
	}
}

@SuppressWarnings("rawtypes")
class SerializerCachedOutput extends StdSerializer<Serializable> 
{
	private static final Logger serializerlog = Logging.getLogger("serializer");

	private static final long serialVersionUID = -1442781576335436898L;
	
	private final JsonSerializer delegate;
	
	public SerializerCachedOutput(final Class<Serializable> clazz, final JsonSerializer<?> delegate) 
	{
        super(clazz);
        
        this.delegate = delegate;
    }

	@SuppressWarnings("unchecked")
	@Override
	public void serialize(final Serializable bean, final JsonGenerator gen, final SerializerProvider provider) throws IOException 
	{
		boolean serialized = false;
		if (gen instanceof RadixCBORGenerator rgen)
		{
			if (rgen.getCodec() instanceof JacksonCborMapper jcm && (jcm.getDsonOutput() == DsonOutput.Output.WIRE || jcm.getDsonOutput() == DsonOutput.Output.PERSIST))
			{
				// Synchronized to prevent "overlapping" serializations 
				synchronized(bean)
				{
					ByteBuffer cachedOutput = bean.getCachedDsonOutput();
					if (cachedOutput != null)
					{
						rgen.writeStartObject();
						rgen.writeBytes(cachedOutput.array(), 1, cachedOutput.limit()-2);
						rgen.writeEndObject();
						return;
					}
	
					if (bean.shouldCacheDsonOutput())
					{
						int sectionStart = 0;
						int sectionEnd = 0;
						int sectionLength = 0;					
						
						if (gen.getOutputTarget() instanceof ByteArrayBuilder bab)
						{
							sectionStart = bab.size() + rgen._outputTail;
							this.delegate.serialize(bean, gen, provider);
							sectionEnd = bab.size() + rgen._outputTail;
							sectionLength = sectionEnd - sectionStart;
							serialized = true;
						}
						else if (gen.getOutputTarget() instanceof ByteBufferOutputStream bbos)
						{
							sectionStart = bbos.getByteBuffer().position() + rgen._outputTail;
							this.delegate.serialize(bean, gen, provider);
							sectionEnd = bbos.getByteBuffer().position() + rgen._outputTail;
							sectionLength = sectionEnd - sectionStart;
							serialized = true;
						}
	
						if (serialized)
						{
							try
							{
								if (sectionLength > 0 && rgen._outputTail - sectionLength >= 0)
								{
									try
									{
										cachedOutput = Serialization.bufferPool.acquire(bean, sectionLength);
										cachedOutput.put(rgen._outputBuffer, (rgen._outputTail - sectionLength), sectionLength);
										cachedOutput.flip();
									}
									catch (BufferOverflowException boex)
									{
										serializerlog.fatal("buffer.capacity="+cachedOutput.capacity()+", buffer.limit="+cachedOutput.limit()+", rgen._outputTail="+rgen._outputTail+", sectionstart="+sectionStart+", sectionend="+sectionEnd+", sectionlength="+sectionLength, boex);
										throw boex;
									}
									
									bean.setCachedDsonOutput(cachedOutput);
									Serialization.getInstance().statistics(bean.getClass().getSimpleName(), sectionLength);
								}
								else if (serializerlog.hasLevel(Logging.DEBUG))
									serializerlog.warn("Did not extract dson output for caching on object "+bean);
							}
							catch (Exception ex)
							{
								throw ex;
							}
						}
					}
					else
						serialized = false;
				}
			}
		}
		
		// No special serialization path available 
		if (serialized == false)
		{
			this.delegate.serialize(bean, gen, provider);
			Serialization.getInstance().statistics(bean.getClass().getSimpleName(), 0);
		}
	}
}

class DeserializerWithTrigger extends DelegatingDeserializer 
{
	private static final long serialVersionUID = -6643494127590363539L;

	public DeserializerWithTrigger(JsonDeserializer<?> delegate) 
	{
        super(delegate);
    }

    @Override
    protected JsonDeserializer<?> newDelegatingInstance(JsonDeserializer<?> delegate) 
    {
        return new DeserializerWithTrigger(delegate);
    }

    @Override
    public Object deserialize(JsonParser p, DeserializationContext ctxt) throws IOException 
    {
        final Object object = super.deserialize(p, ctxt);
        
        if (object instanceof Serializable serializable)
        	serializable.onDeserialized();
    
        return object;
    }
}