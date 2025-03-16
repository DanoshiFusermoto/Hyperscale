package org.radix.hyperscale.serialization.mapper;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.DeserializationConfig;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.KeyDeserializer;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.BeanDeserializerModifier;
import com.fasterxml.jackson.databind.deser.std.UntypedObjectDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.FilterProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.fasterxml.jackson.datatype.eclipsecollections.EclipseCollectionsModule;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsonorg.JsonOrgModule;
import java.io.IOException;
import java.util.ArrayList;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.crypto.Identity;
import org.radix.hyperscale.crypto.MerkleProof;
import org.radix.hyperscale.crypto.bls12381.BLSSignature;
import org.radix.hyperscale.crypto.ed25519.EDSignature;
import org.radix.hyperscale.ledger.ShardGroupID;
import org.radix.hyperscale.ledger.ShardID;
import org.radix.hyperscale.ledger.StateAddress;
import org.radix.hyperscale.serialization.Serialization;
import org.radix.hyperscale.serialization.SerializerConstants;
import org.radix.hyperscale.serialization.SerializerDummy;
import org.radix.hyperscale.serialization.SerializerIds;
import org.radix.hyperscale.utils.Bytes;
import org.radix.hyperscale.utils.UInt256;

/**
 * A Jackson {@link ObjectMapper} that will serialize and deserialize to the JSON in the format that
 * Radix requires.
 */
public class JacksonJsonMapper extends ObjectMapper {
  private static final long serialVersionUID = 4917479892309630214L;

  private JacksonJsonMapper() {
    super(createJsonFactory());
  }

  private static JsonFactory createJsonFactory() {
    return new JsonFactory();
  }

  /**
   * Create an {@link ObjectMapper} that will serialize to/from the JSON format that radix requires.
   *
   * @param idLookup A {@link SerializerIds} used to perform serializer ID lookup
   * @param filterProvider A {@link FilterProvider} to use for filtering serialized fields
   * @param sortProperties {@code true} if JSON output properties should be sorted in
   *     lexicographical order
   * @return A freshly created {@link JacksonJsonMapper}
   */
  public static JacksonJsonMapper create(
      SerializerIds idLookup, FilterProvider filterProvider, boolean sortProperties) {
    SimpleModule jsonModule = new SimpleModule();
    jsonModule.addSerializer(Hash.class, new JacksonJsonHashSerializer());
    jsonModule.addSerializer(byte[].class, new JacksonJsonBytesSerializer());
    jsonModule.addSerializer(String.class, new JacksonJsonStringSerializer());
    jsonModule.addSerializer(ShardGroupID.class, new JacksonJsonShardGroupIDSerializer());
    jsonModule.addSerializer(StateAddress.class, new JacksonJsonStateAddressSerializer());
    jsonModule.addSerializer(SerializerDummy.class, new JacksonSerializerDummySerializer(idLookup));
    jsonModule.addSerializer(Identity.class, new JacksonJsonIdentitySerializer());
    jsonModule.addSerializer(
        ShardID.class,
        new JacksonJsonObjectStringSerializer<>(
            ShardID.class, JacksonCodecConstants.SHARD_ID_STR_VALUE, ShardID::toString));
    jsonModule.addSerializer(UInt256.class, new JacksonJsonUInt256Serializer());
    jsonModule.addSerializer(
        MerkleProof.class,
        new JacksonJsonObjectStringSerializer<>(
            MerkleProof.class,
            JacksonCodecConstants.MERKLE_PROOF_STR_VALUE,
            MerkleProof::toString));
    jsonModule.addSerializer(
        BLSSignature.class,
        new JacksonJsonObjectStringSerializer<>(
            BLSSignature.class,
            JacksonCodecConstants.BLS_SIGNATURE_STR_VALUE,
            s -> Bytes.toHexString(s.toByteArray())));
    jsonModule.addSerializer(
        EDSignature.class,
        new JacksonJsonObjectStringSerializer<>(
            EDSignature.class,
            JacksonCodecConstants.EC_SIGNATURE_STR_VALUE,
            s -> Bytes.toHexString(s.toByteArray())));

    jsonModule.addKeySerializer(Hash.class, new StandardKeySerializer());
    jsonModule.addKeySerializer(String.class, new StandardKeySerializer());
    jsonModule.addKeySerializer(ShardID.class, new StandardKeySerializer());
    jsonModule.addKeySerializer(ShardGroupID.class, new StandardKeySerializer());
    jsonModule.addKeySerializer(Identity.class, new StandardKeySerializer());
    jsonModule.addKeySerializer(UInt256.class, new StandardKeySerializer());

    jsonModule.addDeserializer(Hash.class, new JacksonJsonHashDeserializer());
    jsonModule.addDeserializer(byte[].class, new JacksonJsonBytesDeserializer());
    jsonModule.addDeserializer(String.class, new JacksonJsonStringDeserializer());
    jsonModule.addDeserializer(ShardGroupID.class, new JacksonJsonShardGroupIDDeserializer());
    jsonModule.addDeserializer(StateAddress.class, new JacksonJsonStateAddressDeserializer());
    jsonModule.addDeserializer(SerializerDummy.class, new JacksonSerializerDummyDeserializer());
    jsonModule.addDeserializer(Identity.class, new JacksonJsonIdentityDeserializer());
    jsonModule.addDeserializer(ShardID.class, new JacksonJsonShardIDDeserializer());
    jsonModule.addDeserializer(UInt256.class, new JacksonJsonUInt256Deserializer());
    jsonModule.addDeserializer(
        MerkleProof.class,
        new JacksonJsonObjectStringDeserializer<>(
            MerkleProof.class, JacksonCodecConstants.MERKLE_PROOF_STR_VALUE, MerkleProof::from));
    jsonModule.addDeserializer(
        BLSSignature.class,
        new JacksonJsonObjectStringDeserializer<>(
            BLSSignature.class,
            JacksonCodecConstants.BLS_SIGNATURE_STR_VALUE,
            s -> BLSSignature.from(Bytes.fromHexString(s))));
    jsonModule.addDeserializer(
        EDSignature.class,
        new JacksonJsonObjectStringDeserializer<>(
            EDSignature.class,
            JacksonCodecConstants.EC_SIGNATURE_STR_VALUE,
            s -> EDSignature.from(Bytes.fromHexString(s))));

    jsonModule.addKeyDeserializer(Hash.class, new StandardKeyDeserializer());
    jsonModule.addKeyDeserializer(String.class, new StandardKeyDeserializer());
    jsonModule.addKeyDeserializer(ShardID.class, new StandardKeyDeserializer());
    jsonModule.addKeyDeserializer(ShardGroupID.class, new StandardKeyDeserializer());
    jsonModule.addKeyDeserializer(Identity.class, new StandardKeyDeserializer());
    jsonModule.addKeyDeserializer(UInt256.class, new StandardKeyDeserializer());

    // Special modifier for Enum values to remove :str: leadin from front
    jsonModule.setDeserializerModifier(
        new BeanDeserializerModifier() {
          @Override
          @SuppressWarnings("rawtypes")
          public JsonDeserializer<Enum> modifyEnumDeserializer(
              DeserializationConfig config,
              final JavaType type,
              BeanDescription beanDesc,
              final JsonDeserializer<?> deserializer) {
            return new JsonDeserializer<Enum>() {
              @Override
              @SuppressWarnings("unchecked")
              public Enum deserialize(JsonParser jp, DeserializationContext ctxt)
                  throws IOException {
                //						String name = jp.getValueAsString();
                //						if (!name.startsWith(JacksonCodecConstants.STR_STR_VALUE)) {
                //							throw new IllegalStateException(String.format("Expected value starting with
                // %s, found: %s", JacksonCodecConstants.STR_STR_VALUE, name));
                //						}
                Class<? extends Enum> rawClass = (Class<Enum<?>>) type.getRawClass();
                return Enum.valueOf(
                    rawClass,
                    jp.getValueAsString()); // .substring(JacksonCodecConstants.STR_VALUE_LEN));
              }
            };
          }
        });

    // Special modifier for untyped objects which may be carry a compact primitive type
    jsonModule.setDeserializerModifier(
        new BeanDeserializerModifier() {
          @Override
          public JsonDeserializer<?> modifyDeserializer(
              DeserializationConfig config,
              BeanDescription beanDesc,
              JsonDeserializer<?> deserializer) {
            if ((deserializer instanceof UntypedObjectDeserializer) == false) {
              if (beanDesc.getType().isPrimitive()) return deserializer;

              if (beanDesc.getType().isInterface() == false
                  && beanDesc.getType().isAbstract() == false) return deserializer;
            }

            return new JsonDeserializer<Object>() {
              @Override
              public Object deserialize(JsonParser jp, DeserializationContext ctxt)
                  throws IOException {
                final ObjectCodec codec = jp.getCodec();
                final JsonNode object = jp.readValueAsTree();
                final JsonNode typeNameNodeOrNull =
                    object.get(SerializerConstants.SERIALIZER_TYPE_NAME);
                if (typeNameNodeOrNull != null) {
                  final String typeName = typeNameNodeOrNull.asText();
                  final Class<?> typeClass = Serialization.getInstance().getClassForId(typeName);
                  return object.traverse(codec).readValueAs(typeClass);
                } else if (object.isNumber()) return object.asLong();
                else if (object.isArray())
                  return object.traverse(codec).readValueAs(ArrayList.class);

                String value = object.asText();
                if (value.startsWith(JacksonCodecConstants.HASH_STR_VALUE))
                  return Hash.from(value, JacksonCodecConstants.STR_VALUE_LEN);
                else if (value.startsWith(JacksonCodecConstants.U256_STR_VALUE))
                  return UInt256.from(value, JacksonCodecConstants.STR_VALUE_LEN);
                else if (value.startsWith(JacksonCodecConstants.MERKLE_PROOF_STR_VALUE))
                  return MerkleProof.from(value.substring(JacksonCodecConstants.STR_VALUE_LEN));
                else if (value.startsWith(JacksonCodecConstants.SHARD_ID_STR_VALUE))
                  return ShardID.from(value, JacksonCodecConstants.STR_VALUE_LEN);
                else if (value.startsWith(JacksonCodecConstants.SHARD_GROUP_ID_STR_VALUE))
                  return ShardGroupID.from(value.substring(JacksonCodecConstants.STR_VALUE_LEN));
                else if (value.startsWith(JacksonCodecConstants.IDENTITY_STR_VALUE)) {
                  try {
                    return Identity.from(value, JacksonCodecConstants.STR_VALUE_LEN);
                  } catch (Exception cex) {
                    throw new IOException(cex);
                  }
                } else return value;
              }
            };
          }
        });

    JacksonJsonMapper mapper = new JacksonJsonMapper();
    mapper.registerModule(jsonModule);
    mapper.registerModule(new JsonOrgModule());
    mapper.registerModule(new GuavaModule());
    mapper.registerModule(new EclipseCollectionsModule());
    mapper.registerModule(new Jdk8Module());

    mapper.configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, sortProperties);
    mapper.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, sortProperties);
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    mapper.setVisibility(
        mapper
            .getSerializationConfig()
            .getDefaultVisibilityChecker()
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

  /**
   * Create an {@link ObjectMapper} that will serialize to/from the JSON format that radix requires.
   *
   * @param idLookup A {@link SerializerIds} used to perform serializer ID lookup
   * @param filterProvider A {@link FilterProvider} to use for filtering serialized fields
   * @return A freshly created {@link JacksonJsonMapper} that does not sort properties
   * @see #create(SerializerIds, FilterProvider, boolean)
   */
  public static JacksonJsonMapper create(SerializerIds idLookup, FilterProvider filterProvider) {
    return create(idLookup, filterProvider, false);
  }
}

class StandardKeySerializer extends StdSerializer<Object> {
  private static final long serialVersionUID = -1221942136965520977L;

  protected StandardKeySerializer() {
    super(Object.class);
  }

  @Override
  public void serialize(Object value, JsonGenerator gen, SerializerProvider provider)
      throws IOException {
    if (value instanceof String) gen.writeFieldName(value.toString());
    else if (value instanceof Hash)
      gen.writeFieldName(JacksonCodecConstants.HASH_STR_VALUE + value.toString());
    else if (value instanceof Identity)
      gen.writeFieldName(JacksonCodecConstants.IDENTITY_STR_VALUE + value.toString());
    else if (value instanceof UInt256)
      gen.writeFieldName(JacksonCodecConstants.U256_STR_VALUE + value.toString());
    else if (value instanceof ShardID)
      gen.writeFieldName(JacksonCodecConstants.SHARD_ID_STR_VALUE + value.toString());
    else if (value instanceof ShardGroupID)
      gen.writeFieldName(JacksonCodecConstants.SHARD_GROUP_ID_STR_VALUE + value.toString());
    else throw new IOException("Unsupported key type " + value.getClass());
  }
}

class StandardKeyDeserializer extends KeyDeserializer {
  @Override
  public Object deserializeKey(String key, DeserializationContext ctxt) throws IOException {
    if (key.startsWith(JacksonCodecConstants.HASH_STR_VALUE))
      return Hash.from(key, JacksonCodecConstants.STR_VALUE_LEN);
    else if (key.startsWith(JacksonCodecConstants.U256_STR_VALUE))
      return UInt256.from(key, JacksonCodecConstants.STR_VALUE_LEN);
    else if (key.startsWith(JacksonCodecConstants.SHARD_ID_STR_VALUE))
      return ShardID.from(key, JacksonCodecConstants.STR_VALUE_LEN);
    else if (key.startsWith(JacksonCodecConstants.SHARD_GROUP_ID_STR_VALUE))
      return ShardGroupID.from(key.substring(JacksonCodecConstants.STR_VALUE_LEN));
    else if (key.startsWith(JacksonCodecConstants.IDENTITY_STR_VALUE)) {
      try {
        return Identity.from(key, JacksonCodecConstants.STR_VALUE_LEN);
      } catch (Exception cex) {
        throw new IOException(cex);
      }
    } else return key;
  }
}
