package org.radix.hyperscale.serialization;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.radix.hyperscale.serialization.DsonOutput.Output;

/**
 * Class that maintains a map of {@link DsonOutput.Output} types to a set of pairs of classes and
 * field/method names to output for that serialization type.
 *
 * <p>This {@link SerializationPolicy} operates by scanning a supplied list of classes.
 */
public abstract class ClassScanningSerializationPolicy implements SerializationPolicy {

  private final EnumMap<Output, Map<Class<?>, Set<String>>> outputs = new EnumMap<>(Output.class);

  /**
   * Scan for all classes with an {@code SerializerId} annotation. The entire classpath is scanned,
   * including JAR files.
   *
   * @param classes The list of classes to scan for serialization annotations
   * @throws IllegalStateException If issues with serialization configuration are found while
   *     scanning.
   */
  protected ClassScanningSerializationPolicy(Collection<Class<?>> classes) {
    Map<Output, Map<Class<?>, Set<String>>> tempOutputs = new EnumMap<>(Output.class);
    // These are the outputs we will be collecting.
    // ALL and NONE are replaced with the complete set and empty set respectively
    tempOutputs.put(Output.HASH, new HashMap<>());
    tempOutputs.put(Output.API, new HashMap<>());
    tempOutputs.put(Output.WIRE, new HashMap<>());
    tempOutputs.put(Output.PERSIST, new HashMap<>());

    // First fields
    for (Class<?> outerCls : classes) {
      for (Class<?> cls = outerCls; !Object.class.equals(cls); cls = cls.getSuperclass()) {
        for (Field field : cls.getDeclaredFields()) {
          DsonOutput dsonOutput = field.getDeclaredAnnotation(DsonOutput.class);
          JsonProperty jsonProperty = field.getDeclaredAnnotation(JsonProperty.class);
          if (dsonOutput == null && jsonProperty != null) {
            throw new IllegalStateException(
                String.format(
                    "Field %s#%s has a %s annotation, but no %s annotation",
                    outerCls.getName(),
                    field.getName(),
                    JsonProperty.class.getSimpleName(),
                    DsonOutput.class.getSimpleName()));
          }
          if (dsonOutput != null && jsonProperty == null) {
            throw new IllegalStateException(
                String.format(
                    "Field %s#%s has a %s annotation, but no %s annotation",
                    outerCls.getName(),
                    field.getName(),
                    DsonOutput.class.getSimpleName(),
                    JsonProperty.class.getSimpleName()));
          }
          if (dsonOutput != null && jsonProperty != null) {
            String fieldName = jsonProperty.value();
            for (DsonOutput.Output out :
                DsonOutput.Output.toEnumSet(dsonOutput.value(), dsonOutput.include())) {
              if (!tempOutputs
                  .get(out)
                  .computeIfAbsent(outerCls, k -> new HashSet<>())
                  .add(fieldName)) {
                throw new IllegalStateException(
                    String.format(
                        "Duplicate property %s in class %s", fieldName, outerCls.getName()));
              }
            }
          }
        }
        // Now methods
        for (Method method : cls.getDeclaredMethods()) {
          DsonOutput dsonOutput = method.getDeclaredAnnotation(DsonOutput.class);
          JsonProperty jsonProperty = method.getDeclaredAnnotation(JsonProperty.class);
          JsonAnyGetter jsonAnyGetter = method.getDeclaredAnnotation(JsonAnyGetter.class);
          if (dsonOutput == null && jsonProperty != null) {
            if (method.getParameterCount() == 1) {
              // Ignore setter
              continue;
            }
            throw new IllegalStateException(
                String.format(
                    "Method %s#%s has a %s annotation, but no %s annotation",
                    outerCls.getName(),
                    method.getName(),
                    JsonProperty.class.getSimpleName(),
                    DsonOutput.class.getSimpleName()));
          }
          if (dsonOutput != null && jsonProperty == null && jsonAnyGetter == null) {
            throw new IllegalStateException(
                String.format(
                    "Method %s#%s has a %s annotation, but no %s or %s annotation",
                    outerCls.getName(),
                    method.getName(),
                    DsonOutput.class.getSimpleName(),
                    JsonProperty.class.getSimpleName(),
                    JsonAnyGetter.class.getSimpleName()));
          }
          if (dsonOutput != null && jsonProperty != null) {
            String fieldName = jsonProperty.value();
            if (method.getParameterCount() != 0) {
              throw new IllegalStateException(
                  String.format(
                      "Property %s in class %s not a getter", fieldName, outerCls.getName()));
            }
            for (DsonOutput.Output out :
                DsonOutput.Output.toEnumSet(dsonOutput.value(), dsonOutput.include())) {
              if (!tempOutputs
                  .get(out)
                  .computeIfAbsent(outerCls, k -> new HashSet<>())
                  .add(fieldName)) {
                throw new IllegalStateException(
                    String.format(
                        "Duplicate property %s in class %s", fieldName, outerCls.getName()));
              }
            }
          }
          if (dsonOutput != null && jsonAnyGetter != null) {
            DsonAnyProperties properties = method.getDeclaredAnnotation(DsonAnyProperties.class);
            if (properties == null) {
              throw new IllegalStateException(
                  String.format(
                      "Found %s annotation without %s annotation in class %s",
                      JsonAnyGetter.class.getSimpleName(),
                      DsonAnyProperties.class.getSimpleName(),
                      cls.getName()));
            }
            Set<String> fieldNames =
                Arrays.stream(properties.value())
                    .collect(Collectors.toSet()); // ImmutableSet.copyOf(properties.value());
            for (DsonOutput.Output out :
                DsonOutput.Output.toEnumSet(dsonOutput.value(), dsonOutput.include())) {
              Set<String> fields =
                  tempOutputs.get(out).computeIfAbsent(outerCls, k -> new HashSet<>());
              for (String fieldName : fieldNames) {
                if (!fields.add(fieldName)) {
                  throw new IllegalStateException(
                      String.format(
                          "Duplicate property %s in class %s", fieldName, outerCls.getName()));
                }
              }
            }
          }
        }
      }
    }
    Map<Output, Map<Class<?>, Set<String>>> newOutputs = new EnumMap<>(Output.class);
    Map<Class<?>, Set<String>> classFields = new HashMap<>();
    for (Map.Entry<Output, Map<Class<?>, Set<String>>> output : tempOutputs.entrySet()) {
      newOutputs.put(output.getKey(), toImmutableMap(output.getValue()));
      for (Map.Entry<Class<?>, Set<String>> fields : output.getValue().entrySet()) {
        classFields
            .computeIfAbsent(fields.getKey(), k -> new HashSet<>())
            .addAll(fields.getValue());
      }
    }
    List<String> classesWithMissingSerializer =
        classFields.entrySet().stream()
            .filter(e -> !e.getValue().contains(SerializerConstants.SERIALIZER_TYPE_NAME))
            .map(Map.Entry::getKey)
            .map(Class::getName)
            .sorted()
            .collect(Collectors.toList());
    if (!classesWithMissingSerializer.isEmpty()) {
      throw new IllegalStateException(
          String.format(
              "The following class%s missing the '%s' field: %s",
              classesWithMissingSerializer.size() == 1 ? " is" : "es are",
              SerializerConstants.SERIALIZER_TYPE_NAME,
              String.join(", ", classesWithMissingSerializer)));
    }
    outputs.putAll(newOutputs);
  }

  @Override
  public Map<Class<?>, Set<String>> getIncludedFields(Output output) {
    Map<Class<?>, Set<String>> includedFields = outputs.get(output);
    if (includedFields == null) {
      throw new IllegalArgumentException("No such output selection: " + output);
    }
    return includedFields;
  }

  private static Map<Class<?>, Set<String>> toImmutableMap(Map<Class<?>, Set<String>> value) {
    Map<Class<?>, Set<String>> mapBuilder = new HashMap<>();
    for (Map.Entry<Class<?>, Set<String>> e : value.entrySet())
      mapBuilder.put(e.getKey(), new HashSet<>(e.getValue()));
    return mapBuilder;
  }
}
