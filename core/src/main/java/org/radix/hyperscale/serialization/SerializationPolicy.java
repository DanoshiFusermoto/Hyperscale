package org.radix.hyperscale.serialization;

import java.util.Map;
import java.util.Set;
import org.radix.hyperscale.serialization.DsonOutput.Output;

/**
 * Serialization policy that returns a set of classes and field names for a specified {@link
 * DsonOutput.Output} mode.
 */
public interface SerializationPolicy {

  /**
   * Retrieve the fields to output for the given output mode.
   *
   * @param output The output mode
   * @return The set of pairs of {@code Class<?>} and field names to output
   */
  Map<Class<?>, Set<String>> getIncludedFields(Output output);
}
