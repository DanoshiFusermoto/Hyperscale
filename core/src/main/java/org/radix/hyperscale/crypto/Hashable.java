package org.radix.hyperscale.crypto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.lang.reflect.Field;
import org.radix.hyperscale.common.UID;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.DsonOutput.Output;
import org.radix.hyperscale.serialization.Serialization;

public interface Hashable {
  default UID getUID() {
    final long prime = 31;
    long result = 1;

    Field[] fields = getClass().getDeclaredFields();
    for (Field field : fields) {
      JsonProperty sid = field.getAnnotation(JsonProperty.class);
      if (sid == null) continue;

      DsonOutput outputs = field.getAnnotation(DsonOutput.class);
      if (outputs == null) continue;

      boolean include = false;
      for (Output output : outputs.value()) {
        if (output.equals(Output.ALL) || output.equals(Output.HASH)) {
          include = true;
          break;
        }
      }

      if (include == false) continue;

      result = prime * result + field.hashCode();
    }

    return new UID(result);
  }

  @JsonProperty("hash")
  @DsonOutput(Output.API)
  default Hash getHash() {
    try {
      byte[] hashBytes = Serialization.getInstance().toDson(this, Output.HASH);
      return Hash.hash(hashBytes);
    } catch (Exception e) {
      throw new RuntimeException("Error generating hash: " + e, e);
    }
  }
}
