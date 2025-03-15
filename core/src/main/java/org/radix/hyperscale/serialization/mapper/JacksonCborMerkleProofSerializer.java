package org.radix.hyperscale.serialization.mapper;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.radix.hyperscale.crypto.MerkleProof;

public class JacksonCborMerkleProofSerializer extends StdSerializer<MerkleProof> {
  private static final ThreadLocal<ByteBuffer> byteBuffer =
      ThreadLocal.withInitial(() -> ByteBuffer.allocate(Byte.BYTES + MerkleProof.BYTES));

  private static final long serialVersionUID = 6716766456336170593L;

  JacksonCborMerkleProofSerializer() {
    this(null);
  }

  JacksonCborMerkleProofSerializer(Class<MerkleProof> t) {
    super(t);
  }

  @Override
  public void serialize(MerkleProof value, JsonGenerator jgen, SerializerProvider provider)
      throws IOException {
    final ByteBuffer buffer = byteBuffer.get();
    buffer.clear();
    buffer.put(JacksonCodecConstants.MERKLE_PROOF_VALUE);
    buffer.put((byte) value.getDirection().ordinal());
    buffer.put(value.getHash().toByteArray());
    jgen.writeBinary(buffer.array());
  }
}
