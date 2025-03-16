package org.radix.hyperscale.network.messages;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;
import org.radix.hyperscale.Universe;
import org.radix.hyperscale.common.Direction;
import org.radix.hyperscale.crypto.CryptoException;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.crypto.Hashable;
import org.radix.hyperscale.crypto.ed25519.EDSignature;
import org.radix.hyperscale.network.AbstractConnection;
import org.radix.hyperscale.network.TransportParameters;
import org.radix.hyperscale.network.exceptions.BanException;
import org.radix.hyperscale.serialization.ByteBufferOutputStream;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.DsonOutput.Output;
import org.radix.hyperscale.serialization.Serializable;
import org.radix.hyperscale.serialization.Serialization;
import org.radix.hyperscale.serialization.SerializerId2;
import org.radix.hyperscale.time.Time;
import org.xerial.snappy.Snappy;

public abstract class Message extends Serializable implements Hashable {
  public static final int HEADER_SIZE = Integer.BYTES + Byte.BYTES + Short.BYTES;
  public static final int PAYLOAD_BUFFER_SIZE = 1 << 20;
  public static final int COMPRESSION_BUFFER_SIZE = 1 << 20;
  public static final int MAX_MESSAGE_SIZE = 1 << 24; // 16MB max payload size
  public static final int MAX_SIGNATURE_SIZE = 1 << 14; // 16KB max signature size

  private static final ThreadLocal<ByteBuffer> headerBuffer =
      ThreadLocal.withInitial(() -> ByteBuffer.allocate(HEADER_SIZE));
  private static final ThreadLocal<ByteBuffer> payloadBuffer =
      ThreadLocal.withInitial(() -> ByteBuffer.allocate(PAYLOAD_BUFFER_SIZE));
  private static final ThreadLocal<ByteBuffer> compressionBuffer =
      ThreadLocal.withInitial(() -> ByteBuffer.allocate(COMPRESSION_BUFFER_SIZE));
  private static final ThreadLocal<ByteBufferOutputStream> payloadOutputStream =
      ThreadLocal.withInitial(
          () -> new ByteBufferOutputStream(Message.payloadBuffer.get().clear(), true));
  public static final boolean ALWAYS_COMPRESS =
      false; // FIXME weird issue with hashes / signatures when compressed

  private static final AtomicLong nextSeq = new AtomicLong(0);

  /*
   * payloadlength 	= int
   * signaturelength 	= short
   * signature		= byte[n]
   * payload			= byte[n]
   */
  public static Message inbound(DataInputStream dataInputStream, AbstractConnection connection)
      throws IOException, BanException, CryptoException {
    final ByteBuffer headerBuffer = Message.headerBuffer.get();
    headerBuffer.clear();

    // Read header //
    dataInputStream.readFully(headerBuffer.array());

    // Extract message length
    final int payloadLength = headerBuffer.getInt();
    if (payloadLength > Message.MAX_MESSAGE_SIZE)
      throw new IOException("Message payload size " + payloadLength + " is too large");

    // Payload is compressed?
    final boolean payloadCompressed = headerBuffer.get() == 0 ? false : true;

    // Extract signature length
    final short signatureLength = headerBuffer.getShort();
    if (signatureLength > Message.MAX_SIGNATURE_SIZE)
      throw new IOException("Message signature size " + signatureLength + " is too large");

    // Read signature if there is one
    final byte[] signatureBytes;
    if (signatureLength > 0) {
      signatureBytes = new byte[signatureLength];
      dataInputStream.readFully(signatureBytes);
    } else signatureBytes = null;

    // Read payload, decompress if required
    final ByteBuffer payloadBuffer;
    if (payloadCompressed) {
      final ByteBuffer compressionBuffer =
          payloadLength > COMPRESSION_BUFFER_SIZE
              ? ByteBuffer.allocate(payloadLength)
              : Message.compressionBuffer.get().clear();
      dataInputStream.readFully(compressionBuffer.array(), 0, payloadLength);
      final int uncompressedLength =
          Snappy.uncompressedLength(compressionBuffer.array(), 0, payloadLength);
      payloadBuffer =
          uncompressedLength > PAYLOAD_BUFFER_SIZE
              ? ByteBuffer.allocate(uncompressedLength)
              : Message.payloadBuffer.get().clear();
      Snappy.uncompress(compressionBuffer.array(), 0, payloadLength, payloadBuffer.array(), 0);
      payloadBuffer.position(uncompressedLength);
    } else {
      payloadBuffer =
          payloadLength > PAYLOAD_BUFFER_SIZE
              ? ByteBuffer.allocate(payloadLength)
              : Message.payloadBuffer.get();
      dataInputStream.readFully(payloadBuffer.array(), 0, payloadLength);
      payloadBuffer.position(payloadLength);
    }

    // Deserialize message
    final Message message =
        Serialization.getInstance()
            .fromDson(payloadBuffer.array(), 0, payloadBuffer.position(), Message.class);
    if (message.getMagic() != Universe.getDefault().getMagic())
      throw new BanException("Wrong magic for this deployment");

    message.setDirection(Direction.INBOUND);
    message.setSize(Message.HEADER_SIZE + signatureLength + payloadLength);

    if (connection.isHandshaked()) {
      if (connection.requiresSignatures() || message.requiresSignature()) {
        final Hash payloadHash = Hash.hash(payloadBuffer.array(), 0, payloadLength);
        final EDSignature signature =
            Serialization.getInstance().fromDson(signatureBytes, EDSignature.class);

        if (connection.getEphemeralRemotePublicKey().verify(payloadHash, signature) == false)
          throw new CryptoException(
              payloadHash
                  + ": Did not validate against connection key "
                  + connection.getEphemeralRemotePublicKey());
      }
    }

    return message;
  }

  public static void outbound(
      final Message message,
      final DataOutputStream dataOutputStream,
      final AbstractConnection connection)
      throws IOException, CryptoException {
    final ByteBuffer headerBuffer = Message.headerBuffer.get();
    headerBuffer.clear();

    byte[] payloadBytes = null;
    int payloadLength = 0;
    boolean payloadCompressed = false;
    byte[] signatureBytes = null;

    final ByteBuffer payloadBuffer;
    synchronized (message) {
      message.setDirection(Direction.OUTBOUND);

      payloadBytes = message.getCachedPayload();
      if (payloadBytes == null) {
        final ByteBufferOutputStream payloadOutputStream = Message.payloadOutputStream.get();
        payloadOutputStream.reset();

        Serialization.getInstance().toDson(payloadOutputStream, message, Output.WIRE);

        payloadBuffer = payloadOutputStream.getByteBuffer();
        if (payloadBuffer.position() > Message.MAX_MESSAGE_SIZE)
          throw new IOException(
              "Message payload size " + payloadBuffer.position() + " is too large");

        final TransportParameters transportParameters =
            message.getClass().getAnnotation(TransportParameters.class);
        if (transportParameters != null && transportParameters.cache()) {
          payloadBytes = new byte[payloadBuffer.position()];
          payloadLength = payloadBytes.length;

          System.arraycopy(payloadBuffer.array(), 0, payloadBytes, 0, payloadBuffer.position());
          message.setCachedPayload(payloadBytes);
          message.setCachedPayloadCompressed(payloadCompressed);
        } else {
          payloadBytes = payloadBuffer.array();
          payloadLength = payloadBuffer.position();
        }

        /*				if (ALWAYS_COMPRESS || payloadBuffer.position() > Constants.COMPRESS_PAYLOADS_THRESHOLD)
        {
        	payloadCompressed = true;
        	final int maxCompressedLength = Snappy.maxCompressedLength(payloadBuffer.position());
        	final ByteBuffer compressionBuffer = maxCompressedLength > COMPRESSION_BUFFER_SIZE ? ByteBuffer.allocate(maxCompressedLength) : Message.compressionBuffer.get().clear();

        	final int compressedLength = Snappy.compress(payloadBuffer.array(), 0, payloadBuffer.position(), compressionBuffer.array(), 0);
        	payloadBytes = new byte[compressedLength];
        	System.arraycopy(compressionBuffer.array(), 0, payloadBytes, 0, compressedLength);
        }
        else
        {
        	payloadBytes = new byte[payloadBuffer.position()];
        	System.arraycopy(payloadBuffer.array(), 0, payloadBytes, 0, payloadBuffer.position());
        }

        message.setCachedPayload(payloadBytes);
        message.setCachedPayloadCompressed(payloadCompressed);*/
      } else {
        payloadLength = payloadBytes.length;
        payloadCompressed = message.isCachedPayloadCompressed();
      }

      if (connection.requiresSignatures() || message.requiresSignature()) {
        signatureBytes = message.getCachedSignature();
        if (signatureBytes == null) {
          EDSignature signature;
          if (connection.isHandshaked()) {
            Hash payloadHash = Hash.hash(payloadBytes, 0, payloadLength);
            signature = connection.getEphemeralLocalKeyPair().getPrivateKey().sign(payloadHash);
          } else signature = EDSignature.NULL;

          signatureBytes = Serialization.getInstance().toDson(signature, Output.WIRE);

          message.setCachedSignature(signatureBytes);
        }

        if (signatureBytes.length > Message.MAX_SIGNATURE_SIZE)
          throw new IOException(
              "Message signature size " + signatureBytes.length + " is too large");
      }

      message.setSize(
          Message.HEADER_SIZE
              + (signatureBytes == null ? 0 : signatureBytes.length)
              + payloadLength);
    }

    headerBuffer.putInt(payloadLength);
    headerBuffer.put(payloadCompressed ? (byte) 1 : (byte) 0);
    headerBuffer.putShort(signatureBytes == null ? 0 : (short) signatureBytes.length);

    dataOutputStream.write(headerBuffer.array());
    if (signatureBytes != null) dataOutputStream.write(signatureBytes);
    dataOutputStream.write(payloadBytes, 0, payloadLength);
  }

  public final String MSN() {
    return this.getClass().getAnnotation(SerializerId2.class).value();
  }

  @JsonProperty("magic")
  @DsonOutput(value = Output.HASH, include = false)
  private int magic = Universe.getDefault().getMagic();

  @JsonProperty("seq")
  @DsonOutput(value = Output.HASH, include = false)
  private long seq = Message.nextSeq.incrementAndGet();

  @JsonProperty("timestamp")
  @DsonOutput(Output.ALL)
  private long timestamp = 0;

  // Transients //
  private transient int size = 0;
  private transient Direction direction;

  // Caches for multiple transmissions //
  private transient byte[] cachedPayload;
  private transient byte[] cachedSignature;
  private transient boolean cachedPayloadCompressed;

  protected Message() {
    super();

    this.timestamp = Time.getSystemTime();
  }

  public final String getCommand() {
    return getClass().getAnnotation(SerializerId2.class).value();
  }

  public final Direction getDirection() {
    return this.direction;
  }

  public final void setDirection(Direction direction) {
    this.direction = direction;
  }

  public final int getSize() {
    if (this.size == 0) throw new IllegalStateException("Size is unknown for message " + this);

    return this.size;
  }

  private final void setSize(int size) {
    this.size = size;
  }

  public final long getMagic() {
    return this.magic;
  }

  public final long getSeq() {
    return this.seq;
  }

  public long getTimestamp() {
    return this.timestamp;
  }

  public boolean isUrgent() {
    final TransportParameters transportParameters =
        getClass().getAnnotation(TransportParameters.class);
    if (transportParameters == null) return false;

    return true;
  }

  public int getPriority() {
    final TransportParameters transportParameters =
        getClass().getAnnotation(TransportParameters.class);
    if (transportParameters == null) return 0;

    return transportParameters.priority();
  }

  @Override
  public String toString() {
    return getCommand() + ":" + getDirection() + ":" + getHash() + " @ " + getTimestamp();
  }

  private synchronized byte[] getCachedPayload() {
    if (this.direction == null) throw new IllegalStateException("Direction has not been set");

    if (this.direction == Direction.INBOUND)
      throw new IllegalStateException("Cache is not available on INBOUND messages");

    return this.cachedPayload;
  }

  private synchronized void setCachedPayload(byte[] payload) {
    if (this.direction == null) throw new IllegalStateException("Direction has not been set");

    if (this.direction == Direction.INBOUND)
      throw new IllegalStateException("Cache is not available on INBOUND messages");

    this.cachedPayload = payload;
  }

  private synchronized boolean isCachedPayloadCompressed() {
    if (this.direction == null) throw new IllegalStateException("Direction has not been set");

    if (this.direction == Direction.INBOUND)
      throw new IllegalStateException("Cache is not available on INBOUND messages");

    return this.cachedPayloadCompressed;
  }

  private synchronized void setCachedPayloadCompressed(boolean compressed) {
    if (this.direction == null) throw new IllegalStateException("Direction has not been set");

    if (this.direction == Direction.INBOUND)
      throw new IllegalStateException("Cache is not available on INBOUND messages");

    this.cachedPayloadCompressed = compressed;
  }

  public boolean requiresSignature() {
    return false;
  }

  private synchronized byte[] getCachedSignature() {
    if (this.direction == null) throw new IllegalStateException("Direction has not been set");

    if (this.direction == Direction.INBOUND)
      throw new IllegalStateException("Cache is not available on INBOUND messages");

    return this.cachedSignature;
  }

  private synchronized void setCachedSignature(byte[] signature) {
    if (this.direction == null) throw new IllegalStateException("Direction has not been set");

    if (this.direction == Direction.INBOUND)
      throw new IllegalStateException("Cache is not available on INBOUND messages");

    this.cachedSignature = signature;
  }
}
