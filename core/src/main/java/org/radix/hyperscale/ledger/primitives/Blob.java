package org.radix.hyperscale.ledger.primitives;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Objects;
import org.apache.commons.lang3.ArrayUtils;
import org.radix.hyperscale.common.BasicObject;
import org.radix.hyperscale.common.Primitive;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.ledger.StateContext;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.DsonOutput.Output;
import org.radix.hyperscale.serialization.SerializerId2;
import org.radix.hyperscale.utils.Numbers;

@SerializerId2("ledger.blob")
@StateContext("blob")
public final class Blob extends BasicObject implements Primitive {
  @JsonProperty("mime")
  @DsonOutput(Output.ALL)
  private String mime;

  @JsonProperty("encoding")
  @DsonOutput(Output.ALL)
  private String encoding;

  @JsonProperty("bytes")
  @DsonOutput(Output.ALL)
  private byte[] bytes;

  private transient volatile Hash checksum;

  @SuppressWarnings("unused")
  private Blob() {
    // FOR SERIALIZER
  }

  public Blob(final String dataURL) {
    Objects.requireNonNull(dataURL, "DataURL string is null");
    Numbers.isZero(dataURL.length(), "DataURL string is empty");

    if (dataURL.startsWith("data:") == false)
      throw new IllegalArgumentException("Data is not an encoded URL");

    int dataOffset = dataURL.indexOf(',');
    int mimeOffset = dataURL.indexOf(':');
    int encodingOffset = dataURL.indexOf(';');
    if (encodingOffset > dataOffset) encodingOffset = -1;

    this.mime =
        dataURL.substring(mimeOffset + 1, encodingOffset == -1 ? dataOffset : encodingOffset);
    if (this.mime.isEmpty()) this.mime = "text/plain";

    if (encodingOffset == -1) {
      this.encoding = "utf-8";
      this.bytes = dataURL.substring(dataOffset + 1).getBytes(StandardCharsets.UTF_8);
    } else {
      String dataURLEncoding = dataURL.substring(encodingOffset + 1, dataOffset).toLowerCase();
      if (dataURLEncoding.equalsIgnoreCase("base64") == false)
        throw new IllegalArgumentException("DataURL unsupported encoding type " + dataURLEncoding);

      this.encoding = "binary";
      this.bytes = Base64.getDecoder().decode(dataURL.substring(dataOffset + 1));
    }
  }

  public Blob(String mime, String data) {
    Objects.requireNonNull(data, "Data string is null");
    Numbers.isZero(data.length(), "Data string is empty");
    Objects.requireNonNull(mime, "Mime is null");
    Numbers.isZero(mime.length(), "Mime is empty");

    this.mime = mime;
    this.encoding = "utf-8";
    this.bytes = data.getBytes(StandardCharsets.UTF_8);
  }

  public Blob(String mime, final byte[] data) {
    Objects.requireNonNull(data, "Data is null");
    Numbers.isZero(data.length, "Data is empty");
    Objects.requireNonNull(mime, "Mime is null");
    Numbers.isZero(mime.length(), "Mime is empty");

    this.mime = mime;
    this.encoding = "binary";
    this.bytes = ArrayUtils.clone(data);
  }

  public Blob(String mime, String encoding, final byte[] data) {
    Objects.requireNonNull(data, "Data is null");
    Numbers.isZero(data.length, "Data is empty");
    Objects.requireNonNull(encoding, "Encoding is null");
    Numbers.isZero(encoding.length(), "Encoding is empty");
    Objects.requireNonNull(mime, "Mime is null");
    Numbers.isZero(mime.length(), "Mime is empty");

    this.mime = mime;
    this.encoding = encoding;
    this.bytes = ArrayUtils.clone(data);
  }

  @JsonProperty("checksum")
  @DsonOutput(Output.ALL)
  public Hash getChecksum() {
    if (this.checksum == null) this.checksum = Hash.hash(this.bytes);

    return this.checksum;
  }

  public int size() {
    return this.bytes.length;
  }

  public String getEncoding() {
    return this.encoding;
  }

  public String getMimeType() {
    return this.mime;
  }

  public byte[] getBytes() {
    return ArrayUtils.clone(this.bytes);
  }

  public String asDataURL() {
    StringBuilder builder = new StringBuilder();
    builder.append("data:");

    if (this.mime != null && this.mime.isEmpty() == false) builder.append(this.mime);

    if (this.encoding != null && this.encoding.equalsIgnoreCase("binary")) {
      builder.append(";base64,");
      builder.append(Base64.getEncoder().encodeToString(this.bytes));
    } else {
      builder.append(",");
      builder.append(new String(this.bytes, StandardCharsets.UTF_8));
    }

    return builder.toString();
  }
}
