package org.radix.hyperscale.ledger.sme;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;
import org.radix.hyperscale.common.Primitive;
import org.radix.hyperscale.ledger.StateAddress;
import org.radix.hyperscale.ledger.primitives.Blob;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.DsonOutput.Output;
import org.radix.hyperscale.serialization.SerializerId2;
import org.radix.hyperscale.utils.Numbers;
import org.radix.hyperscale.utils.Strings;

@SerializerId2("sm.package.polyglot")
public final class PolyglotPackage extends Package implements Primitive {
  @JsonProperty("language")
  @DsonOutput(Output.ALL)
  private String language;

  @JsonProperty("code")
  @DsonOutput(Output.ALL)
  private Blob code;

  PolyglotPackage() {
    super();
  }

  public PolyglotPackage(final String blueprint, final String language, final Blob code) {
    this(StateAddress.from(PolyglotPackage.class, blueprint), language, code);
  }

  public PolyglotPackage(final StateAddress address, final String language, final Blob code) {
    super(address);

    Objects.requireNonNull(language, "Language for code is null");
    Numbers.isZero(language.length(), "Language length is 0");
    Objects.requireNonNull(code, "Code is null");
    Numbers.isZero(code.size(), "Code length is 0");

    this.code = code;
    this.language = Strings.toLowerCase(language);
  }

  public Blob getCode() {
    return this.code;
  }

  public String getLanguage() {
    return this.language;
  }

  @Override
  public String toString() {
    return "PolyglotPackage [hash="
        + getHash()
        + " address="
        + getAddress()
        + " language="
        + getLanguage()
        + " blob="
        + getCode().size()
        + " bytes]";
  }
}
