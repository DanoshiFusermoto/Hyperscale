package org.radix.hyperscale.ledger.sme;

import java.util.regex.Pattern;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.crypto.Identity;
import org.radix.hyperscale.ledger.StateAddress;
import org.radix.hyperscale.ledger.StateContext;
import org.radix.hyperscale.ledger.StateLockMode;
import org.radix.hyperscale.ledger.Substate.NativeField;
import org.radix.hyperscale.ledger.primitives.Blob;
import org.radix.hyperscale.utils.Strings;

@StateContext("blueprint")
public final class Deploy implements Instruction {
  private final String blueprintName;
  private final String language;
  private final Blob code;
  private final Identity authority;
  private final StateAddress packageAddress;
  private final StateAddress blueprintAddress;

  public Deploy(
      final String blueprintName,
      final String language,
      final Blob code,
      final Identity authority) {
    super();

    if (Pattern.matches("^[a-zA-Z0-9_.-]*$", blueprintName) == false)
      throw new IllegalArgumentException("Blueprint name " + blueprintName + " is invalid");

    this.code = code;
    this.authority = authority;
    this.language = Strings.toLowerCase(language);
    this.blueprintName = Strings.toLowerCase(blueprintName);
    this.packageAddress =
        StateAddress.from(
            Package.class.getAnnotation(StateContext.class).value(),
            Hash.valueOf(this.blueprintName));
    this.blueprintAddress = StateAddress.from(Deploy.class, Hash.valueOf(this.blueprintName));
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((authority == null) ? 0 : authority.hashCode());
    result = prime * result + ((blueprintName == null) ? 0 : blueprintName.hashCode());
    result = prime * result + ((code == null) ? 0 : code.hashCode());
    result = prime * result + ((language == null) ? 0 : language.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;

    if (obj == null) return false;

    if (getClass() != obj.getClass()) return false;

    Deploy other = (Deploy) obj;
    if (authority == null) {
      if (other.authority != null) return false;
    } else if (!authority.equals(other.authority)) return false;

    if (blueprintName == null) {
      if (other.blueprintName != null) return false;
    } else if (!blueprintName.equals(other.blueprintName)) return false;

    if (code == null) {
      if (other.code != null) return false;
    } else if (!code.equals(other.code)) return false;

    if (language == null) {
      if (other.language != null) return false;
    } else if (!language.equals(other.language)) return false;

    return true;
  }

  void lock(StateMachine stateMachine) {
    stateMachine.lock(this.packageAddress, StateLockMode.WRITE);
    stateMachine.lock(this.blueprintAddress, StateLockMode.WRITE);
  }

  StateAddress getBlueprintAddress() {
    return this.blueprintAddress;
  }

  String getBlueprintName() {
    return this.blueprintName;
  }

  StateAddress getPackageAddress() {
    return this.packageAddress;
  }

  PolyglotPackage execute(final StateMachine stateMachine) {
    stateMachine.assertCreate(getBlueprintAddress(), this.authority);

    stateMachine.set(getBlueprintAddress(), NativeField.CODE, this.code.getHash(), this.authority);
    stateMachine.set(
        getBlueprintAddress(),
        NativeField.LANGUAGE,
        Strings.toLowerCase(this.language),
        this.authority);
    stateMachine.set(
        getBlueprintAddress(),
        NativeField.BLUEPRINT,
        Strings.toLowerCase(this.blueprintName),
        this.authority);
    stateMachine.associate(getBlueprintAddress(), this.authority);

    final PolyglotPackage pakage =
        new PolyglotPackage(this.packageAddress, this.language, this.code);
    stateMachine.inject(pakage);
    return pakage;
  }

  @Override
  public String toString() {
    return this.blueprintName
        + " "
        + this.language
        + " "
        + this.code.getHash()
        + " "
        + this.authority;
  }
}
