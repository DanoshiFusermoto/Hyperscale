package org.radix.hyperscale.ledger.messages;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.ledger.Substate;
import org.radix.hyperscale.network.messages.Message;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.DsonOutput.Output;
import org.radix.hyperscale.serialization.SerializerId2;

@SerializerId2("ledger.messages.substate")
public final class SubstateMessage extends Message {
  @JsonProperty("substate")
  @DsonOutput(Output.ALL)
  private Substate substate;

  @JsonProperty("version")
  @DsonOutput(Output.ALL)
  private Hash version;

  @SuppressWarnings("unused")
  private SubstateMessage() {
    super();
  }

  public SubstateMessage(final Substate substate, final Hash version) {
    super();

    this.substate = Objects.requireNonNull(substate, "Substate is null");
    this.version = Objects.requireNonNull(version, "Version hash is null");
  }

  public Hash getVersion() {
    return this.version;
  }

  public Substate getSubstate() {
    return this.substate;
  }
}
