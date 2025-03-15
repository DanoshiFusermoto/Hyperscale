package org.radix.hyperscale.ledger.messages;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.network.messages.Message;
import org.radix.hyperscale.serialization.DsonOutput;
import org.radix.hyperscale.serialization.DsonOutput.Output;
import org.radix.hyperscale.serialization.SerializerId2;

@SerializerId2("ledger.messages.block.sync.inv.get")
public final class GetSyncBlockInventoryMessage extends Message {
  @JsonProperty("head")
  @DsonOutput(Output.ALL)
  private Hash head;

  GetSyncBlockInventoryMessage() {
    super();
  }

  public GetSyncBlockInventoryMessage(final Hash head) {
    this();

    this.head = Objects.requireNonNull(head, "Block head hash is null");
    Hash.notZero(head, "Head hash is ZERO");
  }

  public Hash getHead() {
    return this.head;
  }
}
