package org.radix.hyperscale.node;

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import org.java_websocket.WebSocketImpl;
import org.radix.hyperscale.Configuration;
import org.radix.hyperscale.Universe;
import org.radix.hyperscale.WebService;
import org.radix.hyperscale.common.Agent;
import org.radix.hyperscale.crypto.CryptoException;
import org.radix.hyperscale.crypto.bls12381.BLSKeyPair;
import org.radix.hyperscale.crypto.bls12381.BLSSignature;
import org.radix.hyperscale.crypto.ed25519.EDKeyPair;
import org.radix.hyperscale.ledger.BlockHeader;
import org.radix.hyperscale.serialization.Polymorphic;
import org.radix.hyperscale.serialization.SerializerId2;

@SerializerId2("node")
public final class LocalNode extends Node implements Polymorphic {
  public static final LocalNode load(
      final String name, final Configuration configuration, final boolean create)
      throws CryptoException {
    try {
      final BLSKeyPair BLSKey =
          BLSKeyPair.fromFile(new File(configuration.get("node.key.path", name + ".key")), create);
      final Set<Services> services = new LinkedHashSet<>();
      if (configuration.get("node.service.bootstrap", false).equals(Boolean.TRUE))
        services.add(Services.BOOTSTRAP);
      if (configuration.get("node.service.gateway", false).equals(Boolean.TRUE))
        services.add(Services.GATEWAY);
      if (configuration.get("network.address") != null) services.add(Services.GATEWAY);

      return new LocalNode(
          BLSKey,
          configuration.get("network.port", Universe.getDefault().getPort()),
          configuration.get("api.port", WebService.DEFAULT_PORT),
          configuration.get("websocket.port", WebSocketImpl.DEFAULT_PORT),
          services,
          Universe.getDefault().getGenesis().getHeader(),
          Agent.AGENT,
          Agent.AGENT_VERSION,
          Agent.PROTOCOL_VERSION);
    } catch (IOException ex) {
      throw new IllegalStateException(ex);
    }
  }

  private final BLSKeyPair BLSKey;
  private final EDKeyPair ephemeralKeyPair;
  private final BLSSignature ephemeralBinding;
  private volatile boolean isProgressing = false;

  public LocalNode(
      final BLSKeyPair BLSKey,
      final int networkPort,
      final int apiPort,
      final int websocketPort,
      final Set<Services> services,
      final BlockHeader head)
      throws CryptoException {
    this(
        BLSKey,
        networkPort,
        apiPort,
        websocketPort,
        services,
        head,
        Agent.AGENT,
        Agent.AGENT_VERSION,
        Agent.PROTOCOL_VERSION);
  }

  public LocalNode(
      final BLSKeyPair BLSKey,
      final int networkPort,
      final int apiPort,
      final int websocketPort,
      final Set<Services> services,
      final BlockHeader head,
      final String agent,
      final int agentVersion,
      final int protocolVersion)
      throws CryptoException {
    super(
        Objects.requireNonNull(BLSKey, "Key is null").getIdentity(),
        head,
        agent,
        agentVersion,
        protocolVersion,
        networkPort,
        apiPort,
        websocketPort,
        services,
        false);

    this.BLSKey = BLSKey;
    this.ephemeralKeyPair = new EDKeyPair();
    this.ephemeralBinding =
        this.BLSKey.getPrivateKey().sign(ephemeralKeyPair.getPublicKey().toByteArray());
  }

  public void fromPersisted(final Node persisted) {
    Objects.requireNonNull(persisted, "Persisted local node is null");
    if (persisted.getIdentity().equals(this.BLSKey.getIdentity()) == false)
      throw new IllegalArgumentException(
          "Persisted node BLS key does not match " + this.BLSKey.getIdentity());

    setHead(persisted.getHead());
    setNetworkPort(persisted.getNetworkPort());
    setAPIPort(persisted.getAPIPort());
    setWebsocketPort(persisted.getWebsocketPort());
    setSynced(false);
  }

  public BLSKeyPair getKeyPair() {
    return this.BLSKey;
  }

  public EDKeyPair getEphemeralKeyPair() {
    return this.ephemeralKeyPair;
  }

  public BLSSignature getEphemeralBinding() {
    return this.ephemeralBinding;
  }

  public boolean isProgressing() {
    return this.isProgressing;
  }

  public void setProgressing(boolean progressing) {
    this.isProgressing = progressing;
  }
}
