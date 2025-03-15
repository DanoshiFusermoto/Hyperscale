package org.radix.hyperscale.network;

import java.io.IOException;
import java.net.Socket;
import java.net.URI;
import org.radix.hyperscale.Context;
import org.radix.hyperscale.crypto.CryptoException;
import org.radix.hyperscale.serialization.Polymorphic;
import org.radix.hyperscale.serialization.SerializerId2;

@SerializerId2("network.connection")
public final class TCPConnection extends AbstractConnection implements Polymorphic {
  public TCPConnection(final Context context, final URI host) throws IOException, CryptoException {
    super(context, host);
  }

  public TCPConnection(final Context context, final Socket socket)
      throws IOException, CryptoException {
    super(context, socket);
  }

  @Override
  public Protocol getProtocol() {
    return Protocol.TCP;
  }

  @Override
  public boolean requiresSignatures() {
    return false;
  }
}
