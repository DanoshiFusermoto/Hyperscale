package org.radix.hyperscale.network.peers;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseNotFoundException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.json.JSONObject;
import org.radix.hyperscale.Context;
import org.radix.hyperscale.crypto.Identity;
import org.radix.hyperscale.database.DatabaseException;
import org.radix.hyperscale.database.DatabaseStore;
import org.radix.hyperscale.exceptions.ServiceException;
import org.radix.hyperscale.exceptions.StartupException;
import org.radix.hyperscale.exceptions.TerminationException;
import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;
import org.radix.hyperscale.network.discovery.PeerFilter;
import org.radix.hyperscale.serialization.DsonOutput.Output;
import org.radix.hyperscale.serialization.Serialization;
import org.radix.hyperscale.utils.Numbers;
import org.radix.hyperscale.utils.URIs;

public class PeerStore extends DatabaseStore {
  private static final Logger networklog = Logging.getLogger("network");

  public static final int MAX_CONNECTION_ATTEMPTS = 10;

  private final Context context;
  private final Object cachesMutex = new Object();
  private final Map<URI, Peer> hostToPeerCache;
  private final Map<Identity, URI> identityToHostCache;

  private Database peersDB;

  public PeerStore(final Context context) {
    super(Objects.requireNonNull(context, "Context is null").getDatabaseEnvironment());

    this.context = context;
    this.hostToPeerCache = new LinkedHashMap<>(1 << 12);
    this.identityToHostCache = new HashMap<>(1 << 12);
  }

  @Override
  public void start() throws StartupException {
    try {
      if (this.context.getConfiguration().getCommandLine("clean", false).equals(Boolean.TRUE))
        clean();

      final DatabaseConfig config = new DatabaseConfig();
      config.setAllowCreate(true);
      config.setTransactional(true);
      this.peersDB = getEnvironment().openDatabase(null, "peers", config);

      final Transaction transaction = this.getEnvironment().beginTransaction(null, null);
      try {
        try (final Cursor cursor = this.peersDB.openCursor(transaction, null)) {
          final DatabaseEntry key = new DatabaseEntry();
          final DatabaseEntry value = new DatabaseEntry();

          synchronized (this.cachesMutex) {
            while (cursor.getNext(key, value, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
              final Peer peer = Serialization.getInstance().fromDson(value.getData(), Peer.class);

              // TODO check for duplicates
              this.hostToPeerCache.put(peer.getHost(), peer);
              this.identityToHostCache.put(peer.getIdentity(), peer.getHost());
            }
          }
        }

        transaction.commit();
      } catch (Exception ex) {
        transaction.abort();
        throw ex;
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void stop() throws TerminationException {
    try {
      close();
    } catch (IOException ioex) {
      throw new TerminationException(ioex);
    }
  }

  @Override
  public void clean() throws ServiceException {
    final Transaction transaction =
        getEnvironment().beginTransaction(null, new TransactionConfig().setReadUncommitted(true));
    try {
      getEnvironment().truncateDatabase(transaction, "peers", false);
      transaction.commit();

      synchronized (this.cachesMutex) {
        this.hostToPeerCache.clear();
        this.identityToHostCache.clear();
      }
    } catch (DatabaseNotFoundException dsnfex) {
      transaction.abort();
      log.warn(dsnfex.getMessage());
    } catch (Exception ex) {
      transaction.abort();
      throw new ServiceException(ex, getClass());
    }
  }

  @Override
  public void close() throws IOException {
    super.close();

    if (this.peersDB != null) this.peersDB.close();
  }

  @Override
  public void flush() throws DatabaseException {
    final Transaction transaction = this.getEnvironment().beginTransaction(null, null);
    try {
      final List<Peer> peers;
      synchronized (this.cachesMutex) {
        peers = new ArrayList<Peer>(this.hostToPeerCache.values());
      }

      for (final Peer peer : peers) {
        final DatabaseEntry key =
            new DatabaseEntry(
                peer.getURI().toString().toLowerCase().getBytes(StandardCharsets.UTF_8));
        final byte[] bytes = Serialization.getInstance().toDson(peer, Output.PERSIST);
        final DatabaseEntry value = new DatabaseEntry(bytes);

        if (this.peersDB.put(transaction, key, value) == OperationStatus.SUCCESS) {
          if (networklog.hasLevel(Logging.DEBUG))
            networklog.debug(this.context.getName() + ": Updated " + peer);
        }
      }

      transaction.commit();
    } catch (Exception e) {
      transaction.abort();
      throw new DatabaseException(e);
    }
  }

  boolean delete(final URI uri) throws IOException {
    Objects.requireNonNull(uri, "URI is null");

    final Transaction transaction = this.getEnvironment().beginTransaction(null, null);
    try {
      final Peer peer;
      final URI hostAndPort = URIs.toHostAndPort(uri);
      synchronized (this.cachesMutex) {
        peer = this.hostToPeerCache.remove(hostAndPort);
        if (peer != null && peer.getIdentity() != null)
          this.identityToHostCache.remove(peer.getIdentity(), hostAndPort);
      }

      final DatabaseEntry key =
          new DatabaseEntry(hostAndPort.toString().toLowerCase().getBytes(StandardCharsets.UTF_8));
      final OperationStatus status = this.peersDB.delete(transaction, key);
      if (status == OperationStatus.SUCCESS) {
        if (networklog.hasLevel(Logging.DEBUG))
          networklog.debug(this.context.getName() + ": Deleted peer " + hostAndPort);

        transaction.commit();
        return true;
      }

      transaction.abort();
      return false;
    } catch (Exception e) {
      transaction.abort();
      throw new DatabaseException(e);
    }
  }

  boolean delete(final Identity identity) throws IOException {
    Objects.requireNonNull(identity, "Identity is null");

    final URI hostAndPort;
    synchronized (this.cachesMutex) {
      hostAndPort = this.identityToHostCache.remove(identity);
      if (hostAndPort == null) return false;

      this.hostToPeerCache.remove(hostAndPort);
    }

    final Transaction transaction = this.getEnvironment().beginTransaction(null, null);
    try {
      final DatabaseEntry key =
          new DatabaseEntry(hostAndPort.toString().toLowerCase().getBytes(StandardCharsets.UTF_8));
      final OperationStatus status = this.peersDB.delete(transaction, key);
      if (status == OperationStatus.SUCCESS) {
        if (networklog.hasLevel(Logging.DEBUG))
          networklog.debug(
              this.context.getName() + ": Deleted peer " + identity + " @ " + hostAndPort);

        transaction.commit();
        return true;
      }

      transaction.abort();
      return false;
    } catch (Exception e) {
      transaction.abort();
      throw new DatabaseException(e);
    }
  }

  boolean store(final Peer peer) throws IOException {
    Objects.requireNonNull(peer, "Peer is null");

    if (peer.getIdentity() == null)
      throw new IOException("Can not store a peer without an identity");

    try {
      synchronized (this.cachesMutex) {
        final URI hostAndPort = URIs.toHostAndPort(peer.getHost());
        this.hostToPeerCache.put(hostAndPort, peer);
        this.identityToHostCache.put(peer.getIdentity(), hostAndPort);
      }

      if (networklog.hasLevel(Logging.DEBUG)) {
        JSONObject peerJSONObject = Serialization.getInstance().toJsonObject(peer, Output.PERSIST);
        networklog.info(this.context.getName() + ": Stored peer");
        networklog.info(peerJSONObject.toString(4));
      }

      return true;
    } catch (Exception e) {
      throw new DatabaseException(e);
    }
  }

  boolean has(final URI uri) {
    Objects.requireNonNull(uri, "URI is null");

    final URI hostAndPort = URIs.toHostAndPort(uri);
    synchronized (this.cachesMutex) {
      return this.hostToPeerCache.containsKey(hostAndPort);
    }
  }

  Peer get(final URI uri) {
    Objects.requireNonNull(uri, "URI is null");

    final Peer peer;
    final URI hostAndPort = URIs.toHostAndPort(uri);
    synchronized (this.cachesMutex) {
      peer = this.hostToPeerCache.get(hostAndPort);
      if (peer == null) return null;

      if (peer.getIdentity() == null) return peer;

      URI expectedHostAndPort = this.identityToHostCache.get(peer.getIdentity());
      if (hostAndPort.equals(expectedHostAndPort) == false)
        networklog.error(
            PeerStore.this.context.getName()
                + ": Detected host inconsistency "
                + hostAndPort
                + " but discovered "
                + expectedHostAndPort);

      return peer;
    }
  }

  boolean has(final Identity identity) {
    Objects.requireNonNull(identity, "Identity is null");

    final Peer peer;
    final URI hostAndPort;
    synchronized (this.cachesMutex) {
      hostAndPort = this.identityToHostCache.get(identity);
      if (hostAndPort == null) return false;

      peer = this.hostToPeerCache.get(hostAndPort);
      if (peer == null || identity.equals(peer.getIdentity()) == false) {
        networklog.error(
            PeerStore.this.context.getName()
                + ": Detected identity inconsistency "
                + identity
                + "@"
                + hostAndPort
                + " but is assigned to "
                + peer);
        return false;
      }

      return true;
    }
  }

  Peer get(final Identity identity) {
    Objects.requireNonNull(identity, "Identity is null");

    final Peer peer;
    final URI hostAndPort;
    synchronized (this.cachesMutex) {
      hostAndPort = this.identityToHostCache.get(identity);
      if (hostAndPort == null) return null;

      peer = this.hostToPeerCache.get(hostAndPort);
      if (peer == null || identity.equals(peer.getIdentity()) == false)
        networklog.error(
            PeerStore.this.context.getName()
                + ": Detected identity inconsistency "
                + identity
                + "@"
                + hostAndPort
                + " but is assigned to "
                + peer);

      return peer;
    }
  }

  List<Peer> get(final PeerFilter filter, final int index, final int limit) throws IOException {
    Numbers.lessThan(limit, 1, "Limit can not be less than 1");
    Numbers.isNegative(index, "Index can not be less than 0");

    Objects.requireNonNull(filter, "PeerFilter is null");

    final List<Peer> peers = new ArrayList<>();
    synchronized (this.cachesMutex) {
      int i = 0;
      for (final Peer peer : this.hostToPeerCache.values()) {
        try {
          if (index > i) continue;

          if (filter.filter(peer)) peers.add(peer);

          if (peers.size() == limit) break;
        } finally {
          i++;
        }
      }
    }

    return peers;
  }

  List<Peer> get(final PeerFilter filter) throws IOException {
    Objects.requireNonNull(filter, "Peer filter is null");

    final List<Peer> peers = new ArrayList<>();
    synchronized (this.cachesMutex) {
      for (final Peer peer : this.hostToPeerCache.values()) {
        if (filter.filter(peer)) peers.add(peer);
      }
    }

    return peers;
  }
}
