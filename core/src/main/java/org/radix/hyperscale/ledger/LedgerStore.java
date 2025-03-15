package org.radix.hyperscale.ledger;

import com.google.common.primitives.Longs;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import org.eclipse.collections.api.factory.Maps;
import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.map.MutableMap;
import org.eclipse.collections.api.set.MutableSet;
import org.radix.hyperscale.Constants;
import org.radix.hyperscale.Context;
import org.radix.hyperscale.Universe;
import org.radix.hyperscale.common.CompoundPrimitive;
import org.radix.hyperscale.common.EphemeralPrimitive;
import org.radix.hyperscale.common.Order;
import org.radix.hyperscale.common.Primitive;
import org.radix.hyperscale.crypto.ComputeKey;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.database.DatabaseException;
import org.radix.hyperscale.database.DatabaseStore;
import org.radix.hyperscale.database.vamos.Cursor;
import org.radix.hyperscale.database.vamos.Database;
import org.radix.hyperscale.database.vamos.DatabaseConfig;
import org.radix.hyperscale.database.vamos.Transaction;
import org.radix.hyperscale.exceptions.ServiceException;
import org.radix.hyperscale.exceptions.StartupException;
import org.radix.hyperscale.exceptions.TerminationException;
import org.radix.hyperscale.executors.Executor;
import org.radix.hyperscale.executors.ScheduledExecutable;
import org.radix.hyperscale.ledger.BlockHeader.InventoryType;
import org.radix.hyperscale.ledger.Substate.NativeField;
import org.radix.hyperscale.ledger.primitives.Atom;
import org.radix.hyperscale.ledger.primitives.AtomCertificate;
import org.radix.hyperscale.ledger.primitives.Blob;
import org.radix.hyperscale.ledger.primitives.StateCertificate;
import org.radix.hyperscale.ledger.primitives.StateOutput;
import org.radix.hyperscale.ledger.sme.PolyglotPackage;
import org.radix.hyperscale.ledger.sme.SubstateTransitions;
import org.radix.hyperscale.ledger.timeouts.AcceptTimeout;
import org.radix.hyperscale.ledger.timeouts.AtomTimeout;
import org.radix.hyperscale.ledger.timeouts.CommitTimeout;
import org.radix.hyperscale.ledger.timeouts.ExecutionTimeout;
import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;
import org.radix.hyperscale.serialization.ByteBufferOutputStream;
import org.radix.hyperscale.serialization.DsonOutput.Output;
import org.radix.hyperscale.serialization.Serializable;
import org.radix.hyperscale.serialization.Serialization;
import org.radix.hyperscale.utils.Numbers;

public class LedgerStore extends DatabaseStore implements LedgerProvider {
  public static final int BYTE_BUFFER_SIZE = 1 << 20;

  private static final ThreadLocal<DatabaseEntry> keyEntry =
      ThreadLocal.withInitial(() -> new DatabaseEntry());
  private static final ThreadLocal<DatabaseEntry> valueEntry =
      ThreadLocal.withInitial(() -> new DatabaseEntry());
  private static final ThreadLocal<ByteBuffer> byteBuffer =
      ThreadLocal.withInitial(() -> ByteBuffer.allocate(BYTE_BUFFER_SIZE));

  private static final Logger databaseLog = Logging.getLogger("database");

  static {
    databaseLog.setLevel(Logging.INFO);
  }

  private final Context context;

  private Database substates;
  private Database associations;
  private Database syncChain;

  protected class InventoryRecord {
    private final Class<? extends Primitive> clazz;
    private final Hash head;
    private final Hash hash;
    private final long witnessedAt;
    private final Primitive primitive;

    private final long clockStart;
    private volatile long clockEnd;

    InventoryRecord(final Primitive primitive, final Hash head) throws IOException {
      Objects.requireNonNull(head, "Inventory record header hash is null");
      Hash.notZero(head, "Inventory record header hash is ZERO");
      Objects.requireNonNull(primitive, "Inventory record primitive is null");

      this.head = head;
      this.hash = primitive.getHash();
      this.clazz = primitive.getClass();
      this.primitive = primitive;
      this.witnessedAt = System.currentTimeMillis();

      this.clockStart = Block.toHeight(head);
      this.clockEnd = Long.MAX_VALUE;
    }

    boolean isStale() {
      // Liveness check
      if (Block.toHeight(this.head) == Block.toHeight(LedgerStore.this.inventoryHead)) return false;

      // TODO 30 seconds is enough?  Very latent replicas should have transitioned to a "syncing"
      // state by then
      return System.currentTimeMillis() - this.witnessedAt
          > TimeUnit.MILLISECONDS.convert(Constants.PRIMITIVE_STALE, TimeUnit.SECONDS);
    }

    boolean isExpired() {
      // Liveness check
      if (Block.toHeight(this.head) == Block.toHeight(LedgerStore.this.inventoryHead)) return false;

      // TODO 60 seconds is enough?  Very latent replicas should have transitioned to a "syncing"
      // state by then
      return System.currentTimeMillis() - this.witnessedAt
          > TimeUnit.MILLISECONDS.convert(Constants.PRIMITIVE_EXPIRE, TimeUnit.SECONDS);
    }

    @SuppressWarnings("unchecked")
    <T extends Primitive> T get() {
      return (T) this.primitive;
    }

    Hash getHash() {
      return this.hash;
    }

    Hash getHead() {
      return this.head;
    }

    long getClockStart() {
      return this.clockStart;
    }

    long getClockEnd() {
      return this.clockEnd;
    }

    private void setClockEnd(long clock) {
      this.clockEnd = clock;
    }

    long getWitnessedAt() {
      return this.witnessedAt;
    }

    @Override
    public String toString() {
      return this.clazz.getName()
          + " "
          + this.hash
          + " "
          + this.witnessedAt
          + " "
          + this.clockStart
          + ":"
          + this.clockEnd;
    }
  }

  /* Internal tracking of last committed ledger head for inventory purposes.
   *
   * IMPORTANT Should match the actual head of the ledger at all times!
   */
  private transient volatile Hash inventoryHead = Hash.ZERO;

  /* Inventory of primitives primarily used for sync. */
  private final Map<Hash, InventoryRecord> primitivesInventory;

  /* Inventory of substates currently processing (saves I/O and serialization) */
  // TODO why is this needed when the Vamos caches should do most of the heavy lifting here?
  private final Map<StateAddress, SubstateCommit> substateInventory;

  public LedgerStore(final Context context) {
    super(Objects.requireNonNull(context, "Context is null").getDatabaseEnvironment());
    this.context = context;

    this.primitivesInventory = new ConcurrentHashMap<>(1 << 16);
    this.substateInventory =
        Maps.mutable.<StateAddress, SubstateCommit>ofInitialCapacity(1 << 16).asSynchronized();

    final ScheduledExecutable inventoryTask =
        new ScheduledExecutable(Constants.PRIMITIVE_GC_INTERVAL, TimeUnit.SECONDS) {
          @Override
          public void execute() {
            // NOTE shouldn't have any effect if there is a liveness break as proposals will still
            // have been voted upon
            //		may be some considerations when networks converge, but the strongest branch should
            // then start to receive
            //		2f+1 of the votes and become the commit branch.
            //
            // TODO currently nothing should be executed in a liveness break, if this was to change,
            // then issues may arise here as the
            //		primitive cache will grow excessively but will not be pruned as there wont be any
            // commits

            int evictedPrimitiveInventory = 0;
            final Iterator<InventoryRecord> inventoryIterator =
                LedgerStore.this.primitivesInventory.values().iterator();
            while (inventoryIterator.hasNext()) {
              final InventoryRecord inventory = inventoryIterator.next();
              if (inventory.isStale() && inventory.get() instanceof Serializable serializable)
                serializable.flushCachedDsonOutput();

              if (inventory.isExpired()) {
                inventoryIterator.remove();
                evictedPrimitiveInventory++;

                if (databaseLog.hasLevel(Logging.DEBUG))
                  databaseLog.debug(
                      LedgerStore.this.context.getName() + ": " + inventory + " is expired");
              }
            }

            databaseLog.info(
                LedgerStore.this.context.getName()
                    + ": Primitive inventory contains "
                    + LedgerStore.this.primitivesInventory.size()
                    + " items. Garbage collection evacuated "
                    + evictedPrimitiveInventory
                    + " items");
          }
        };
    Executor.getInstance().scheduleAtFixedRate(inventoryTask);
  }

  @Override
  public void start() throws StartupException {
    try {
      final DatabaseConfig syncChainConfig = new DatabaseConfig();
      syncChainConfig.setAllowCreate(true);

      final DatabaseConfig substatesConfig = new DatabaseConfig();
      substatesConfig.setAllowCreate(true);

      final DatabaseConfig associationsConfig = new DatabaseConfig();
      associationsConfig.setAllowCreate(true);
      associationsConfig.setAllowDuplicates(true);

      this.substates = this.context.getVamosEnvironment().open("substates", substatesConfig);
      this.associations =
          this.context.getVamosEnvironment().open("associations", associationsConfig);
      this.syncChain = this.context.getVamosEnvironment().open("sync.chain", syncChainConfig);

      this.inventoryHead = head();
    } catch (Exception ex) {
      throw new StartupException(ex, getClass());
    }
  }

  @Override
  public void stop() throws TerminationException {
    try {
      this.inventoryHead = Hash.ZERO;
      this.primitivesInventory.clear();

      close();
    } catch (IOException ioex) {
      throw new TerminationException(ioex);
    }
  }

  @Override
  public void close() throws IOException {
    super.close();

    if (this.associations != null) this.associations.close();
    if (this.substates != null) this.substates.close();
    if (this.syncChain != null) this.syncChain.close();
  }

  @Override
  public void clean() throws ServiceException {
    try {
      // TODO Vamos cleaning

      new File(
              this.getEnvironment().getEnvironment().getHome().toString()
                  + File.separator
                  + "ploom.dat")
          .delete();
    } catch (Exception ex) {
      throw new ServiceException(ex, getClass());
    }
  }

  @Override
  public void flush() throws DatabaseException {
    /* Not used */
  }

  void reset() {
    this.primitivesInventory.clear();
  }

  // PRIMITIVES //
  public OperationStatus delete(
      final Hash hash, final Class<? extends EphemeralPrimitive> primitive) throws IOException {
    if (EphemeralPrimitive.class.isAssignableFrom(primitive))
      throw new IllegalArgumentException("Ledger store does not support EphemeralPrimitive");

    try {
      final StateAddress stateAddress = StateAddress.from(primitive, hash);
      final DatabaseEntry key = keyEntry.get();
      key.setData(stateAddress.getHash().toByteArray());
      return this.substates.delete(null, key, LockMode.DEFAULT);
    } catch (Exception ex) {
      if (ex instanceof IOException) throw ex;
      else throw new DatabaseException(ex);
    }
  }

  final OperationStatus delete(
      final Collection<Hash> hashes, final Class<? extends EphemeralPrimitive> primitive)
      throws IOException {
    Objects.requireNonNull(hashes, "Hashes is null");
    Numbers.isZero(hashes.size(), "Hashes is empty");
    Objects.requireNonNull(primitive, "Primitive type is null");

    if (EphemeralPrimitive.class.isAssignableFrom(primitive))
      throw new IllegalArgumentException("Ledger store does not support EphemeralPrimitive");

    final Transaction transaction =
        new Transaction(this.context.getVamosEnvironment(), hashes.size());
    try {
      for (final Hash hash : hashes) {
        final StateAddress stateAddress = StateAddress.from(primitive, hash);
        final DatabaseEntry key = keyEntry.get();
        key.setData(stateAddress.getHash().toByteArray());
        final OperationStatus status = this.substates.delete(transaction, key, LockMode.DEFAULT);
        if (status.equals(OperationStatus.SUCCESS) == false)
          throw new DatabaseException(
              "Failed to delete " + primitive + " " + hash + " due to " + status.name());
      }

      transaction.commit();

      return OperationStatus.SUCCESS;
    } catch (Exception ex) {
      transaction.abort();
      if (ex instanceof DatabaseException) throw ex;
      throw new DatabaseException(ex);
    }
  }

  public <T extends Primitive> Collection<T> get(
      final Collection<Hash> hashes, final Class<T> primitive) throws IOException {
    return get(hashes, primitive, null);
  }

  public <T extends Primitive> Collection<T> get(
      final Collection<Hash> hashes, final Class<T> primitive, final BiConsumer<Hash, T> consumer)
      throws IOException {
    if (hashes.isEmpty()) return Collections.emptyList();

    final List<Hash> remaining = new ArrayList<Hash>(hashes.size());
    final MutableMap<Hash, T> results = Maps.mutable.ofInitialCapacity(hashes.size());

    if (CompoundPrimitive.class.isAssignableFrom(primitive) == false) {
      for (final Hash hash : hashes) {
        final InventoryRecord inventoryRecord = this.primitivesInventory.get(hash);
        if (inventoryRecord != null) results.putIfAbsent(hash, inventoryRecord.get());
        else remaining.add(hash);
      }
    }

    if (EphemeralPrimitive.class.isAssignableFrom(primitive) == false) {
      for (final Hash hash : remaining) {
        try {
          final T result = getPrimitive(null, hash, primitive);
          if (result != null) results.putIfAbsent(hash, result);
        } catch (IOException ioex) {
          databaseLog.warn(this.context.getName(), ioex);
        }
      }
    }

    if (consumer != null) results.forEach(consumer);

    return results.toList();
  }

  public <T extends Primitive> T get(final Hash hash, final Class<T> primitive) throws IOException {
    return getPrimitive(null, hash, primitive);
  }

  @SuppressWarnings("unchecked")
  private <T extends Primitive> T getPrimitive(
      final Transaction transaction, final Hash hash, final Class<T> type) throws IOException {
    try {
      if (CompoundPrimitive.class.isAssignableFrom(type) == false) {
        final InventoryRecord inventoryRecord = this.primitivesInventory.get(hash);
        if (inventoryRecord != null) return (T) inventoryRecord.get();
      }

      if (EphemeralPrimitive.class.isAssignableFrom(type)) return null;

      if (Atom.class.isAssignableFrom(type)
          || Blob.class.isAssignableFrom(type)
          || BlockHeader.class.isAssignableFrom(type)
          || PolyglotPackage.class.isAssignableFrom(type)) {
        final StateAddress stateAddress = StateAddress.from(type, hash);
        final SubstateCommit substateCommit = getSubstateCommit(transaction, stateAddress);
        if (substateCommit == null) return null;

        return substateCommit.getSubstate().get("primitive");
      } else if (AtomCertificate.class.isAssignableFrom(type)
          || AtomTimeout.class.isAssignableFrom(type)) {
        final DatabaseEntry key = keyEntry.get();
        key.setData(hash.toByteArray());
        final DatabaseEntry value = valueEntry.get();
        OperationStatus status = this.substates.get(transaction, key, value, LockMode.DEFAULT);
        if (status.equals(OperationStatus.SUCCESS) == false) return null;

        key.setData(value.getData());
        status = this.substates.get(transaction, key, value, LockMode.DEFAULT);
        if (status.equals(OperationStatus.SUCCESS) == false)
          throw new IllegalStateException("Atom substate referenced by " + hash + " is missing");

        final SubstateCommit atomSubstateCommit =
            Serialization.getInstance().fromDson(value.getData(), SubstateCommit.class);
        if (AtomCertificate.class.isAssignableFrom(type))
          return atomSubstateCommit.getSubstate().get("certificate");
        else if (AtomTimeout.class.isAssignableFrom(type))
          return atomSubstateCommit.getSubstate().get("timeout");
      } else if (type.equals(Block.class)) {
        final StateAddress stateAddress = StateAddress.from(BlockHeader.class, hash);
        final SubstateCommit substateCommit = getSubstateCommit(transaction, stateAddress);
        if (substateCommit == null) return null;

        final BlockHeader header = substateCommit.getSubstate().get("primitive");
        final List<Atom> accepted =
            new ArrayList<Atom>(header.getInventorySize(InventoryType.ACCEPTED));
        for (final Hash atomHash : header.getInventory(InventoryType.ACCEPTED)) {
          final Atom atom = getPrimitive(transaction, atomHash, Atom.class);
          if (atom == null)
            throw new IllegalStateException(
                "Found block header " + hash + " but contained atom " + atomHash + " is missing");

          accepted.add(atom);
        }

        final List<Atom> unaccepted =
            new ArrayList<Atom>(header.getInventorySize(InventoryType.UNACCEPTED));
        for (final Hash atomHash : header.getInventory(InventoryType.UNACCEPTED)) {
          final Atom atom = getPrimitive(transaction, atomHash, Atom.class);
          if (atom == null)
            throw new IllegalStateException(
                "Found block header " + hash + " but contained atom " + atomHash + " is missing");

          unaccepted.add(atom);
        }

        final List<AtomCertificate> certificates =
            new ArrayList<AtomCertificate>(header.getInventorySize(InventoryType.COMMITTED));
        for (final Hash certificateHash : header.getInventory(InventoryType.COMMITTED)) {
          final AtomCertificate certificate =
              getPrimitive(transaction, certificateHash, AtomCertificate.class);
          if (certificate == null)
            throw new IllegalStateException(
                "Found block header "
                    + hash
                    + " but contained certificate "
                    + certificateHash
                    + " is missing");

          certificates.add(certificate);
        }

        final List<ExecutionTimeout> unexecuted =
            new ArrayList<ExecutionTimeout>(header.getInventorySize(InventoryType.UNEXECUTED));
        for (final Hash timeoutHash : header.getInventory(InventoryType.UNEXECUTED)) {
          final ExecutionTimeout timeout =
              getPrimitive(transaction, timeoutHash, ExecutionTimeout.class);
          if (timeout == null)
            throw new IllegalStateException(
                "Found block header "
                    + hash
                    + " but contained execution timeout "
                    + timeoutHash
                    + " is missing");

          unexecuted.add(timeout);
        }

        final List<CommitTimeout> uncommitted =
            new ArrayList<CommitTimeout>(header.getInventorySize(InventoryType.UNCOMMITTED));
        for (final Hash timeoutHash : header.getInventory(InventoryType.UNCOMMITTED)) {
          final CommitTimeout timeout = getPrimitive(transaction, timeoutHash, CommitTimeout.class);
          if (timeout == null)
            throw new IllegalStateException(
                "Found block header "
                    + hash
                    + " but contained commit timeout "
                    + timeoutHash
                    + " is missing");

          uncommitted.add(timeout);
        }

        final List<PolyglotPackage> packages =
            new ArrayList<PolyglotPackage>(header.getInventorySize(InventoryType.PACKAGES));
        for (final Hash packageHash : header.getInventory(InventoryType.PACKAGES)) {
          final PolyglotPackage pakage =
              getPrimitive(transaction, packageHash, PolyglotPackage.class);
          if (pakage == null)
            throw new IllegalStateException(
                "Found block header "
                    + hash
                    + " but contained package "
                    + packageHash
                    + " is missing");

          packages.add(pakage);
        }

        // TODO check existence of signal atoms (latent, timedout, executable)?

        return (T)
            new Block(
                header, accepted, unaccepted, unexecuted, certificates, uncommitted, packages);
      } else
        throw new IllegalArgumentException(
            "Primitive type " + type + " not supported in LedgerStore");

      return null;
    } catch (Exception ex) {
      ex.printStackTrace();

      if (ex instanceof IOException) throw ex;
      else throw new DatabaseException(ex);
    }
  }

  Set<Hash> has(final Collection<Hash> hashes, final Class<? extends Primitive> primitive)
      throws DatabaseException {
    Objects.requireNonNull(hashes, "Hashes is null");

    if (hashes.isEmpty()) return Collections.emptySet();

    try {
      final List<Hash> remaining = new ArrayList<Hash>(hashes.size());
      final MutableSet<Hash> known = Sets.mutable.ofInitialCapacity(hashes.size());
      for (final Hash hash : hashes) {
        if (this.primitivesInventory.containsKey(hash)) known.add(hash);
        else remaining.add(hash);
      }

      if (EphemeralPrimitive.class.isAssignableFrom(primitive) == false) {
        for (final Hash hash : remaining) {
          if (has(null, StateAddress.from(primitive, hash)).equals(OperationStatus.SUCCESS))
            known.add(hash);
        }
      }

      return known.asUnmodifiable();
    } catch (Throwable t) {
      if (t instanceof DatabaseException) throw (DatabaseException) t;
      else throw new DatabaseException(t);
    }
  }

  boolean has(final Hash hash, final Class<? extends Primitive> primitive)
      throws DatabaseException {
    Objects.requireNonNull(hash, "Hash is null");
    Hash.notZero(hash, "Hash is ZERO");

    try {
      if (this.primitivesInventory.containsKey(hash)) return true;

      if (EphemeralPrimitive.class.isAssignableFrom(primitive) == false) {
        if (has(null, StateAddress.from(primitive, hash)).equals(OperationStatus.SUCCESS))
          return true;
      }

      return false;
    } catch (Throwable t) {
      if (t instanceof DatabaseException) throw (DatabaseException) t;
      else throw new DatabaseException(t);
    }
  }

  private OperationStatus store(final Transaction transaction, final Primitive primitive)
      throws IOException {
    Objects.requireNonNull(primitive, "Primitive is null");

    final StateAddress stateAddress = StateAddress.from(primitive.getClass(), primitive.getHash());
    final Substate substate = new Substate(stateAddress).set("primitive", primitive);
    final SubstateCommit substateCommit = new SubstateCommit(substate, System.currentTimeMillis());
    return store(transaction, substateCommit, false);
  }

  // Stores a hash which references a substate
  private OperationStatus store(final Transaction transaction, Hash referrer, Hash reference)
      throws IOException {
    Objects.requireNonNull(referrer, "Referring hash is null");
    Objects.requireNonNull(reference, "Reference hash is null");

    final DatabaseEntry key = keyEntry.get();
    key.setData(referrer.toByteArray());
    final DatabaseEntry value = valueEntry.get();
    value.setData(reference.toByteArray());
    final OperationStatus status =
        this.substates.putNoOverwrite(transaction, key, value, LockMode.DEFAULT);
    if (status.equals(OperationStatus.SUCCESS) == false)
      throw new DatabaseException(
          "Failed to store reference to "
              + reference
              + " via referrer "
              + referrer
              + " due to "
              + status.name());

    return OperationStatus.SUCCESS;
  }

  final OperationStatus store(final Primitive primitive) throws IOException {
    Objects.requireNonNull(primitive, "Primitive is null");

    OperationStatus status = OperationStatus.SUCCESS;
    if (CompoundPrimitive.class.isAssignableFrom(primitive.getClass()) == false) {
      // Enabled to output serializer stats for everything
      if (Serialization.DEBUG_STATISTICS) {
        if (primitive instanceof StateVoteBlock svb)
          Serialization.getInstance().toDson(svb.getHeader(), Output.WIRE);
        else Serialization.getInstance().toDson(primitive, Output.WIRE);
      }

      if (this.primitivesInventory.putIfAbsent(
              primitive.getHash(), new InventoryRecord(primitive, this.inventoryHead))
          != null) {
        databaseLog.warn(
            this.context.getName()
                + ": "
                + primitive.getClass()
                + " "
                + primitive.getHash()
                + " is already present in primitives");
        return OperationStatus.KEYEXIST;
      }
    }

    // TODO Special case for some primitives (primarily Atoms and BlockHeaders) as we want them to
    // be known by the LedgerStore for
    //		use with gossip and syncing other nodes etc, but we don't want to actually write them to
    // storage at this point.
    //
    //		These primitives are persisted later under some condition being met such as a proposal
    // containing a "completion"
    //		object (a certificate or timeout) being committed.
    //
    //		Storing them at this stage has a significant performance penalty later on, so we want to
    // defer the write.
    //
    //		This is a bit hacky though, perhaps it and can be improved.
    if (primitive.isDeferredPersist()) return OperationStatus.SUCCESS;

    if (EphemeralPrimitive.class.isAssignableFrom(primitive.getClass()) == false) {
      final Transaction transaction = new Transaction(this.context.getVamosEnvironment());
      try {
        status = store(transaction, primitive);
        if (status.equals(OperationStatus.SUCCESS) == false) {
          if (status.equals(OperationStatus.KEYEXIST)) {
            databaseLog.warn(
                this.context.getName()
                    + ": "
                    + primitive.getClass()
                    + " "
                    + primitive.getHash()
                    + " is already present");
            transaction.abort();
            return status;
          } else
            throw new DatabaseException(
                "Failed to store "
                    + primitive.getClass()
                    + " "
                    + primitive.getHash()
                    + " due to "
                    + status.name());
        }

        transaction.commit();

        if (databaseLog.hasLevel(Logging.DEBUG))
          databaseLog.debug(
              this.context.getName()
                  + ": Stored primitive "
                  + primitive.getClass()
                  + " "
                  + primitive.getHash());

        status = OperationStatus.SUCCESS;
      } catch (Exception ex) {
        transaction.abort();
        if (ex instanceof DatabaseException) throw ex;
        throw new DatabaseException(ex);
      }
    }

    return status;
  }

  final OperationStatus store(final Collection<Primitive> primitives) throws IOException {
    Objects.requireNonNull(primitives, "Primitives is null");
    Numbers.isZero(primitives.size(), "Primitives is empty");

    // TODO abort / clean up inventory on failure

    OperationStatus status = OperationStatus.SUCCESS;
    final List<Primitive> remaining = new ArrayList<Primitive>(primitives);
    final Iterator<Primitive> remainingIterator = remaining.iterator();
    while (remainingIterator.hasNext()) {
      final Primitive primitive = remainingIterator.next();

      if (CompoundPrimitive.class.isAssignableFrom(primitive.getClass()) == false) {
        if (this.primitivesInventory.putIfAbsent(
                primitive.getHash(), new InventoryRecord(primitive, this.inventoryHead))
            != null) return OperationStatus.KEYEXIST;

        //				this.primitivesLookup.put(primitive.getClass(), primitive.getHash());
      }

      if (EphemeralPrimitive.class.isAssignableFrom(primitive.getClass()))
        remainingIterator.remove();
    }

    final Transaction transaction =
        new Transaction(this.context.getVamosEnvironment(), primitives.size());
    try {
      for (final Primitive primitive : remaining) {
        final StateAddress stateAddress =
            StateAddress.from(primitive.getClass(), primitive.getHash());
        final Substate substate = new Substate(stateAddress).set("primitive", primitive);
        final SubstateCommit substateCommit =
            new SubstateCommit(substate, System.currentTimeMillis());

        status = store(transaction, substateCommit, false);
        if (status.equals(OperationStatus.SUCCESS) == false) {
          if (status.equals(OperationStatus.KEYEXIST)) {
            databaseLog.warn(
                this.context.getName()
                    + ": "
                    + primitive.getClass()
                    + " "
                    + primitive.getHash()
                    + " is already present");
            transaction.abort();
            return status;
          } else
            throw new DatabaseException(
                "Failed to store "
                    + primitive.getClass()
                    + " "
                    + primitive.getHash()
                    + " due to "
                    + status.name());
        }
      }

      transaction.commit();

      status = OperationStatus.SUCCESS;
    } catch (Exception ex) {
      transaction.abort();
      if (ex instanceof DatabaseException) throw ex;
      throw new DatabaseException(ex);
    }

    return status;
  }

  // STATE //
  final SubstateCommit search(final StateAddress stateAddress) throws IOException {
    Objects.requireNonNull(stateAddress, "State address is null");

    try {
      return getSubstateCommit(null, stateAddress);
    } catch (Exception ex) {
      if (ex instanceof DatabaseException) throw ex;
      else throw new DatabaseException(ex);
    }
  }

  final Map<StateAddress, SubstateCommit> search(final Collection<StateAddress> stateAddresses)
      throws IOException {
    Objects.requireNonNull(stateAddresses, "State addresses is null");
    if (stateAddresses.isEmpty()) return Collections.emptyMap();

    final Transaction transaction =
        new Transaction(this.context.getVamosEnvironment(), stateAddresses.size() * 2);
    try {
      final MutableMap<StateAddress, SubstateCommit> substateCommits =
          Maps.mutable.ofInitialCapacity(stateAddresses.size());
      for (final StateAddress stateAddress : stateAddresses) {
        final SubstateCommit substateCommit = getSubstateCommit(transaction, stateAddress);
        if (substateCommit != null) substateCommits.put(stateAddress, substateCommit);
      }

      return substateCommits.asUnmodifiable();
    } catch (Exception ex) {
      if (ex instanceof DatabaseException) throw ex;
      else throw new DatabaseException(ex);
    } finally {
      transaction.commit();
    }
  }

  @Override
  public final boolean has(final StateAddress stateAddress) throws IOException {
    final OperationStatus status = has(null, stateAddress);
    if (status.equals(OperationStatus.SUCCESS) == false) return false;

    return true;
  }

  private OperationStatus has(final Transaction transaction, final StateAddress stateAddress)
      throws IOException {
    Objects.requireNonNull(stateAddress, "State address is null");

    try {
      if (this.substateInventory.containsKey(stateAddress)) return OperationStatus.SUCCESS;

      final DatabaseEntry stateKey = keyEntry.get();
      stateKey.setData(stateAddress.getHash().toByteArray());
      final OperationStatus status =
          this.substates.get(transaction, stateKey, null, LockMode.DEFAULT);
      return status;
    } catch (Exception ex) {
      if (ex instanceof DatabaseException) throw ex;
      else throw new DatabaseException(ex);
    }
  }

  @Override
  public Substate get(final StateAddress stateAddress) throws IOException {
    Objects.requireNonNull(stateAddress, "State address is null");

    try {
      final SubstateCommit substateCommit = getSubstateCommit(null, stateAddress);
      if (substateCommit == null) return new Substate(stateAddress);

      return substateCommit.getSubstate();
    } catch (Exception ex) {
      if (ex instanceof DatabaseException) throw ex;
      else throw new DatabaseException(ex);
    }
  }

  final Map<StateAddress, Substate> get(final Collection<StateAddress> stateAddresses)
      throws IOException {
    Objects.requireNonNull(stateAddresses, "State addresses is null");
    if (stateAddresses.isEmpty()) return Collections.emptyMap();

    try {
      final Map<StateAddress, Substate> substates =
          new HashMap<StateAddress, Substate>(stateAddresses.size(), 1.0f);
      for (final StateAddress stateAddress : stateAddresses) {
        final Substate substate;
        final SubstateCommit substateCommit = getSubstateCommit(null, stateAddress);
        if (substateCommit == null) substate = new Substate(stateAddress);
        else substate = substateCommit.getSubstate();

        substates.put(stateAddress, substate);
      }

      return substates;
    } catch (Exception ex) {
      if (ex instanceof DatabaseException) throw ex;
      else throw new DatabaseException(ex);
    }
  }

  final OperationStatus commit(
      final Block block, final Collection<CommitOperation> commitOperations) throws IOException {
    Objects.requireNonNull(block, "Block to commit is null");

    OperationStatus status;
    final Transaction transaction =
        new Transaction(this.context.getVamosEnvironment(), commitOperations.size() * 2);
    try {
      final StateAddress blockHeaderStateAddress =
          StateAddress.from(BlockHeader.class, block.getHeader().getHash());
      status = store(transaction, block.getHeader());
      if (status.equals(OperationStatus.SUCCESS) == false)
        throw new DatabaseException(
            "Failed to store " + block.getHash() + " block header due to " + status.name());
      block.getHeader().flushCachedDsonOutput();

      if (block.getHash().equals(Universe.getDefault().getGenesis().getHash()) == false) {
        status =
            has(transaction, StateAddress.from(BlockHeader.class, block.getHeader().getPrevious()));
        if (status.equals(OperationStatus.SUCCESS) == false)
          throw new IllegalStateException(
              "Previous block "
                  + block.getHeader().getPrevious()
                  + " not found in state for "
                  + block.getHash());
      }

      // Store a reference to the block header substate as the block substate
      final StateAddress blockStateAddress =
          StateAddress.from(Block.class, block.getHeader().getHash());
      status = store(transaction, blockStateAddress.getHash(), blockHeaderStateAddress.getHash());
      if (status.equals(OperationStatus.SUCCESS) == false)
        throw new IllegalStateException(
            "Failed to store reference to block header " + block.getHash() + " substate for block");

      final Collection<Atom> accepted = block.getAccepted();
      final Collection<Atom> unaccepted = block.getUnaccepted();
      final Collection<AtomCertificate> certificates = block.getCertificates();
      final Collection<AtomTimeout> timeouts =
          new ArrayList<AtomTimeout>(
              unaccepted.stream()
                  .map(a -> new AcceptTimeout(a.getHash()))
                  .collect(Collectors.toList()));
      timeouts.addAll(block.getUnexecuted());
      timeouts.addAll(block.getUncommitted());
      final Collection<PolyglotPackage> packages = block.getPackages();

      for (final Atom atom : accepted) {
        final StateAddress atomStateAddress = StateAddress.from(Atom.class, atom.getHash());
        final Substate atomSubstate =
            new Substate(atomStateAddress, ComputeKey.NULL.getIdentity())
                .set("block", block.getHeader().getHash())
                .set("atom", atom.getHash())
                .set("primitive", atom);
        final SubstateCommit atomSubstateCommit =
            new SubstateCommit(
                atomSubstate,
                block.getHeader().getIndexOf(atom.getHash()),
                block.getHeader().getTimestamp());

        pushSubstateCommit(atomSubstateCommit);
        status = store(transaction, atomSubstateCommit, false);
        if (status.equals(OperationStatus.SUCCESS) == false)
          throw new IllegalStateException(
              "Failed to store accepted atom substate "
                  + atom.getHash()
                  + " in proposal "
                  + block.getHash());
      }

      for (final Atom atom : unaccepted) {
        final StateAddress atomStateAddress = StateAddress.from(Atom.class, atom.getHash());
        final Substate atomSubstate =
            new Substate(atomStateAddress, ComputeKey.NULL.getIdentity())
                .set("block", block.getHeader().getHash())
                .set("atom", atom.getHash())
                .set("unaccepted", 1)
                .set("primitive", atom);
        final SubstateCommit atomSubstateCommit =
            new SubstateCommit(
                atomSubstate,
                block.getHeader().getIndexOf(atom.getHash()),
                block.getHeader().getTimestamp());

        pushSubstateCommit(atomSubstateCommit);
        status = store(transaction, atomSubstateCommit, false);
        if (status.equals(OperationStatus.SUCCESS) == false)
          throw new IllegalStateException(
              "Failed to store unaccepted atom substate "
                  + atom.getHash()
                  + " in proposal "
                  + block.getHash());
      }

      // TODO check that the commit operation atoms have certificates in the proposal
      // TODO implement the merkle proofs here (are they actually needed?)
      for (final CommitOperation commitOperation : commitOperations) {
        for (final SubstateTransitions substateTransitions :
            commitOperation.getSubstateTransitions()) {
          SubstateCommit substateCommit = null;
          SubstateCommit committedSubstate =
              getSubstateCommit(transaction, substateTransitions.getAddress());
          if (committedSubstate == null) {
            substateCommit =
                new SubstateCommit(
                    substateTransitions.toSubstate(SubstateOutput.TRANSITIONS),
                    commitOperation.getAtom().getHash(),
                    commitOperation.getHead().getIndexOf(commitOperation.getAtom().getHash()),
                    commitOperation.getTimestamp());
          } else {
            committedSubstate.getSubstate().merge(substateTransitions);
            substateCommit =
                new SubstateCommit(
                    committedSubstate.getSubstate(),
                    commitOperation.getAtom().getHash(),
                    commitOperation.getHead().getIndexOf(commitOperation.getAtom().getHash()),
                    commitOperation.getTimestamp(),
                    committedSubstate);
          }

          status = store(transaction, substateCommit, true);
          if (status.equals(OperationStatus.SUCCESS) == false)
            throw new IllegalStateException(
                "Failed to store substate "
                    + substateTransitions.getAddress()
                    + " in proposal "
                    + block.getHash());
        }

        for (Entry<StateAddress, Hash> association : commitOperation.getAssociations()) {
          AssociationCommit associationCommit =
              new AssociationCommit(
                  association.getValue(),
                  association.getKey(),
                  commitOperation.getHead().getIndexOf(commitOperation.getAtom().getHash()),
                  commitOperation.getTimestamp());
          status = store(transaction, associationCommit);
          if (status.equals(OperationStatus.SUCCESS) == false)
            throw new IllegalStateException(
                "Failed to store association "
                    + association.getValue()
                    + ":"
                    + association.getKey()
                    + " in proposal "
                    + block.getHash());
        }

        if (commitOperation.getSubstateOpLog() != null) {
          StateAddress atomStateAddress =
              StateAddress.from(Atom.class, commitOperation.getAtom().getHash());
          SubstateCommit atomSubstateCommit = peekSubstateCommit(transaction, atomStateAddress);
          atomSubstateCommit
              .getSubstate()
              .set("substate.oplog", commitOperation.getSubstateOpLog());
        }
      }

      for (final AtomTimeout timeout : timeouts) {
        final StateAddress atomStateAddress = StateAddress.from(Atom.class, timeout.getAtom());
        final SubstateCommit atomSubstateCommit = peekSubstateCommit(transaction, atomStateAddress);
        atomSubstateCommit.getSubstate().set("timeout", timeout);
        pullSubstateCommit(atomSubstateCommit.getSubstate().getAddress());
        status = store(transaction, atomSubstateCommit, true);
        if (status.equals(OperationStatus.SUCCESS) == false)
          throw new IllegalStateException(
              "Failed to store timedout atom substate "
                  + timeout.getAtom()
                  + " in proposal "
                  + block.getHash());

        status = store(transaction, timeout.getHash(), atomStateAddress.getHash());
        if (status.equals(OperationStatus.SUCCESS) == false)
          throw new IllegalStateException(
              "Failed to store timeout association to atom substate "
                  + timeout.getAtom()
                  + " in proposal "
                  + block.getHash());
      }

      for (final AtomCertificate certificate : certificates) {
        final StateAddress atomStateAddress = StateAddress.from(Atom.class, certificate.getAtom());
        final SubstateCommit atomSubstateCommit = peekSubstateCommit(transaction, atomStateAddress);
        atomSubstateCommit.getSubstate().set("certificate", certificate);
        pullSubstateCommit(atomSubstateCommit.getSubstate().getAddress());
        status = store(transaction, atomSubstateCommit, true);
        if (status.equals(OperationStatus.SUCCESS) == false)
          throw new IllegalStateException(
              "Failed to store committed atom substate "
                  + certificate.getAtom()
                  + " in proposal "
                  + block.getHash());

        status = store(transaction, certificate.getHash(), atomStateAddress.getHash());
        if (status.equals(OperationStatus.SUCCESS) == false)
          throw new IllegalStateException(
              "Failed to store certificate association to atom substate "
                  + certificate.getAtom()
                  + " in proposal "
                  + block.getHash());

        // Update the sync inventory record for the relevant Atom and its state information
        final InventoryRecord atomInventoryRecord =
            this.primitivesInventory.get(certificate.getAtom());
        if (atomInventoryRecord == null) {
          if (databaseLog.hasLevel(Logging.DEBUG))
            databaseLog.warn(
                this.context.getName()
                    + ": Inventory record not found for commit atom "
                    + certificate.getAtom());
        } else atomInventoryRecord.setClockEnd(block.getHeader().getHeight());

        for (final StateOutput stateOutput : certificate.getInventory(StateOutput.class)) {
          if (stateOutput instanceof StateCertificate stateCertificate) {
            final InventoryRecord stateOutputInventoryRecord =
                this.primitivesInventory.get(stateCertificate.getHash());
            if (stateOutputInventoryRecord == null) {
              if (databaseLog.hasLevel(Logging.DEBUG))
                databaseLog.warn(
                    this.context.getName()
                        + ": Inventory record not found for state certificate "
                        + stateCertificate.getHash()
                        + " in commit atom "
                        + certificate.getAtom());
            } else stateOutputInventoryRecord.setClockEnd(block.getHeader().getHeight());
          }

          final Hash stateInputHash =
              Hash.hash(certificate.getAtom(), stateOutput.getAddress().getHash());
          final InventoryRecord stateInputInventoryRecord =
              this.primitivesInventory.get(stateInputHash);
          if (stateInputInventoryRecord == null) {
            if (databaseLog.hasLevel(Logging.DEBUG))
              databaseLog.warn(
                  this.context.getName()
                      + ": Inventory record not found for state input "
                      + stateInputHash
                      + " in commit atom "
                      + certificate.getAtom());
          } else stateInputInventoryRecord.setClockEnd(block.getHeader().getHeight());
        }

        // Can flush the DSON caches of Atom, StateCertificate and AtomCertificate
        final InventoryRecord atomRecord = this.primitivesInventory.get(certificate.getAtom());
        if (atomRecord == null) {
          if (databaseLog.hasLevel(Logging.DEBUG))
            databaseLog.warn(
                this.context.getName()
                    + ": Inventory record not found for commit atom "
                    + certificate.getAtom());
        } else atomRecord.<Atom>get().flushCachedDsonOutput();

        for (final StateCertificate stateCertificate :
            certificate.getInventory(StateCertificate.class)) {
          stateCertificate.getStates().flushCachedDsonOutput();
          stateCertificate.flushCachedDsonOutput();
        }
        certificate.flushCachedDsonOutput();
      }

      for (final PolyglotPackage pakage : packages) {
        final StateAddress packageStateAddress =
            StateAddress.from(PolyglotPackage.class, pakage.getHash());
        final SubstateCommit packageSubstateCommit =
            getSubstateCommit(transaction, packageStateAddress);
        if (packageSubstateCommit == null)
          throw new IllegalStateException(
              "Failed to load substate commit for package "
                  + packageStateAddress
                  + " at "
                  + pakage.getAddress());

        packageSubstateCommit.getSubstate().set(NativeField.BLOCK, block.getHash());
        final SubstateCommit updatedPackageSubstateCommit =
            new SubstateCommit(
                packageSubstateCommit.getSubstate(),
                block.getHeader().getHash(),
                block.getHeader().getIndexOf(pakage.getHash()),
                block.getHeader().getTimestamp());

        status = store(transaction, updatedPackageSubstateCommit, true);
        if (status.equals(OperationStatus.SUCCESS) == false)
          throw new IllegalStateException(
              "Failed to store substate for package "
                  + packageStateAddress
                  + " at "
                  + pakage.getAddress()
                  + " in proposal "
                  + block.getHash());

        status = store(transaction, pakage.getAddress().getHash(), packageStateAddress.getHash());
        if (status.equals(OperationStatus.SUCCESS) == false)
          throw new IllegalStateException(
              "Failed to store package reference "
                  + pakage.getHash()
                  + " for "
                  + pakage.getAddress()
                  + " in proposal "
                  + block.getHash());
      }

      final DatabaseEntry syncKey = keyEntry.get();
      syncKey.setData(Longs.toByteArray(block.getHeader().getHeight()));
      final DatabaseEntry syncValue = valueEntry.get();
      syncValue.setData(block.getHeader().getHash().toByteArray());
      status = this.syncChain.putNoOverwrite(transaction, syncKey, syncValue, LockMode.DEFAULT);
      if (status.equals(OperationStatus.SUCCESS) == false)
        throw new DatabaseException(
            "Failed to commit to sync chain " + block.getHash() + " due to " + status.name());

      transaction.commit();

      this.inventoryHead = block.getHash();

      return status;
    } catch (Throwable t) {
      databaseLog.error(this.context.getName() + ": Block commit aborting", t);
      transaction.abort();

      if (t instanceof DatabaseException) throw t;

      throw new DatabaseException(t);
    } finally {
      if (has(StateAddress.from(BlockHeader.class, block.getHeader().getHash())) == false)
        throw new IllegalStateException(
            "Block " + block.getHeader().getHash() + " not found in state for " + block.getHash());
    }
  }

  private SubstateCommit getSubstateCommit(
      final Transaction transaction, final StateAddress stateAddress) throws IOException {
    // TODO need isolation control here for substates which are currently being mutated?

    final DatabaseEntry key = keyEntry.get();
    key.setData(stateAddress.getHash().toByteArray());
    final DatabaseEntry value = valueEntry.get();
    OperationStatus status = this.substates.get(transaction, key, value, LockMode.DEFAULT);
    if (status.equals(OperationStatus.SUCCESS)) {
      // Is a reference?
      if (value.getSize() == Hash.BYTES) {
        key.setData(value.getData());
        status = this.substates.get(transaction, key, value, LockMode.DEFAULT);
      }

      if (status.equals(OperationStatus.SUCCESS))
        return Serialization.getInstance().fromDson(value.getData(), SubstateCommit.class);
    }

    return null;
  }

  private void pushSubstateCommit(final SubstateCommit substateCommit) throws IOException {
    synchronized (this.substateInventory) {
      if (this.substateInventory.containsKey(substateCommit.getSubstate().getAddress()))
        throw new DatabaseException(
            "Failed to push substate commit "
                + substateCommit
                + " due to "
                + OperationStatus.KEYEXIST);

      this.substateInventory.put(substateCommit.getSubstate().getAddress(), substateCommit);
    }
  }

  private SubstateCommit peekSubstateCommit(
      final Transaction transaction, final StateAddress stateAddress) throws IOException {
    synchronized (this.substateInventory) {
      SubstateCommit substateCommit = this.substateInventory.get(stateAddress);
      if (substateCommit == null) {
        substateCommit = getSubstateCommit(transaction, stateAddress);
        if (substateCommit == null)
          throw new DatabaseException(
              "Failed to peek substate commit "
                  + stateAddress
                  + " due to "
                  + OperationStatus.NOTFOUND);

        pushSubstateCommit(substateCommit);
      }

      return substateCommit;
    }
  }

  private SubstateCommit pullSubstateCommit(final StateAddress stateAddress) throws IOException {
    synchronized (this.substateInventory) {
      final SubstateCommit substateCommit = this.substateInventory.remove(stateAddress);
      if (substateCommit == null)
        throw new DatabaseException(
            "Failed to pull substate commit "
                + stateAddress
                + " due to "
                + OperationStatus.NOTFOUND);

      return substateCommit;
    }
  }

  private OperationStatus store(
      final Transaction transaction, final SubstateCommit substateCommit, final boolean update)
      throws IOException {
    final ByteBuffer byteBuffer = LedgerStore.byteBuffer.get().clear();
    final ByteBufferOutputStream byteBufferOutputStream =
        new ByteBufferOutputStream(byteBuffer, true);
    Serialization.getInstance().toDson(byteBufferOutputStream, substateCommit, Output.PERSIST);

    final DatabaseEntry key = keyEntry.get();
    key.setData(substateCommit.getSubstate().getAddress().getHash().toByteArray());
    final DatabaseEntry value = valueEntry.get();
    value.setData(
        byteBufferOutputStream.getByteBuffer().array(),
        0,
        byteBufferOutputStream.getByteBuffer().position());
    final OperationStatus status;

    if (update) status = this.substates.put(transaction, key, value, LockMode.DEFAULT);
    else status = this.substates.putNoOverwrite(transaction, key, value, LockMode.DEFAULT);

    if (status.equals(OperationStatus.SUCCESS) == false)
      throw new DatabaseException(
          "Failed to "
              + (update ? "update" : "store")
              + " substate "
              + substateCommit.getSubstate().getAddress()
              + " due to "
              + status.name());

    return status;
  }

  private OperationStatus store(final Transaction transaction, final AssociationCommit association)
      throws IOException {
    final DatabaseEntry key = keyEntry.get();
    key.setData(association.getReferrer().toByteArray());
    final DatabaseEntry value = valueEntry.get();
    value.setData(Serialization.getInstance().toDson(association, Output.PERSIST));
    final OperationStatus status = this.associations.put(transaction, key, value, LockMode.DEFAULT);
    if (status.equals(OperationStatus.SUCCESS) == false)
      throw new DatabaseException(
          "Failed to commit assiociation " + association + " due to " + status.name());

    return status;
  }

  // SYNC //
  boolean has(final long height) throws DatabaseException {
    Numbers.isNegative(height, "Height is negative");

    try {
      final DatabaseEntry key = keyEntry.get();
      key.setData(Longs.toByteArray(height));
      final OperationStatus status = this.syncChain.get(null, key, null, LockMode.DEFAULT);
      if (status.equals(OperationStatus.SUCCESS)) return true;

      return false;
    } catch (Throwable t) {
      if (t instanceof DatabaseException) throw (DatabaseException) t;
      else throw new DatabaseException(t);
    }
  }

  Hash head() throws DatabaseException {
    try {
      final DatabaseEntry key = keyEntry.get();
      final DatabaseEntry value = valueEntry.get();
      long iteration = Long.SIZE - 2; // Start at 62 instead of 63
      long bestHeadHeight = 0;
      long searchSyncHeight = 1L << iteration; // Start at 2^61

      while (iteration >= 0) {
        key.setData(Longs.toByteArray(searchSyncHeight));
        final OperationStatus status = this.syncChain.get(null, key, value, LockMode.DEFAULT);
        if (status.equals(OperationStatus.SUCCESS)) {
          bestHeadHeight = searchSyncHeight;
          searchSyncHeight += (1L << (iteration - 1));
        } else searchSyncHeight -= (1L << (iteration - 1));

        iteration--;
      }

      key.setData(Longs.toByteArray(bestHeadHeight));
      final OperationStatus status = this.syncChain.get(null, key, value, LockMode.DEFAULT);
      if (status.equals(OperationStatus.SUCCESS)) return Hash.from(value.getData());

      return Universe.getDefault().getGenesis().getHash();
    } catch (Throwable t) {
      if (t instanceof DatabaseException) throw (DatabaseException) t;
      else throw new DatabaseException(t);
    }
  }

  // SYNC //
  Hash getSyncBlock(final long height) throws DatabaseException {
    Numbers.isNegative(height, "Height is negative");

    try {
      final DatabaseEntry key = keyEntry.get();
      key.setData(Longs.toByteArray(height));
      final DatabaseEntry value = valueEntry.get();
      final OperationStatus status = this.syncChain.get(null, key, value, LockMode.DEFAULT);
      if (status.equals(OperationStatus.SUCCESS)) return Hash.from(value.getData());

      return null;
    } catch (Throwable t) {
      if (t instanceof DatabaseException) throw (DatabaseException) t;
      else throw new DatabaseException(t);
    }
  }

  OperationStatus setSyncBlock(BlockHeader header) throws DatabaseException {
    try {
      final DatabaseEntry key = keyEntry.get();
      key.setData(Longs.toByteArray(header.getHeight()));
      final DatabaseEntry value = valueEntry.get();
      key.setData(header.getHash().toByteArray());
      final OperationStatus status =
          this.syncChain.putNoOverwrite(null, key, value, LockMode.DEFAULT);
      return status;
    } catch (Throwable t) {
      if (t instanceof DatabaseException) throw (DatabaseException) t;
      else throw new DatabaseException(t);
    }
  }

  List<InventoryRecord> getSyncInventory(final long clock, final Class<? extends Primitive> type)
      throws IOException {
    Objects.requireNonNull(type, "Primitive type class is null");
    Numbers.isNegative(clock, "Clock is negative");

    final List<InventoryRecord> items = new ArrayList<InventoryRecord>();
    for (final InventoryRecord inventoryRecord : this.primitivesInventory.values()) {
      if (inventoryRecord.clockStart != clock) continue;

      if (type.isAssignableFrom(inventoryRecord.clazz) == false) continue;

      if (inventoryRecord.isStale()) {
        if (databaseLog.hasLevel(Logging.DEBUG))
          databaseLog.debug(
              "Sync inventory at " + clock + " is skipping stale primitive " + inventoryRecord);

        continue;
      }

      items.add(inventoryRecord);
    }

    return Collections.unmodifiableList(items);
  }

  // IDENTIFIER SEARCH //
  final Collection<SubstateCommit> search(final AssociationSearchQuery query) throws IOException {
    Objects.requireNonNull(query, "Search query is null");

    final LinkedList<SubstateCommit> substateCommits = new LinkedList<SubstateCommit>();
    try {
      long indexCutoff = -1;
      for (final Hash association : query.getAssociations()) {
        long nextOffset = -1;
        final DatabaseEntry associationKey = keyEntry.get();
        associationKey.setData(association.toByteArray());
        final DatabaseEntry associationValue = valueEntry.get();

        final Cursor searchCursor = new Cursor(this.associations, associationKey);
        try {
          OperationStatus status;
          if (query.getOrder().equals(Order.DESCENDING))
            status = searchCursor.getLast(associationValue);
          else status = searchCursor.getFirst(associationValue);

          if (status.equals(OperationStatus.SUCCESS)) {
            do {
              final AssociationCommit associationCommit =
                  Serialization.getInstance()
                      .fromDson(associationValue.getData(), AssociationCommit.class);
              nextOffset = associationCommit.getIndex();

              if (query.getOrder().equals(Order.DESCENDING) == false
                  && nextOffset > query.getOffset()) break;

              if (query.getOrder().equals(Order.DESCENDING) == true
                  && nextOffset < query.getOffset()) break;

              if (query.getOrder().equals(Order.DESCENDING))
                status = searchCursor.getPrev(associationValue);
              else status = searchCursor.getNext(associationValue);
            } while (status.equals(OperationStatus.SUCCESS));

            if (indexCutoff != -1) {
              if (query.getOrder().equals(Order.ASCENDING) && nextOffset > indexCutoff
                  || query.getOrder().equals(Order.DESCENDING) && nextOffset < indexCutoff)
                continue;
            }

            while (status.equals(OperationStatus.SUCCESS)
                && substateCommits.size() < query.getLimit()) {
              if (query.getOrder().equals(Order.DESCENDING) == false
                      && nextOffset > query.getOffset()
                  || query.getOrder().equals(Order.DESCENDING) == true
                      && nextOffset < query.getOffset()) {
                // FIXME feels messy, probably just better to store a commit in the associations and
                // use that, would save a read too at the expense of a few more bytes per record
                final AssociationCommit associationCommit =
                    Serialization.getInstance()
                        .fromDson(associationValue.getData(), AssociationCommit.class);
                if (query.getFilterContext().isEmpty()
                    || query
                        .getFilterContext()
                        .equalsIgnoreCase(associationCommit.getAddress().context())) {
                  DatabaseEntry stateKey = keyEntry.get();
                  stateKey.setData(associationCommit.getAddress().getHash().toByteArray());
                  DatabaseEntry substateValue = valueEntry.get();
                  status = this.substates.get(null, stateKey, substateValue, LockMode.DEFAULT);
                  if (status.equals(OperationStatus.SUCCESS)) {
                    SubstateCommit substateCommit =
                        Serialization.getInstance()
                            .fromDson(substateValue.getData(), SubstateCommit.class);
                    substateCommits.add(substateCommit);
                  }
                }
              }

              if (query.getOrder().equals(Order.DESCENDING))
                status = searchCursor.getPrev(associationValue);
              else status = searchCursor.getNext(associationValue);
            }

            // Sort on index to find the highest next_offset needed to continue searching
            substateCommits.sort(
                (ss1, ss2) -> {
                  if (query.getOrder().equals(Order.NATURAL)) {
                    long id1 = ss1.getID();
                    long id2 = ss2.getID();

                    if (id1 < id2) return -1;
                    if (id1 > id2) return 1;

                    return 0;
                  } else {
                    long idx1 = ss1.getIndex();
                    long idx2 = ss2.getIndex();

                    if (idx1 < idx2) return query.getOrder().equals(Order.ASCENDING) ? -1 : 1;

                    if (idx1 > idx2) return query.getOrder().equals(Order.ASCENDING) ? 1 : -1;

                    return 0;
                  }
                });

            if (substateCommits.isEmpty() == false) {
              // Trim results if needed
              while (substateCommits.size() > query.getLimit()) substateCommits.removeLast();

              indexCutoff =
                  query.getOrder().equals(Order.NATURAL)
                      ? substateCommits.getLast().getID()
                      : substateCommits.getLast().getIndex();
            }
          }
        } finally {
          searchCursor.close();
        }
      }
    } catch (Throwable t) {
      if (t instanceof DatabaseException) throw (DatabaseException) t;
      else throw new DatabaseException(t);
    }

    return substateCommits;
  }
}
