package org.radix.hyperscale.database.vamos;

import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.eclipse.collections.api.factory.Sets;
import org.radix.hyperscale.database.DatabaseException;
import org.radix.hyperscale.database.vamos.IndexItem.Type;
import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;

public class Transaction {
  private static final Logger vamosLog = Logging.getLogger("vamos");

  private static final int MINIMUM_LOCK_QUOTA = 4;

  private final Environment environment;
  private final long id;
  private final int lockHint;
  private final Set<InternalKey> locked;
  private volatile Map<InternalKey, TransactionOperation> operations;

  private volatile boolean isComplete = false;
  private volatile Throwable thrown;

  public Transaction(final Environment environment) {
    this(environment, 16);
  }

  public Transaction(final Environment environment, final int lockHint) {
    this.environment = environment;
    this.id = Math.abs(ThreadLocalRandom.current().nextLong());
    this.lockHint = lockHint < MINIMUM_LOCK_QUOTA ? MINIMUM_LOCK_QUOTA : lockHint;
    this.locked = Sets.mutable.ofInitialCapacity(this.lockHint);

    // operations is lazy init as not all transactions will perform writes
    this.operations = null;
  }

  long ID() {
    return this.id;
  }

  private void lock(
      final Database database,
      final DatabaseEntry key,
      final InternalKey internalKey,
      final LockMode lockMode)
      throws DatabaseException {
    synchronized (this) {
      throwIfCompleted();

      try {
        this.environment
            .getLockManager()
            .lock(
                internalKey, this, this.environment.getConfig().getLockTimeout(), TimeUnit.SECONDS);

        this.locked.add(internalKey);
      } catch (LockInternalKeyTimeoutException tex) {
        vamosLog.error(
            this.environment.getDirectory().getName()
                + ": Lock timeout for key "
                + key
                + " in transaction "
                + this
                + " on database "
                + database
                + " -> "
                + this.locked);

        throw tex;
      } catch (InterruptedException iex) {
        Thread.currentThread().interrupt();
        throw new DatabaseException(
            "Lock interruption on key "
                + key
                + " in transaction "
                + this
                + " on database "
                + database);
      }
    }
  }

  private void unlock() {
    synchronized (this) {
      throwIfCompleted();

      if (this.locked.isEmpty() == false) {
        for (InternalKey key : this.locked) this.environment.getLockManager().unlock(key, this);
      }
    }
  }

  private void thrown(final Throwable thrown) {
    throwIfError();

    this.thrown = thrown;
  }

  private void throwIfCompletedOrError() {
    if (this.isComplete) throw new IllegalStateException("Transaction " + this + " is complete");

    if (this.thrown != null)
      throw new IllegalStateException("Transaction " + this + " threw " + this.thrown);
  }

  private void throwIfCompleted() {
    if (this.isComplete) throw new IllegalStateException("Transaction " + this + " is complete");
  }

  private void throwIfError() {
    if (this.thrown != null)
      throw new IllegalStateException("Transaction " + this + " threw " + this.thrown);
  }

  private void setCompleted() {
    throwIfCompleted();
    this.isComplete = true;
  }

  boolean exists(final Database database, final InternalKey internalKey) throws IOException {
    synchronized (this) {
      throwIfCompletedOrError();

      if (this.operations != null && this.operations.isEmpty() == false) {
        final TransactionOperation operation = this.operations.get(internalKey);
        if (operation != null) {
          if (operation.getOperation().equals(Operation.DELETE)) return false;
          if (operation.getOperation().equals(Operation.PUT)
              || operation.getOperation().equals(Operation.PUT_NO_OVERWRITE)) return true;
        }
      }

      return database.exists(this, internalKey);
    }
  }

  boolean hasOperations() {
    synchronized (this) {
      throwIfCompletedOrError();

      return (this.operations != null && this.operations.isEmpty() == false) ? true : false;
    }
  }

  List<TransactionOperation> getOperations() {
    synchronized (this) {
      throwIfCompletedOrError();

      return (this.operations == null || this.operations.isEmpty())
          ? Collections.emptyList()
          : Collections.unmodifiableList(new ArrayList<>(this.operations.values()));
    }
  }

  List<TransactionOperation> getOperations(final Database database) {
    synchronized (this) {
      throwIfCompletedOrError();

      if (this.operations != null && this.operations.isEmpty() == false) {
        final List<TransactionOperation> operations = new ArrayList<>(this.operations.size());
        for (final TransactionOperation operation : this.operations.values()) {
          if (operation.getKey().getDatabaseID() == database.getID()) operations.add(operation);
        }

        return Collections.unmodifiableList(operations);
      }

      return Collections.emptyList();
    }
  }

  public void commit() throws IOException {
    synchronized (this) {
      try {
        throwIfCompletedOrError();

        this.environment.getLog().commit(this);
      } catch (Throwable t) {
        thrown(t);
        throw t;
      } finally {
        unlock();
        setCompleted();
      }
    }
  }

  public void abort() throws IOException {
    synchronized (this) {
      try {
        throwIfCompleted();
      } catch (Throwable t) {
        thrown(t);
        throw t;
      } finally {
        unlock();
        setCompleted();
      }
    }
  }

  OperationStatus delete(
      final Database database,
      final DatabaseEntry key,
      final InternalKey internalKey,
      final IndexNodeID indexNodeID,
      final LockMode lockMode)
      throws IOException {
    synchronized (this) {
      throwIfCompleted();

      if (database.getConfig().getAllowDuplicates())
        throw new IllegalArgumentException(
            "Operation DELETE is not supported for databases that can have duplicates");

      lock(database, key, internalKey, lockMode);

      if (exists(database, internalKey) == false) return OperationStatus.NOTFOUND;

      final TransactionOperation transactionOperation =
          new TransactionOperation(Operation.DELETE, internalKey);
      if (this.operations == null) this.operations = new LinkedHashMap<>(this.lockHint);
      this.operations.put(internalKey, transactionOperation);
      return OperationStatus.SUCCESS;
    }
  }

  OperationStatus get(
      final Database database,
      final DatabaseEntry key,
      final DatabaseEntry value,
      final InternalKey internalKey,
      final IndexNodeID indexNodeID,
      final LockMode lockMode)
      throws IOException {
    synchronized (this) {
      throwIfCompleted();

      lock(database, key, internalKey, lockMode);

      // FIXME ISOLATION
      // TODO anything to do here for duplicates?
      if (database.getConfig().getAllowDuplicates() == false) {
        if (this.operations != null && this.operations.isEmpty() == false) {
          final TransactionOperation operation = this.operations.get(internalKey);
          if (operation != null) {
            if (operation.getOperation().equals(Operation.DELETE)) return OperationStatus.NOTFOUND;

            if (operation.getOperation().equals(Operation.PUT)
                || operation.getOperation().equals(Operation.PUT_NO_OVERWRITE)) {
              if (value != null) value.setData(operation.getData());

              return OperationStatus.SUCCESS;
            }
          }
        }
      }

      if (exists(database, internalKey) == false) return OperationStatus.NOTFOUND;

      final IndexItem indexItem = this.environment.getIndex().getIndexItem(this, internalKey);
      if (indexItem == null || indexItem == IndexItem.VACANT) return OperationStatus.NOTFOUND;

      if (value != null) {
        final byte[] logData;
        if (indexItem.getType() == Type.EXTENSION) {
          final ExtensionNode extensionNode =
              this.environment.getLog().readExtensionNode(indexItem);
          final ExtensionItem extensionItem =
              this.environment
                  .getLog()
                  .readExtensionItem(extensionNode.getKey(), extensionNode.getFirstPosition());
          logData = this.environment.getLog().readLogData(extensionItem.getLogPosition());
        } else logData = this.environment.getLog().readLogData(indexItem.getPosition());

        value.setData(logData);
      }

      return OperationStatus.SUCCESS;
    }
  }

  OperationStatus put(
      final Database database,
      final DatabaseEntry key,
      final DatabaseEntry value,
      final InternalKey internalKey,
      final IndexNodeID indexNodeID,
      final Operation operation,
      final LockMode lockMode)
      throws IOException {
    synchronized (this) {
      throwIfCompleted();

      lock(database, key, internalKey, lockMode);

      if (database.getConfig().getAllowDuplicates()) {
        if (operation.equals(Operation.PUT_NO_OVERWRITE))
          throw new IllegalArgumentException(
              "Put type NO_OVERWRITE is not supported for databases that can have duplicates");
      } else if (operation.equals(Operation.PUT_NO_OVERWRITE)) {
        if (exists(database, internalKey)) return OperationStatus.KEYEXIST;
      }

      final TransactionOperation transactionOperation =
          new TransactionOperation(
              operation, internalKey, value.getData(), value.getOffset(), value.getSize());
      if (this.operations == null) this.operations = new LinkedHashMap<>(this.lockHint);
      this.operations.put(internalKey, transactionOperation);
      return OperationStatus.SUCCESS;
    }
  }

  public boolean isCompleted() {
    return this.isComplete;
  }

  public boolean hasLocks() {
    synchronized (this) {
      return this.locked.isEmpty() == false;
    }
  }

  @Override
  public String toString() {
    return "[id="
        + this.id
        + " locks="
        + this.locked.size()
        + " operations="
        + (this.operations == null ? 0 : this.operations.size())
        + "]";
  }
}
