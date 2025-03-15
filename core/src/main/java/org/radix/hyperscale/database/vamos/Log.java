package org.radix.hyperscale.database.vamos;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import org.apache.commons.lang3.mutable.MutableLong;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.factory.Maps;
import org.radix.hyperscale.database.DatabaseException;
import org.radix.hyperscale.database.vamos.IndexItem.Type;
import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;
import org.radix.hyperscale.utils.Ints;

class Log {
  private static final Logger vamosLog = Logging.getLogger("vamos");

  /* Thread local ByteBuffer for serializing log operations */
  private static final ThreadLocal<ByteBuffer> byteBuffer =
      ThreadLocal.withInitial(() -> ByteBuffer.allocate(1 << 24));

  private static final int LOG_HEADER_LENGTH = 16;

  private final Environment environment;

  // LOG //
  private final RandomAccessFile logFile;
  private final AtomicLong nextLogID;
  private final AtomicLong nextLogPosition;
  private final AtomicLong lastLogPosition;

  // EXTENSIONS //
  private final RandomAccessFile extensionFile;
  private final AtomicLong nextExtensionPosition;
  private final AtomicLong lastExtensionPosition;

  // CHECKPOINTS
  private volatile long checkpointBytesRemaining;
  private volatile long lastCheckpointTimestamp;
  private volatile long checkpointLogPosition;

  private AtomicLong logReads;
  private AtomicLong logWrites;
  private AtomicLong logReadBytes;
  private AtomicLong logWriteBytes;
  private AtomicLong logOperations;
  private AtomicLong contiguousLogReads;
  private AtomicLong contiguousLogWrites;

  private AtomicLong extensionNodeReads;
  private AtomicLong extensionNodeWrites;
  private AtomicLong extensionItemReads;
  private AtomicLong extensionItemWrites;
  private AtomicLong contiguousExtensionReads;
  private AtomicLong contiguousExtensionWrites;

  private class LogWorker implements Runnable {
    private final LinkedHashMap<Long, LogOperation> logOperations;
    private final List<LogOperation> logOperationsToWrite;
    private final LinkedHashMap<Long, ExtensionObject> extensionOperations;
    private final List<ExtensionObject> extensionOperationsToWrite;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private volatile boolean terminated = false;
    private volatile Thread thread;
    private volatile boolean signalled;
    private final AtomicReference<Thread> latch;

    LogWorker() {
      this.logOperations = new LinkedHashMap<>(1024);
      this.logOperationsToWrite = new ArrayList<>(1024);
      this.extensionOperations = new LinkedHashMap<>(1024);
      this.extensionOperationsToWrite = new ArrayList<>(1024);
      this.latch = new AtomicReference<>();
    }

    @Override
    public void run() {
      this.thread = Thread.currentThread();

      while (this.terminated == false) {
        if (this.signalled == false) {
          this.latch.set(this.thread);
          LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(1));
          if (this.latch.compareAndSet(this.thread, null)) continue;
        }
        this.signalled = false;

        try {
          boolean hasLogOperations = false;
          boolean hasExtensionOperations = false;

          this.lock.writeLock().lock();
          try {
            if (this.logOperations.isEmpty() == false) {
              hasLogOperations = true;
              this.logOperationsToWrite.addAll(this.logOperations.values());
            }

            if (this.extensionOperations.isEmpty() == false) {
              hasExtensionOperations = true;
              this.extensionOperationsToWrite.addAll(this.extensionOperations.values());
            }
          } finally {
            this.lock.writeLock().unlock();
          }

          if (hasLogOperations) Log.this.writeLogOperations(this.logOperationsToWrite);

          if (hasExtensionOperations)
            Log.this.writeExtensionOperations(this.extensionOperationsToWrite);

          this.lock.writeLock().lock();
          try {
            if (hasLogOperations) {
              for (final LogOperation logOperation : this.logOperationsToWrite) {
                if (this.logOperations.remove(logOperation.getLogPosition(), logOperation) == false)
                  throw new DatabaseException("Log operation not removed: " + logOperation);
              }
            }

            if (hasExtensionOperations) {
              for (final ExtensionObject extensionOperation : this.extensionOperationsToWrite) {
                if (this.extensionOperations.remove(
                        extensionOperation.getExtPosition(), extensionOperation)
                    == false)
                  throw new DatabaseException(
                      "Extension operation not removed: " + extensionOperation);
              }
            }
          } finally {
            this.lock.writeLock().unlock();
            this.logOperationsToWrite.clear();
            this.extensionOperationsToWrite.clear();
          }
        } catch (final Throwable t) {
          vamosLog.fatal(t);
          this.terminated = true;
        }
      }
    }

    private final void signal() {
      this.signalled = true;
      Thread t = this.latch.getAndSet(null);
      if (t != null) LockSupport.unpark(t);
    }

    private LogOperation getLogOperation(long position) {
      this.lock.readLock().lock();
      try {
        return this.logOperations.get(position);
      } finally {
        this.lock.readLock().unlock();
      }
    }

    private void queueLogOperations(final Collection<LogOperation> logOperations)
        throws IOException {
      if (this.terminated) throw new IllegalStateException("Log worker is terminated");

      this.lock.writeLock().lock();
      try {
        for (final LogOperation logOperation : logOperations) {
          if (this.logOperations.put(logOperation.getLogPosition(), logOperation) != null)
            throw new DatabaseException(
                "Log operation already present for position " + logOperation.getKey());
        }

        signal();
      } catch (IOException ioex) {
        for (final LogOperation logOperation : logOperations)
          this.logOperations.remove(logOperation.getLogPosition(), logOperation);

        throw ioex;
      } finally {
        this.lock.writeLock().unlock();
      }
    }

    @SuppressWarnings("unchecked")
    private <T> T getExtensionOperation(long position) {
      this.lock.readLock().lock();
      try {
        return (T) this.extensionOperations.get(position);
      } finally {
        this.lock.readLock().unlock();
      }
    }

    private void queueExtensionOperations(
        final Collection<Entry<Long, ExtensionObject>> extensionOperations) {
      if (this.terminated) throw new IllegalStateException("Log worker is terminated");

      this.lock.writeLock().lock();
      try {
        for (final Entry<Long, ExtensionObject> extensionOperation : extensionOperations)
          this.extensionOperations.put(extensionOperation.getKey(), extensionOperation.getValue());

        signal();
      } finally {
        this.lock.writeLock().unlock();
      }
    }
  }

  private final LogWorker logWorker = new LogWorker();

  Log(final Environment environment) throws IOException {
    this.environment = Objects.requireNonNull(environment, "Environment is null");

    this.checkpointLogPosition = Log.LOG_HEADER_LENGTH;
    this.checkpointBytesRemaining = this.environment.getConfig().getCheckpointAfterBytesWritten();
    this.lastCheckpointTimestamp = System.currentTimeMillis();
    this.lastLogPosition = new AtomicLong(-1);

    this.logOperations = new AtomicLong(0);
    this.logReads = new AtomicLong(0);
    this.logWrites = new AtomicLong(0);
    this.logReadBytes = new AtomicLong(0);
    this.logWriteBytes = new AtomicLong(0);
    this.contiguousLogReads = new AtomicLong(0);
    this.contiguousLogWrites = new AtomicLong(0);

    this.extensionNodeReads = new AtomicLong(0);
    this.extensionNodeWrites = new AtomicLong(0);
    this.extensionItemReads = new AtomicLong(0);
    this.extensionItemWrites = new AtomicLong(0);
    this.contiguousExtensionReads = new AtomicLong(0);
    this.contiguousExtensionWrites = new AtomicLong(0);

    if (new File(this.environment.getDirectory(), "vamos.vam").exists() == true) {
      this.logFile =
          new RandomAccessFile(new File(this.environment.getDirectory(), "vamos.vam"), "rw");

      readHeader();

      this.nextLogPosition = new AtomicLong(this.logFile.length());
      this.nextLogID = new AtomicLong(-1);
    } else {
      this.logFile =
          new RandomAccessFile(new File(this.environment.getDirectory(), "vamos.vam"), "rw");

      writeHeader();

      this.nextLogPosition = new AtomicLong(this.logFile.length());
      this.nextLogID = new AtomicLong(1); // TODO need recovery for this
    }

    this.extensionFile =
        new RandomAccessFile(new File(this.environment.getDirectory(), "vamos.ext"), "rw");
    this.nextExtensionPosition = new AtomicLong(this.extensionFile.length());
    this.lastExtensionPosition = new AtomicLong(-1);

    // TODO move to environment
    integrity();

    Thread logWorkerThread = new Thread(this.logWorker);
    logWorkerThread.setDaemon(true);
    logWorkerThread.setName("Log Worker");
    logWorkerThread.setPriority(7);
    logWorkerThread.start();
  }

  public void close() throws IOException {
    writeHeader();
    this.extensionFile.close();
    this.logFile.close();
  }

  // TODO move to environment
  private void integrity() throws IOException {
    long logPosition = this.checkpointLogPosition;
    long logLength = this.logFile.length();
    if (logPosition == logLength) return;

    // TODO should be possible to incorporate into main integrity logic
    long logID = -1;
    LogOperation logOperation = null;
    final Set<Long> txIDs = new HashSet<Long>();
    while (logPosition < logLength) {
      logOperation = readLogOperation(logPosition);
      logPosition += logOperation.length() + Integer.BYTES;

      if (logID != -1 && logID != logOperation.getID() - 1)
        throw new DatabaseException("LOGID " + logOperation.getID() + " sequence is corrupt");
      else logID = logOperation.getID();

      if (logOperation.getOperation().equals(Operation.START_TX)) {
        if (txIDs.add(logOperation.getTXID()) == false)
          throw new DatabaseException(
              "TXID "
                  + logOperation.getTXID()
                  + " is already known for log operation "
                  + logOperation);
      } else if (logOperation.getOperation().equals(Operation.END_TX)) {
        if (txIDs.remove(logOperation.getTXID()) == false)
          throw new DatabaseException(
              "TXID "
                  + logOperation.getTXID()
                  + " is not known or completed for log operation "
                  + logOperation);
      }
    }

    long indexItems = 0;
    long logTransactions = 0;
    logPosition = this.checkpointLogPosition;
    while (logPosition < logLength) {
      logOperation = readLogOperation(logPosition);

      try {
        if (logOperation.getOperation().equals(Operation.START_TX)) {
          if (vamosLog.hasLevel(Logging.DEBUG))
            vamosLog.debug("Starting integrity check on TXID " + logOperation.getTXID());

          continue;
        }

        if (logOperation.getOperation().equals(Operation.END_TX)) {
          logTransactions++;
          if (vamosLog.hasLevel(Logging.DEBUG))
            vamosLog.debug("Completed integrity check on TXID " + logOperation.getTXID());

          System.out.println(
              "Log TX = "
                  + logTransactions
                  + " Log ID = "
                  + logOperation.getID()
                  + " Index items = "
                  + indexItems);

          continue;
        }

        IndexNodeID indexNodeID = this.environment.toIndexNodeID(logOperation.getKey());
        IndexNode indexNode = this.environment.getIndex().readIndexNode(indexNodeID);

        // Hanging TX, revert to ancestor
        if (txIDs.contains(logOperation.getTXID())) {
          if (logOperation.getAncestorPointer() == -1) indexNode.delete(logOperation.getKey());
          else {
            IndexItem indexItem = indexNode.get(logOperation.getKey());
            indexItem =
                new IndexItem(
                    indexItem.getType(), indexItem.getKey(), logOperation.getAncestorPointer());
            indexNode.put(indexItem);
          }

          this.environment.getIndex().writeIndexNode(indexNode);
        } else {
          if (logOperation.getOperation().equals(Operation.DELETE)) {
            IndexItem indexItem = indexNode.get(logOperation.getKey());
            if (indexItem != null && indexItem != IndexItem.VACANT) {
              indexNode.delete(logOperation.getKey());
              vamosLog.warn(
                  "Index node "
                      + indexNode.getID()
                      + " contained unexpected item for key "
                      + logOperation.getKey());
            }
            indexItems++;
          } else if (logOperation.getOperation().equals(Operation.EXT_NODE)) {
            if (logOperation.isExtension() == false)
              throw new IllegalStateException(
                  "Log operations for EXT_NODE must point to an extension position");

            final IndexItem existingIndexItem = indexNode.get(logOperation.getKey());
            if (existingIndexItem == null) {
              final IndexItem indexItem =
                  new IndexItem(
                      Type.EXTENSION, logOperation.getKey(), logOperation.getExtensionPosition());
              indexNode.put(indexItem);

              vamosLog.warn(
                  "Index node "
                      + indexNode.getID()
                      + " did not contain expected extension node for key "
                      + logOperation.getKey());
            }
          } else if (logOperation.getOperation().equals(Operation.PUT)
              || logOperation.getOperation().equals(Operation.PUT_NO_OVERWRITE)) {
            final IndexItem existingIndexItem = indexNode.get(logOperation.getKey());

            // TODO incomplete
            // Still need to actually verify extension item integrity and item chain
            if (logOperation.isExtension()) {
              if (logOperation.getOperation().equals(Operation.PUT_NO_OVERWRITE))
                throw new IllegalArgumentException(
                    "Put type NO_OVERWRITE is not supported for databases that can have duplicates");

              if (existingIndexItem == null)
                throw new IllegalStateException(
                    "Index node "
                        + indexNode.getID()
                        + " did not contain extension node entry for key "
                        + logOperation.getKey());

              final ExtensionNode extensionNode = readExtensionNode(existingIndexItem);
              final ExtensionItem firstExtensionItem =
                  readExtensionItem(extensionNode.getFirstPosition());
              if (extensionNode.getFirstItem().equals(firstExtensionItem) == false)
                throw new DatabaseException(
                    "Extension item did not match expected "
                        + extensionNode.getFirstItem()
                        + " in "
                        + extensionNode);

              final ExtensionItem lastExtensionItem =
                  readExtensionItem(extensionNode.getLastPosition());
              if (extensionNode.getLastItem().equals(lastExtensionItem) == false)
                throw new DatabaseException(
                    "Extension item did not match expected "
                        + extensionNode.getFirstItem()
                        + " in "
                        + extensionNode);

              indexItems++;
            } else {
              IndexItem indexItem = new IndexItem(Type.DATA, logOperation.getKey(), logPosition);

              // TODO need to do this check on integrity?
              if (1 == 0 && logOperation.getOperation().equals(Operation.PUT_NO_OVERWRITE)) {
                if (existingIndexItem != null && indexItem.equals(existingIndexItem) == false)
                  throw new DatabaseException(
                      "PUT_NO_OVERWRITE violation for " + logOperation.getKey() + " on integrity");
              }

              if (existingIndexItem == null || indexItem.equals(existingIndexItem) == false) {
                if (existingIndexItem == null)
                  vamosLog.warn(
                      "Index node "
                          + indexNode.getID()
                          + " did not contain expected item for key "
                          + logOperation.getKey());
                else
                  vamosLog.info(
                      "Index node "
                          + indexNode.getID()
                          + " contained ancestor for key "
                          + logOperation.getKey());

                indexNode.put(indexItem);
              }

              indexItems++;
            }
          }
        }

        if (indexNode.modifications() > 0) this.environment.getIndex().writeIndexNode(indexNode);
      } finally {
        logPosition += logOperation.length() + Integer.BYTES;
      }
    }

    this.logFile.getFD().sync();

    this.nextLogID.set(logOperation.getID());
  }

  private void readHeader() throws IOException {
    synchronized (this.logFile) {
      this.logFile.seek(0);

      if (this.logFile.readInt() != this.environment.getConfig().getIndexNodeCount())
        throw new IOException("Index header may be corrupt or invalid config");

      if (this.logFile.readInt() != this.environment.getConfig().getIndexNodeSize())
        throw new IOException("Index header may be corrupt or invalid config");

      this.checkpointLogPosition = this.logFile.readLong();
      if (this.checkpointLogPosition <= 0)
        this.checkpointLogPosition = this.logFile.getFilePointer();
    }
  }

  private long writeHeader() throws IOException {
    synchronized (this.logFile) {
      this.logFile.seek(0);
      this.logFile.writeInt(this.environment.getConfig().getIndexNodeCount());
      this.logFile.writeInt(this.environment.getConfig().getIndexNodeSize());
      this.logFile.writeLong(this.checkpointLogPosition);
      this.logFile.getFD().sync();
      this.lastLogPosition.set(Log.LOG_HEADER_LENGTH);
      return this.lastLogPosition.get();
    }
  }

  private boolean tryCheckpoint(final long bytesWritten) throws IOException {
    synchronized (this.logFile) {
      boolean doCheckpoint = false;

      if (bytesWritten > this.checkpointBytesRemaining) doCheckpoint = true;
      if (System.currentTimeMillis() - this.lastCheckpointTimestamp
          > this.environment.getConfig().getCheckpointInterval(TimeUnit.MILLISECONDS))
        doCheckpoint = true;

      if (doCheckpoint == true) {
        vamosLog.info("Performing log checkpoint ...");
        writeHeader();
        vamosLog.info("... Log checkpoint completed");

        this.checkpointBytesRemaining =
            this.environment.getConfig().getCheckpointAfterBytesWritten();
        this.lastCheckpointTimestamp = System.currentTimeMillis();
      }

      return doCheckpoint;
    }
  }

  void commit(final Transaction transaction) throws IOException {
    synchronized (transaction) {
      int recordsDelta = 0;
      long bytesWrittenToLog = 0;
      long expectedBytesWrittenToLog = 0;

      final List<TransactionOperation> txOperations;
      final Map<InternalKey, IndexItem> indexItems;
      final Map<InternalKey, IndexItem> indexItemAncestors;

      if (transaction.hasOperations()) {
        txOperations = transaction.getOperations();
        indexItems = Maps.mutable.ofInitialCapacity(txOperations.size());
        indexItemAncestors = Maps.mutable.ofInitialCapacity(txOperations.size());
      } else {
        txOperations = null;
        indexItems = null;
        indexItemAncestors = null;
      }

      try {
        if (txOperations == null) return;

        if (vamosLog.hasLevel(Logging.DEBUG))
          vamosLog.debug("Starting commit of transaction " + transaction);

        // Cache the current open databases in the environment
        final Map<Integer, Database> databaseCache =
            this.environment.getDatabases().stream()
                .collect(Collectors.toMap(database -> database.getID(), database -> database));

        final Map<Long, ExtensionObject> extensionObjects =
            Maps.mutable.ofInitialCapacity(txOperations.size());

        // Log operations size is tx operation size + 2 for START_TX and END_TX operations
        final List<LogOperation> logOperations =
            Lists.mutable.ofInitialCapacity(txOperations.size() + 2);
        synchronized (this.nextLogPosition) {
          final MutableLong workerNextLogID = new MutableLong(this.nextLogID.get());
          final MutableLong workerNextLogPosition = new MutableLong(this.nextLogPosition.get());
          final MutableLong workerNextExtPosition =
              new MutableLong(this.nextExtensionPosition.get());

          final LogOperation startLogOperation =
              new LogOperation(
                  this.environment,
                  workerNextLogID.incrementAndGet(),
                  workerNextLogPosition.longValue(),
                  transaction.ID(),
                  Operation.START_TX);
          workerNextLogPosition.getAndAdd(startLogOperation.length() + Integer.BYTES);
          logOperations.add(startLogOperation);
          expectedBytesWrittenToLog += startLogOperation.length() + Integer.BYTES;

          // Verify operations and create log operations
          for (int t = 0; t < txOperations.size(); t++) {
            final TransactionOperation txOperation = txOperations.get(t);

            final Database database = databaseCache.get(txOperation.getKey().getDatabaseID());

            // Handle ancestors.  Should exclude index items created during this commit.
            IndexItem ancestorIndexItem = indexItemAncestors.get(txOperation.getKey());
            if (ancestorIndexItem == null) {
              ancestorIndexItem =
                  this.environment.getIndex().getIndexItem(transaction, txOperation.getKey());
              indexItemAncestors.put(txOperation.getKey(), ancestorIndexItem);
            }

            final LogOperation logOperation;
            if (txOperation.getOperation().equals(Operation.DELETE)) {
              logOperation =
                  new LogOperation(
                      this.environment,
                      workerNextLogID.incrementAndGet(),
                      workerNextLogPosition.longValue(),
                      transaction.ID(),
                      txOperation,
                      -1,
                      ancestorIndexItem == IndexItem.VACANT ? -1 : ancestorIndexItem.getPosition());
              workerNextLogPosition.getAndAdd(logOperation.length() + Integer.BYTES);
              indexItems.put(txOperation.getKey(), new IndexItem(txOperation.getKey()));
              recordsDelta--;
            } else if (txOperation.getOperation().equals(Operation.PUT)
                || txOperation.getOperation().equals(Operation.PUT_NO_OVERWRITE)) {
              IndexItem indexItem;
              if (database.getConfig().getAllowDuplicates()) {
                if (txOperation.getOperation().equals(Operation.PUT_NO_OVERWRITE))
                  throw new IllegalArgumentException(
                      "Put type NO_OVERWRITE is not supported for databases that can have duplicates");

                indexItem = indexItems.get(txOperation.getKey());
                if (indexItem == null)
                  indexItem =
                      this.environment.getIndex().getIndexItem(transaction, txOperation.getKey());

                ExtensionNode extensionNode;
                if (indexItem == IndexItem.VACANT) {
                  extensionNode =
                      new ExtensionNode(
                          txOperation.getKey(),
                          workerNextExtPosition.getAndAdd(ExtensionNode.BYTES));
                  indexItem =
                      new IndexItem(
                          Type.EXTENSION, txOperation.getKey(), extensionNode.getExtPosition());
                  indexItems.put(txOperation.getKey(), indexItem);

                  // Create a log operation to record the creation of the extension node
                  final LogOperation extensionNodeLogOperation =
                      new LogOperation(
                          this.environment,
                          workerNextLogID.incrementAndGet(),
                          workerNextLogPosition.longValue(),
                          transaction.ID(),
                          txOperation.getKey(),
                          extensionNode.getExtPosition(),
                          Operation.EXT_NODE);
                  workerNextLogPosition.getAndAdd(
                      extensionNodeLogOperation.length() + Integer.BYTES);

                  logOperations.add(extensionNodeLogOperation);
                  expectedBytesWrittenToLog += extensionNodeLogOperation.length() + Integer.BYTES;
                } else {
                  extensionNode = (ExtensionNode) extensionObjects.get(indexItem.getPosition());
                  if (extensionNode == null) extensionNode = readExtensionNode(indexItem);
                }

                if (extensionNode.getLastPosition() < -1)
                  throw new DatabaseException(
                      "Extension node "
                          + extensionNode.getKey()
                          + ":"
                          + extensionNode.getFirstPosition()
                          + " is corrupt");

                logOperation =
                    new LogOperation(
                        this.environment,
                        workerNextLogID.incrementAndGet(),
                        workerNextLogPosition.longValue(),
                        transaction.ID(),
                        txOperation,
                        workerNextExtPosition.getAndAdd(ExtensionItem.BYTES),
                        -1);
                workerNextLogPosition.getAndAdd(logOperation.length() + Integer.BYTES);

                final ExtensionItem newExtensionItem =
                    new ExtensionItem(
                        logOperation.getID(),
                        txOperation.getKey(),
                        logOperation.getExtensionPosition(),
                        logOperation.getLogPosition(),
                        extensionNode.getLastPosition(),
                        -1);
                if (extensionNode.getFirstItem() == null) {
                  extensionNode.updateFirstItem(newExtensionItem);
                } else if (extensionNode.getFirstItem().getNext() == -1) {
                  ExtensionItem currentFirstItem = extensionNode.getFirstItem();
                  ExtensionItem updatedFirstItem =
                      new ExtensionItem(
                          currentFirstItem.getID(),
                          currentFirstItem.getKey(),
                          currentFirstItem.getExtPosition(),
                          currentFirstItem.getLogPosition(),
                          -1,
                          logOperation.getExtensionPosition());
                  extensionNode.updateFirstItem(updatedFirstItem);
                  extensionObjects.put(updatedFirstItem.getExtPosition(), updatedFirstItem);
                }

                if (extensionNode.getLastItem() == null) {
                  extensionNode.updateLastItem(newExtensionItem);
                } else {
                  ExtensionItem currentLastItem = extensionNode.getLastItem();
                  ExtensionItem prevLastItem =
                      new ExtensionItem(
                          currentLastItem.getID(),
                          txOperation.getKey(),
                          currentLastItem.getExtPosition(),
                          currentLastItem.getLogPosition(),
                          currentLastItem.getPrevious(),
                          newExtensionItem.getExtPosition());
                  extensionNode.updateLastItem(newExtensionItem);
                  extensionObjects.put(prevLastItem.getExtPosition(), prevLastItem);
                }

                extensionObjects.put(indexItem.getPosition(), extensionNode);
                extensionObjects.put(extensionNode.getLastPosition(), newExtensionItem);

                recordsDelta++;
              } else {
                logOperation =
                    new LogOperation(
                        this.environment,
                        workerNextLogID.incrementAndGet(),
                        workerNextLogPosition.longValue(),
                        transaction.ID(),
                        txOperation,
                        -1,
                        ancestorIndexItem == IndexItem.VACANT
                            ? -1
                            : ancestorIndexItem.getPosition());
                workerNextLogPosition.getAndAdd(logOperation.length() + Integer.BYTES);
                indexItems.put(
                    txOperation.getKey(),
                    new IndexItem(Type.DATA, txOperation.getKey(), logOperation.getLogPosition()));
                recordsDelta++;
              }
            } else
              throw new UnsupportedOperationException(
                  "Operation " + txOperation.getOperation() + " is not supported");

            logOperations.add(logOperation);
            expectedBytesWrittenToLog += logOperation.length() + Integer.BYTES;
          }

          final LogOperation endLogOperation =
              new LogOperation(
                  this.environment,
                  workerNextLogID.incrementAndGet(),
                  workerNextLogPosition.longValue(),
                  transaction.ID(),
                  Operation.END_TX);
          workerNextLogPosition.getAndAdd(endLogOperation.length() + Integer.BYTES);
          logOperations.add(endLogOperation);
          expectedBytesWrittenToLog += endLogOperation.length() + Integer.BYTES;

          this.logWorker.queueLogOperations(logOperations);

          long logInfoFactor = this.logOperations.get() / 100000;
          this.logOperations.addAndGet(logOperations.size());
          if (this.logOperations.get() / 100000 > logInfoFactor) logInfo();

          this.nextLogID.set(workerNextLogID.longValue());
          this.nextLogPosition.set(workerNextLogPosition.longValue());
          this.nextExtensionPosition.set(workerNextExtPosition.longValue());
          this.checkpointLogPosition = workerNextLogPosition.longValue();

          if (extensionObjects.isEmpty() == false)
            this.logWorker.queueExtensionOperations(extensionObjects.entrySet());

          if (indexItems.isEmpty() == false)
            this.environment.getIndex().writeIndexItems(indexItems.values());
        }

        if (vamosLog.hasLevel(Logging.DEBUG)) {
          LogOperation lastLogOperation = logOperations.get(logOperations.size() - 1);
          vamosLog.debug("Completed commit of transaction " + transaction);
          vamosLog.debug(
              "Log write = "
                  + bytesWrittenToLog
                  + " / "
                  + this.nextLogPosition
                  + " Log ID = "
                  + lastLogOperation.getID());
        }
      } catch (Exception ex) {
        throw ex;
      }

      if (bytesWrittenToLog > 0) tryCheckpoint(bytesWrittenToLog);
    }
  }

  LogOperation readLogOperation(final long position) throws IOException {
    LogOperation logOperation = this.logWorker.getLogOperation(position);
    if (logOperation != null) return logOperation;

    logOperation = this.environment.getCache().getLogOperation(position);
    if (logOperation != null) return logOperation;

    synchronized (this.logFile) {
      if (this.lastLogPosition.get() == -1 || position != this.lastLogPosition.get()) {
        this.logFile.seek(position);
        this.lastLogPosition.set(position);
      } else this.contiguousLogReads.incrementAndGet();

      ByteBuffer buffer = byteBuffer.get();
      buffer.clear();
      int bytesRead = this.logFile.read(buffer.array(), 0, 4096);
      if (bytesRead == -1) throw new IOException("No bytes read at position " + position);

      int logOperationSize = Ints.fromByteArray(buffer.array());
      int remainingBytesToRead = Math.max(0, logOperationSize - (bytesRead - Integer.BYTES));
      this.lastLogPosition.addAndGet(bytesRead);

      buffer.limit(bytesRead);
      buffer.position(Integer.BYTES);
      buffer.compact();

      if (logOperationSize > buffer.capacity()) {
        buffer.flip();
        ByteBuffer dedicatedBuffer = ByteBuffer.allocate(logOperationSize);
        dedicatedBuffer.put(buffer);
        buffer = dedicatedBuffer;
      }

      try {
        if (remainingBytesToRead > 0) {
          bytesRead = this.logFile.read(buffer.array(), buffer.position(), remainingBytesToRead);
          this.lastLogPosition.addAndGet(bytesRead);
        }

        buffer.position(logOperationSize);
        buffer.flip();
        logOperation = new LogOperation(buffer);
      } catch (Exception ex) {
        throw ex;
      }

      this.logReadBytes.addAndGet(logOperationSize);
      this.logReads.incrementAndGet();
    }

    if (logOperation.length() <= this.environment.getConfig().getLogCacheMaxItemSize())
      this.environment.getCache().putLogOperation(logOperation);

    return logOperation;
  }

  byte[] readLogData(final long position) throws IOException {
    LogOperation logOperation = this.logWorker.getLogOperation(position);
    if (logOperation != null) return logOperation.getData();

    logOperation = this.environment.getCache().getLogOperation(position);
    if (logOperation != null) return logOperation.getData();

    final byte[] logData;
    synchronized (this.logFile) {
      if (this.lastLogPosition.get() == -1 || position != this.lastLogPosition.get()) {
        this.logFile.seek(position);
        this.lastLogPosition.set(position);
      } else this.contiguousLogReads.incrementAndGet();

      ByteBuffer buffer = byteBuffer.get();
      buffer.clear();
      int bytesRead = this.logFile.read(buffer.array(), 0, 4096);
      if (bytesRead == -1) throw new IOException("No bytes read at position " + position);

      int logOperationSize = Ints.fromByteArray(buffer.array());
      int remainingBytesToRead = Math.max(0, logOperationSize - (bytesRead - Integer.BYTES));
      this.lastLogPosition.addAndGet(bytesRead);

      buffer.limit(bytesRead);
      buffer.position(Integer.BYTES);
      buffer.compact();

      if (logOperationSize > buffer.capacity()) {
        buffer.flip();
        ByteBuffer dedicatedBuffer = ByteBuffer.allocate(logOperationSize);
        dedicatedBuffer.put(buffer);
        buffer = dedicatedBuffer;
      }

      try {
        if (remainingBytesToRead > 0) {
          bytesRead = this.logFile.read(buffer.array(), buffer.position(), remainingBytesToRead);
          this.lastLogPosition.addAndGet(bytesRead);
        }

        buffer.position(logOperationSize);
        buffer.flip();

        logData = LogOperation.extractData(buffer);
      } catch (Exception ex) {
        throw ex;
      }

      this.logReadBytes.addAndGet(logOperationSize);
      this.logReads.incrementAndGet();
    }

    return logData;
  }

  private int writeLogOperations(final List<LogOperation> logOperations) throws IOException {
    final ByteBuffer buffer = byteBuffer.get();
    buffer.clear();

    int logBytesWritten = 0;
    long writeLogPosition = -1;
    long sequenceLogPosition = -1;
    for (final LogOperation logOperation : logOperations) {
      if (writeLogPosition == -1) {
        writeLogPosition = logOperation.getLogPosition();
        sequenceLogPosition = writeLogPosition + logOperation.length() + Integer.BYTES;
      } else if (sequenceLogPosition != logOperation.getLogPosition()
          || buffer.position() + (logOperation.length() + Integer.BYTES) > buffer.capacity()) {
        writeBufferToLog(buffer, writeLogPosition);

        logBytesWritten += buffer.position();
        writeLogPosition = logOperation.getLogPosition();
        sequenceLogPosition = writeLogPosition + logOperation.length() + Integer.BYTES;
        buffer.clear();
      } else sequenceLogPosition += logOperation.length() + Integer.BYTES;

      if (logOperation.length() + Integer.BYTES > buffer.capacity()) {
        final ByteBuffer tempBuffer = ByteBuffer.allocate(logOperation.length() + Integer.BYTES);
        logOperation.write(tempBuffer);
        writeBufferToLog(tempBuffer, logOperation.getLogPosition());

        logBytesWritten += tempBuffer.position();
        writeLogPosition = -1;
      } else {
        buffer.putInt(logOperation.length());
        logOperation.write(buffer);
      }

      if (logOperation.getOperation().equals(Operation.PUT)
          || logOperation.getOperation().equals(Operation.PUT_NO_OVERWRITE))
        if (logOperation.length() <= this.environment.getConfig().getLogCacheMaxItemSize())
          this.environment.getCache().putLogOperation(logOperation);
    }

    if (buffer.position() > 0) {
      writeBufferToLog(buffer, writeLogPosition);
      logBytesWritten += buffer.position();
    }

    return logBytesWritten;
  }

  private void writeBufferToLog(ByteBuffer buffer, long logPosition) throws IOException {
    writeBytesToLog(buffer.array(), 0, buffer.position(), logPosition);
  }

  private void writeBytesToLog(byte[] bytes, int offset, int length, long logPosition)
      throws IOException {
    synchronized (this.logFile) {
      if (this.lastLogPosition.get() == -1 || logPosition != this.lastLogPosition.get()) {
        this.logFile.seek(logPosition);
        this.lastLogPosition.set(logPosition);
      } else this.contiguousLogWrites.incrementAndGet();

      this.logFile.write(bytes, offset, length);
      this.logWrites.incrementAndGet();
      this.logWriteBytes.addAndGet(length);
      this.lastLogPosition.addAndGet(length);
    }
  }

  ExtensionNode readExtensionNode(final IndexItem indexItem) throws IOException {
    return readExtensionNode(indexItem.getPosition());
  }

  private ExtensionNode readExtensionNode(final long position) throws IOException {
    final ExtensionNode extensionNode = this.logWorker.getExtensionOperation(position);
    if (extensionNode != null) return extensionNode;

    final ByteBuffer buffer = byteBuffer.get();
    buffer.clear();

    synchronized (this.extensionFile) {
      if (this.lastExtensionPosition.get() == -1 || position != this.lastExtensionPosition.get()) {
        this.extensionFile.seek(position);
        this.lastExtensionPosition.set(position);
      } else this.contiguousExtensionReads.incrementAndGet();

      this.extensionFile.readFully(buffer.array(), 0, ExtensionNode.BYTES);
      this.extensionNodeReads.incrementAndGet();
      this.lastExtensionPosition.addAndGet(ExtensionNode.BYTES);
    }

    return new ExtensionNode(buffer);
  }

  ExtensionItem readExtensionItem(final InternalKey internalKey, final long position)
      throws IOException {
    return readExtensionItem(position);
  }

  ExtensionItem readExtensionItem(final long position) throws IOException {
    final ExtensionItem extensionItem = this.logWorker.getExtensionOperation(position);
    if (extensionItem != null) return extensionItem;

    final ByteBuffer buffer = byteBuffer.get();
    buffer.clear();

    synchronized (this.extensionFile) {
      if (this.lastExtensionPosition.get() == -1 || position != this.lastExtensionPosition.get()) {
        this.extensionFile.seek(position);
        this.lastExtensionPosition.set(position);
      } else this.contiguousExtensionReads.incrementAndGet();

      this.extensionFile.readFully(buffer.array(), 0, ExtensionItem.BYTES);
      this.extensionItemReads.incrementAndGet();
      this.lastExtensionPosition.addAndGet(ExtensionItem.BYTES);
    }

    return new ExtensionItem(buffer);
  }

  private void writeExtensionOperations(final Collection<ExtensionObject> extensionObjects)
      throws IOException {
    final List<ExtensionObject> sortedExtensionObjects = Lists.mutable.ofAll(extensionObjects);
    Collections.sort(
        sortedExtensionObjects,
        new Comparator<ExtensionObject>() {
          @Override
          public int compare(ExtensionObject o1, ExtensionObject o2) {
            if (o1.getExtPosition() < o2.getExtPosition()) return -1;
            if (o1.getExtPosition() > o2.getExtPosition()) return 1;
            return 0;
          }
        });

    final ByteBuffer buffer = byteBuffer.get();
    synchronized (this.extensionFile) {
      int batchSize = 0;
      long seekPosition = -1;

      for (int e = 0; e < sortedExtensionObjects.size(); e++) {
        final ExtensionObject extensionObject = sortedExtensionObjects.get(e);

        if (batchSize == 0) {
          buffer.clear();
          seekPosition = extensionObject.getExtPosition();
        } else if (extensionObject.getExtPosition() != seekPosition + buffer.position()
            || buffer.position() + extensionObject.size() > buffer.capacity()) {
          writeBufferToExtension(buffer, seekPosition);
          batchSize = 0;
          buffer.clear();
          seekPosition = extensionObject.getExtPosition();
        }

        if (extensionObject instanceof ExtensionItem) this.extensionItemWrites.incrementAndGet();
        else if (extensionObject instanceof ExtensionNode)
          this.extensionNodeWrites.incrementAndGet();

        extensionObject.write(buffer);
        batchSize++;
      }

      if (batchSize > 0) writeBufferToExtension(buffer, seekPosition);
    }
  }

  private void writeBufferToExtension(final ByteBuffer buffer, final long position)
      throws IOException {
    synchronized (this.extensionFile) {
      if (this.lastExtensionPosition.get() == -1 || position != this.lastExtensionPosition.get()) {
        this.extensionFile.seek(position);
        this.lastExtensionPosition.set(position);
      } else this.contiguousExtensionWrites.incrementAndGet();

      this.extensionFile.write(buffer.array(), 0, buffer.position());
      this.lastExtensionPosition.addAndGet(buffer.position());
    }
  }

  private void logInfo() {
    vamosLog.info("Log:");
    vamosLog.info(String.format("  Operations %d", this.logOperations.get()));
    vamosLog.info(String.format("  Log Reads %d %d", this.logReads.get(), this.logReadBytes.get()));
    vamosLog.info(
        String.format("  Log Writes %d %d", this.logWrites.get(), this.logWriteBytes.get()));
    vamosLog.info(
        String.format(
            "  Ext Item Reads %d %d",
            this.extensionItemReads.get(), this.contiguousExtensionReads.get()));
    vamosLog.info(
        String.format(
            "  Ext Item Writes %d %d",
            this.extensionItemWrites.get(), this.contiguousExtensionWrites.get()));
    vamosLog.info(
        String.format(
            "  Ext Node Reads %d %d",
            this.extensionNodeReads.get(), this.contiguousExtensionReads.get()));
    vamosLog.info(
        String.format(
            "  Ext Node Writes %d %d",
            this.extensionNodeWrites.get(), this.contiguousExtensionWrites.get()));
  }
}
