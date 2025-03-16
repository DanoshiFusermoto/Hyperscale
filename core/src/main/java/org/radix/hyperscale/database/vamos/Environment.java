package org.radix.hyperscale.database.vamos;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.impl.factory.primitive.IntObjectMaps;
import org.radix.hyperscale.database.DatabaseException;
import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;

public class Environment {
  private static final Logger vamosLog = Logging.getLogger("vamos");

  private final File directory;
  private final EnvironmentConfig config;

  private final Log log;
  private final Index index;

  private final EnvironmentCache cache;
  private final MutableIntObjectMap<Database> databases;

  private final LockManager lockManager;
  private final ReentrantReadWriteLock lock;

  public Environment(File directory) throws IOException {
    this(directory, EnvironmentConfig.DEFAULT);
  }

  public Environment(File directory, EnvironmentConfig config) throws IOException {
    this.directory = Objects.requireNonNull(directory, "Directory is null");
    this.config = Objects.requireNonNull(config, "Config is null");

    if (this.directory.exists() == false && this.directory.mkdirs() == false)
      throw new IOException("Could not create directory structure " + this.directory);

    this.cache = new EnvironmentCache(this);
    this.lockManager = new LockManager(this);
    this.index = new Index(this);
    this.log = new Log(this);

    this.databases = IntObjectMaps.mutable.<Database>withInitialCapacity(4).asSynchronized();
    this.lock = new ReentrantReadWriteLock();
  }

  public EnvironmentConfig getConfig() {
    return this.config;
  }

  public void close() {
    // TODO Auto-generated method stub

  }

  public Database open(String name, DatabaseConfig config) throws IOException {
    this.lock.writeLock().lock();
    try {
      if (this.databases.containsKey(name.toLowerCase().hashCode())) // TODO Database ID
      throw new DatabaseException("Database " + name + " is already open");

      Database database = new Database(this, name, config);
      this.databases.put(database.getID(), database);
      return database;
    } finally {
      this.lock.writeLock().unlock();
    }
  }

  Database getDatabase(int databaseID) {
    this.lock.readLock().lock();
    try {
      return this.databases.get(databaseID);
    } finally {
      this.lock.readLock().unlock();
    }
  }

  List<Database> getDatabases() {
    this.lock.readLock().lock();
    try {
      return Lists.immutable.ofAll(this.databases.values()).castToList();
    } finally {
      this.lock.readLock().unlock();
    }
  }

  public File getDirectory() {
    return this.directory;
  }

  EnvironmentCache getCache() {
    return this.cache;
  }

  public LockManager getLockManager() {
    return this.lockManager;
  }

  Log getLog() {
    return this.log;
  }

  Index getIndex() {
    return this.index;
  }

  IndexNodeID toIndexNodeID(InternalKey internalKey) {
    return IndexNodeID.from((int) Math.abs(internalKey.value() % this.config.getIndexNodeCount()));
  }
}
