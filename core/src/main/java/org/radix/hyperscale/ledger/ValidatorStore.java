package org.radix.hyperscale.ledger;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseNotFoundException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;
import java.io.IOException;
import java.util.Objects;
import org.radix.hyperscale.Context;
import org.radix.hyperscale.database.DatabaseException;
import org.radix.hyperscale.database.DatabaseStore;
import org.radix.hyperscale.exceptions.ServiceException;
import org.radix.hyperscale.exceptions.StartupException;
import org.radix.hyperscale.exceptions.TerminationException;
import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;
import org.radix.hyperscale.serialization.DsonOutput.Output;
import org.radix.hyperscale.serialization.Serialization;
import org.radix.hyperscale.utils.Longs;

class ValidatorStore extends DatabaseStore {
  private static final ThreadLocal<DatabaseEntry> keyBuffer =
      ThreadLocal.withInitial(DatabaseEntry::new);
  private static final ThreadLocal<DatabaseEntry> valueBuffer =
      ThreadLocal.withInitial(DatabaseEntry::new);

  private static final Logger powerLog = Logging.getLogger("power");

  private Context context;
  private Database votePowerDatabase;

  public ValidatorStore(Context context) {
    super(Objects.requireNonNull(context).getDatabaseEnvironment());

    this.context = context;
  }

  @Override
  public void start() throws StartupException {
    try {
      if (Boolean.TRUE.equals(this.context.getConfiguration().getCommandLine("clean", false)))
        clean();

      DatabaseConfig config = new DatabaseConfig();
      config.setAllowCreate(true);
      config.setTransactional(true);
      this.votePowerDatabase = getEnvironment().openDatabase(null, "vote_powers", config);
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
    Transaction transaction = null;

    try {
      transaction =
          getEnvironment().beginTransaction(null, new TransactionConfig().setReadUncommitted(true));
      getEnvironment().truncateDatabase(transaction, "vote_powers", false);
      transaction.commit();
    } catch (DatabaseNotFoundException dsnfex) {
      if (transaction != null) transaction.abort();

      log.warn(dsnfex.getMessage());
    } catch (Exception ex) {
      if (transaction != null) transaction.abort();

      throw new ServiceException(ex, getClass());
    }
  }

  @Override
  public void close() throws IOException {
    super.close();

    if (this.votePowerDatabase != null) this.votePowerDatabase.close();
  }

  @Override
  public void flush() throws DatabaseException {
    /* Not used */
  }

  public VotePowers get(final Epoch epoch) throws IOException {
    final DatabaseEntry key = keyBuffer.get();
    final DatabaseEntry value = valueBuffer.get();

    final Transaction transaction =
        this.votePowerDatabase.getEnvironment().beginTransaction(null, null);
    try {
      final VotePowers votePowers;
      key.setData(Longs.toByteArray(epoch.getClock()));
      final OperationStatus status =
          this.votePowerDatabase.get(transaction, key, value, LockMode.DEFAULT);
      if (status.equals(OperationStatus.SUCCESS))
        votePowers = Serialization.getInstance().fromDson(value.getData(), VotePowers.class);
      else votePowers = null;

      transaction.commit();
      return votePowers;
    } catch (Exception e) {
      transaction.abort();
      throw e;
    }
  }

  void store(final VotePowers votePowers) throws DatabaseException {
    Objects.requireNonNull(votePowers, "Vote powers is null");

    final DatabaseEntry key = keyBuffer.get();
    final DatabaseEntry value = valueBuffer.get();
    final Transaction transaction =
        this.votePowerDatabase.getEnvironment().beginTransaction(null, null);
    try {
      OperationStatus status;

      key.setData(Longs.toByteArray(votePowers.getEpoch()));
      value.setData(Serialization.getInstance().toDson(votePowers, Output.WIRE));

      status = this.votePowerDatabase.put(transaction, key, value);
      if (status.equals(OperationStatus.SUCCESS) == false)
        throw new DatabaseException("Failed to set vote powers for epoch " + votePowers.getEpoch());

      transaction.commit();
    } catch (Exception e) {
      transaction.abort();
      throw new DatabaseException(e);
    }
  }
}
