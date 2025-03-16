package org.radix.hyperscale.database;

import com.sleepycat.je.DatabaseEntry;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class UniqueKeyConstraintDatabaseException extends DatabaseException {
  /** */
  private static final long serialVersionUID = -7880501810564801476L;

  private final String database;
  private final DatabaseEntry key;

  public UniqueKeyConstraintDatabaseException(final String database, final DatabaseEntry key) {
    super(
        "Unique key "
            + toString(Objects.requireNonNull(key, "Database entry key is null"))
            + " violates constraints in database "
            + Objects.requireNonNull(database, "Database is null"));

    this.key = key;
    this.database = database;
  }

  public String getDatabase() {
    return this.database;
  }

  public DatabaseEntry getKey() {
    return this.key;
  }

  private static String toString(final DatabaseEntry key) {
    byte[] bytes = key.getData();
    return IntStream.range(0, bytes.length)
        .map(i -> bytes[i] & 0xFF)
        .mapToObj(i -> String.format("%02X", i))
        .collect(Collectors.joining(" "));
  }
}
