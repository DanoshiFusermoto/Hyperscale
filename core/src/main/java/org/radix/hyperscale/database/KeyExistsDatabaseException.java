package org.radix.hyperscale.database;

import com.sleepycat.je.DatabaseEntry;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@SuppressWarnings("serial")
public class KeyExistsDatabaseException extends DatabaseException {
  private final String database;
  private final DatabaseEntry key;

  public KeyExistsDatabaseException(String database, DatabaseEntry key) {
    super("Key " + toString(key) + " EXISTS in database " + database);

    this.key = key;
    this.database = database;
  }

  public String getDatabase() {
    return this.database;
  }

  public DatabaseEntry getKey() {
    return this.key;
  }

  private static String toString(DatabaseEntry de) {
    byte[] bytes = de.getData();
    return IntStream.range(0, bytes.length)
        .map(i -> bytes[i] & 0xFF)
        .mapToObj(i -> String.format("%02X", i))
        .collect(Collectors.joining(" "));
  }
}
