package org.radix.hyperscale.logging;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Date;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public final class Logger implements AutoCloseable {
  private static ThreadLocal<StringBuilder> builder =
      ThreadLocal.withInitial(() -> new StringBuilder(512));

  private final PrintWriter fileWriter;
  private final String name;
  private final String filename;
  private volatile int levels;
  private volatile boolean stdOut;
  private final BlockingQueue<String> entries = new LinkedBlockingQueue<>();

  Logger(final String name, final String filename, final int levels, final boolean stdOut) {
    this.name = name;
    this.levels = levels;
    this.stdOut = stdOut;

    if (filename != null) {
      final File file = new File(filename);
      if (file.exists() == false) file.getParentFile().mkdirs();

      try {
        this.filename = filename;
        this.fileWriter = new PrintWriter(file, "UTF-8");
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    } else {
      this.filename = null;
      this.fileWriter = null;
    }
  }

  synchronized void flush() {
    if (this.fileWriter != null) {
      if (this.entries.isEmpty() == false) {
        String entry = null;
        while ((entry = this.entries.poll()) != null) this.fileWriter.println(entry);

        this.fileWriter.flush();
      }
    }
  }

  @Override
  public void close() {
    if (this.fileWriter != null) {
      flush();
      this.fileWriter.close();
    }
  }

  public void out(final String message) {
    out(message, null);
  }

  public void out(final Throwable ex) {
    out(null, ex);
  }

  public void out(final String message, Throwable ex) {
    log(Logging.ALL, message, ex, true);
  }

  public void log(final String message) {
    log(Logging.ALL, message, null);
  }

  public void log(final Throwable ex) {
    log(Logging.ALL, null, ex);
  }

  public void log(final String message, Throwable ex) {
    log(Logging.ALL, message, ex);
  }

  public void info(final String message) {
    info(message, null);
  }

  public void info(final Throwable ex) {
    info(null, ex);
  }

  public void info(final String message, Throwable ex) {
    log(Logging.INFO, message, ex);
  }

  public void error(final String message) {
    error(message, null);
  }

  public void error(final Throwable ex) {
    error(null, ex);
  }

  public void error(final String message, Throwable ex) {
    log(Logging.ERROR, message, ex);
  }

  public void debug(final String message) {
    debug(message, null);
  }

  public void debug(final Throwable ex) {
    debug(null, ex);
  }

  public void debug(final String message, Throwable ex) {
    log(Logging.DEBUG, message, ex);
  }

  public void warn(final String message) {
    warn(message, null);
  }

  public void warn(final Throwable ex) {
    warn(null, ex);
  }

  public void warn(final String message, Throwable ex) {
    log(Logging.WARN, message, ex);
  }

  public void trace(final String message) {
    trace(message, null);
  }

  public void trace(final Throwable ex) {
    trace(null, ex);
  }

  public void trace(final String message, Throwable ex) {
    log(Logging.TRACE, message, ex);
  }

  public void fatal(final String message) {
    fatal(message, null);
  }

  public void fatal(final Throwable ex) {
    fatal(null, ex);
  }

  public void fatal(final String message, Throwable ex) {
    log(Logging.FATAL, message, ex);
  }

  private void log(final int level, final String message, final Throwable ex) {
    log(level, message, ex, false);
  }

  private void log(
      final int level, final String message, final Throwable ex, final boolean stdOut) {
    final String levelString;
    if (level != Logging.ALL) {
      switch (this.levels & level) {
        case Logging.FATAL:
          levelString = " FATAL: ";
          break;
        case Logging.ERROR:
          levelString = " ERROR: ";
          break;
        case Logging.WARN:
          levelString = " WARN: ";
          break;
        case Logging.INFO:
          levelString = " INFO: ";
          break;
        case Logging.DEBUG:
          levelString = " DEBUG: ";
          break;
        case Logging.TRACE:
          levelString = " TRACE: ";
          break;
        default:
          return;
      }
    } else levelString = " ";

    final StringBuilder builder = Logger.builder.get();
    builder.setLength(0);

    // Who did this??
    final long time = new Date().getTime();
    final long midnight = time - ((time / (86400 * 1000)) * (86400 * 1000));
    final long hours = midnight / (3600 * 1000);
    final long minutes = time / (1000 * 60) % 60;
    final long seconds = time / (1000) % 60;
    final long milliseconds = time % 1000;

    if (hours <= 9) builder.append('0');
    builder.append(hours);
    builder.append(':');

    if (minutes <= 9) builder.append('0');
    builder.append(minutes);
    builder.append(':');

    if (seconds <= 9) builder.append('0');
    builder.append(seconds);
    builder.append(',');

    if (milliseconds <= 99) builder.append('0');
    if (milliseconds <= 9) builder.append('0');
    builder.append(milliseconds);

    builder.append(levelString);

    if (this.name != null) {
      builder.append('[');
      builder.append(this.name);
      builder.append("] ");
    }

    int stackIndex = 0;
    final StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
    for (; stackIndex < stackTrace.length; stackIndex++) {
      if (stackTrace[stackIndex].getClassName().equals(getClass().getCanonicalName()) == false
          && stackTrace[stackIndex].getClassName().equals(Thread.class.getCanonicalName()) == false)
        break;
    }

    final StackTraceElement stackElement = stackTrace[stackIndex];
    builder.append(
        stackElement.getClassName(),
        stackElement.getClassName().lastIndexOf('.') + 1,
        stackElement.getClassName().length());
    builder.append(':');
    builder.append(stackElement.getLineNumber());
    builder.append(" - ");

    if (message != null) builder.append(message);

    if (ex != null) {
      final StringWriter writer = new StringWriter(256);
      ex.printStackTrace(new PrintWriter(writer));
      builder.append(System.lineSeparator());
      builder.append(writer.toString().trim());
    }

    final String logEntry = builder.toString();

    if (this.fileWriter != null) this.entries.add(logEntry);

    if (stdOut || this.stdOut || level == Logging.FATAL) Logging.getInstance().toStdOut(logEntry);
  }

  public int getLevels() {
    return this.levels;
  }

  public void setLevel(final int level) {
    this.levels |= level;
  }

  public void setLevels(final int levels) {
    this.levels = levels;
  }

  public boolean hasLevel(final int level) {
    return (this.levels & level) == level;
  }

  public String getName() {
    return this.name;
  }

  public String getFilename() {
    return this.filename;
  }

  public boolean isStdOut() {
    return this.stdOut;
  }

  public void setStdOut(final boolean stdOut) {
    this.stdOut = stdOut;
  }
}
