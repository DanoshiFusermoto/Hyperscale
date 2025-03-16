package org.radix.hyperscale.tools.spam;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.Range;
import org.radix.hyperscale.Plugin;
import org.radix.hyperscale.executors.Executable;
import org.radix.hyperscale.ledger.ShardGroupID;
import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;

public final class Spamathon {
  private static final Logger spammerLog = Logging.getLogger("spammer");

  static {
    spammerLog.setLevels(Logging.ERROR | Logging.FATAL | Logging.INFO | Logging.WARN);
  }

  private static final int MAX_SPAM_EXECUTION_THREADS = 32;
  private static Spamathon instance;

  public static Spamathon getInstance() {
    if (instance == null) instance = new Spamathon();

    return instance;
  }

  private final int numSpamExecutors;
  private final ExecutorService spamExecutor;
  private volatile Executable spammer = null;

  private final Map<String, Class<? extends Spammer>> spammers =
      new HashMap<String, Class<? extends Spammer>>();

  private Spamathon() {
    // Load all the internal spam plugins
    Plugin.loadTypeHierarchy(
        "org.radix.hyperscale",
        Spammer.class,
        clazz -> {
          if (Spammer.class.isAssignableFrom(clazz) == false) {
            spammerLog.error("Class " + clazz + " is not a Spammer plugin");
            return;
          }

          @SuppressWarnings("unchecked")
          Class<? extends Spammer> spammer = (Class<? extends Spammer>) clazz;
          SpamConfig spamConfig = spammer.getAnnotation(SpamConfig.class);
          if (spamConfig == null) {
            spammerLog.error("Loading of spammer failed: No spam config annotation " + spammer);
            return;
          }

          if (Spamathon.this.spammers.putIfAbsent(spamConfig.name(), spammer) != null)
            spammerLog.error("Spammer plugin is already loaded: " + spammer);
        });

    this.numSpamExecutors =
        Math.max(
            1, Math.min(MAX_SPAM_EXECUTION_THREADS, Runtime.getRuntime().availableProcessors()));
    this.spamExecutor =
        Executors.newFixedThreadPool(
            this.numSpamExecutors,
            new ThreadFactory() {
              private final AtomicInteger counter = new AtomicInteger(0);

              @Override
              public Thread newThread(Runnable r) {
                Thread thread = new Thread(r, "spam-thread-" + this.counter.getAndIncrement());
                thread.setPriority(3);
                thread.setDaemon(true);
                return thread;
              }
            });
  }

  int getExecutorCount() {
    return this.numSpamExecutors;
  }

  ExecutorService getExecutorService() {
    if (this.numSpamExecutors <= 1) return null;

    return this.spamExecutor;
  }

  public boolean isSpamming() {
    return this.spammer == null ? false : true;
  }

  public void completed(Executable spammer) {
    if (this.spammer == spammer) this.spammer = null;
  }

  public void cancel() {
    if (this.spammer == null) throw new IllegalStateException("No spammer currently running");

    if (this.spammer.cancel() == false)
      throw new IllegalStateException("Current spam session could not be cancelled");
  }

  @SuppressWarnings("unchecked")
  public <T extends Spammer> T spam(
      final String name,
      final int iterations,
      final int rate,
      final Range<Integer> saturation,
      final ShardGroupID targetShard,
      final double shardFactor)
      throws InstantiationException,
          NoSuchMethodException,
          SecurityException,
          IllegalAccessException,
          IllegalArgumentException,
          InvocationTargetException {
    if (Spamathon.getInstance().isSpamming())
      throw new IllegalStateException("Already an instance of spammer running");

    Class<? extends Spammer> spammerClass = this.spammers.get(name.toLowerCase());
    if (spammerClass == null)
      throw new InstantiationException("Spammer plugin is unknown: " + name);

    final Constructor<? extends Spammer> constructor =
        spammerClass.getDeclaredConstructor(
            Spamathon.class, int.class, int.class, Range.class, ShardGroupID.class, double.class);
    final Spammer spammer =
        constructor.newInstance(this, iterations, rate, saturation, targetShard, shardFactor);
    final Future<?> spamFuture = this.spamExecutor.submit(spammer);
    spammer.setFuture(spamFuture);

    this.spammer = spammer;
    return (T) spammer;
  }
}
