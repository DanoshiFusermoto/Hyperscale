package org.radix.hyperscale;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.TimeUnit;
import javax.imageio.ImageIO;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.radix.hyperscale.console.Console;
import org.radix.hyperscale.crypto.Hash;
import org.radix.hyperscale.crypto.ed25519.EDKeyPair;
import org.radix.hyperscale.crypto.ed25519.EDPublicKey;
import org.radix.hyperscale.crypto.ed25519.EDSignature;
import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;
import org.radix.hyperscale.network.AbstractConnection;
import org.radix.hyperscale.network.MessageProcessor;
import org.radix.hyperscale.network.StandardConnectionFilter;
import org.radix.hyperscale.network.messages.KillMessage;
import org.radix.hyperscale.serialization.SerializerId2;
import org.radix.hyperscale.time.Time;
import org.radix.hyperscale.time.TimeProvider;
import org.radix.hyperscale.utils.Bytes;

public class Hyperscale {
  private static final Logger log = Logging.getLogger();

  public static void main(String[] args) {
    // Check available cores is sufficient
    if (Runtime.getRuntime().availableProcessors() < 2) {
      System.out.println(
          "Processor cores of "
              + Runtime.getRuntime().availableProcessors()
              + " is too low for efficient operation!");
      System.out.println("Please execute with at least 2 processor cores");
      System.exit(911);
      return;
    }

    // Check available memory is sufficient
    long XMX = Runtime.getRuntime().maxMemory();
    long minXMX = (long) (1l << 31l);
    if (XMX < minXMX) // 4GB
    {
      System.out.println(
          "Allocated heap of "
              + Runtime.getRuntime().maxMemory()
              + " is too low for efficient operation!");
      System.out.println(
          "Please increase allocated heap to AT LEAST 2GB by including the runtime option -Xmx.");
      System.out.println("Example:  -Xmx2G = 2GB heap");
      System.out.println("          -Xmx4G = 4GB heap");
      System.out.println("          -Xmx8M = 8GB heap");
      System.exit(911);
      return;
    }

    // Check Java version is supported
    if (Runtime.version().feature() < Constants.JAVA_VERSION_MIN
        || Runtime.version().feature() > Constants.JAVA_VERSION_MAX) {
      System.out.println("Java runtime version " + Runtime.version() + " not supported!");
      System.out.println(
          "Supported Java runtimes are versions "
              + Constants.JAVA_VERSION_MIN
              + " to "
              + Constants.JAVA_VERSION_MAX);
      System.exit(911);
    }

    try {
      // Check multiple instances
      if (1 == 0 && JarDetector.isInstanceRunning()) {
        System.out.println("Naughty!! Trying to run multiple instances without the Godix key!");
        System.exit(911);
        return;
      }

      Configuration.createAsDefault("/commandline_options.json", args);

      if (Configuration.getDefault().getCommandLine("singleton", false) == true
          && Configuration.getDefault().getCommandLine("contexts", 1) > 1)
        System.out.println("WARN: Instantiating multiple singleton contexts!");

      // Set some java library options
      ImageIO.setUseCache(false);

      try {
        Field isRestricted =
            Class.forName("javax.crypto.JceSecurity").getDeclaredField("isRestricted");

        log.info("Encryption restrictions are set, need to override...");

        if (Modifier.isFinal(isRestricted.getModifiers())) {
          Field modifiers = Field.class.getDeclaredField("modifiers");
          modifiers.setAccessible(true);
          modifiers.setInt(isRestricted, isRestricted.getModifiers() & ~Modifier.FINAL);
        }

        isRestricted.setAccessible(true);
        isRestricted.setBoolean(null, false);
        isRestricted.setAccessible(false);
        log.info("...override success!");
      } catch (NoSuchFieldException nsfex) {
        log.error("No such field - isRestricted");
      }

      if (Configuration.getDefault().get("ledger.liveness.recovery", Boolean.TRUE) == false)
        System.err.println("WARNING: Liveness recovery is disabled for DEBUG");

      // Sys Props //
      System.setProperty(
          "user.dir",
          Configuration.getDefault().getCommandLine("home", System.getProperty("user.dir")));
      System.setProperty(
          "console", Boolean.toString(Configuration.getDefault().getCommandLine("console", false)));
      System.setProperty(
          "godix", Boolean.toString(Configuration.getDefault().getCommandLine("godix", false)));
      System.setProperty(
          "singleton",
          Boolean.toString(Configuration.getDefault().getCommandLine("singleton", false)));

      // Crypto toggles //
      if (Configuration.getDefault().getCommandLine("skipec", false)) {
        System.setProperty("EC.skip_signing", Boolean.toString(true));
        System.setProperty("EC.skip_verification", Boolean.toString(true));
        System.err.println("WARNING: EC functions are disabled");
      }

      if (Configuration.getDefault().getCommandLine("skipbls", false)) {
        System.setProperty("BLS.skip_signing", Boolean.toString(true));
        System.setProperty("BLS.skip_verification", Boolean.toString(true));
        System.err.println("WARNING: BLS functions are disabled");
      }

      if (Configuration.getDefault().getCommandLine("skipmerkle", false)) {
        System.setProperty("merkle.audit.disabled", Boolean.toString(true));
        System.err.println("WARNING: Merkle audits are disabled");
      }

      // Logging //
      System.setProperty(
          "info", Boolean.toString(Configuration.getDefault().getCommandLine("info", false)));
      System.setProperty(
          "debug", Boolean.toString(Configuration.getDefault().getCommandLine("debug", false)));

      // Universe //
      Universe.createAsDefault(Bytes.fromBase64String(Configuration.getDefault().get("universe")));

      // Time //
      Constructor<?> timeConstructor =
          Class.forName(
                  Configuration.getDefault()
                      .get("time.provider", "org.radix.hyperscale.time.WallClockTime"))
              .getConstructor(Configuration.class);
      TimeProvider timeProvider =
          (TimeProvider) timeConstructor.newInstance(Configuration.getDefault());
      Time.createAsDefault(timeProvider);

      // Check the universe key exists to run Godix functions
      if (Boolean.getBoolean("godix") == true) {
        final File universeKeyPath =
            new File(
                System.getProperty("universe.key.path", System.getProperty("user.dir"))
                    + File.separatorChar
                    + "universe.key");
        if (universeKeyPath.exists() == false) {
          log.fatal("Unable to start with Godix functions as universe.key is not found!");
          System.exit(911);
          return;
        }

        final EDKeyPair godixKeyPair = EDKeyPair.fromFile(universeKeyPath, false);
        final Hash godixKeyValidationHash = Hash.random();
        final EDSignature godixKeyValidationSignature =
            godixKeyPair.getPrivateKey().sign(godixKeyValidationHash);
        godixKeyValidationSignature.reset();
        if (Universe.getDefault()
                .getCreator()
                .<EDPublicKey>getKey()
                .verify(godixKeyValidationHash, godixKeyValidationSignature)
            == false) {
          System.out.println(
              "Unable to start with Godix functions verification of universe.key against Universe definition failed!");
          System.exit(911);
          return;
        }
      }

      new Hyperscale();

      // Ephemeral only applies to contexts at startup!
      System.setProperty("singleton", Boolean.FALSE.toString());

      // Start console
      if (Boolean.getBoolean("console") == true) {
        // Check the universe key exists to run Godix functions
        if (Boolean.getBoolean("godix") == true) {
          new Console(
              System.in,
              System.out,
              new org.radix.hyperscale.console.Ledger(),
              new org.radix.hyperscale.console.Network(),
              new org.radix.hyperscale.console.Atoms(),
              new org.radix.hyperscale.console.Serializer(),
              new org.radix.hyperscale.console.System(),
              new org.radix.hyperscale.console.Validators(),
              new org.radix.hyperscale.console.Wallet(),
              new org.radix.hyperscale.console.Tokens(),
              new org.radix.hyperscale.console.StateMachine(),
              new org.radix.hyperscale.console.Database(),

              // Godix special functions
              new org.radix.hyperscale.console.Godix(),
              new org.radix.hyperscale.console.Contexts());
        } else {
          new Console(
              System.in,
              System.out,
              new org.radix.hyperscale.console.Ledger(),
              new org.radix.hyperscale.console.Network(),
              new org.radix.hyperscale.console.Atoms(),
              new org.radix.hyperscale.console.Serializer(),
              new org.radix.hyperscale.console.System(),
              new org.radix.hyperscale.console.Validators(),
              new org.radix.hyperscale.console.Wallet(),
              new org.radix.hyperscale.console.Tokens(),
              new org.radix.hyperscale.console.StateMachine(),
              new org.radix.hyperscale.console.Database());
        }
      }
    } catch (Throwable t) {
      log.fatal("Unable to start", t);
      java.lang.System.exit(-1);
    }
  }

  private static class JarDetector {
    public static boolean isInstanceRunning() throws Exception {
      // Get the location of the current JAR
      final String jarPath =
          JarDetector.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath();

      // Get the JAR file's hash for unique identification
      final Hash thisHash = getInstanceHash(jarPath);

      // Get current PID
      final String currentPid = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];

      // Get all Java processes using standard JMX
      final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      final Set<ObjectName> runtimeBeans =
          mbs.queryNames(new ObjectName("java.lang:type=Runtime,*"), null);

      for (ObjectName runtime : runtimeBeans) {
        final String pid = (String) mbs.getAttribute(runtime, "Name");
        if (pid.startsWith(currentPid) == false) {
          try {
            String classPath = (String) mbs.getAttribute(runtime, "ClassPath");

            System.out.println(pid + ":" + classPath);

            if (classPath != null) {
              Hash otherHash = getInstanceHash(classPath);
              if (thisHash.equals(otherHash)) return true;
            }
          } catch (Exception e) {
            continue;
          }
        }
      }

      return false;
    }

    private static Hash getInstanceHash(String jarPath) throws Exception {
      final File jarFile = new File(jarPath);
      if (jarFile.isDirectory()) return Hash.random();

      try (final FileInputStream fis = new FileInputStream(jarPath)) {
        final MessageDigest digest = MessageDigest.getInstance("SHA-256");
        final byte[] buffer = new byte[8192];
        int count;
        while ((count = fis.read(buffer)) > 0) digest.update(buffer, 0, count);

        return Hash.from(digest.digest());
      }
    }
  }

  private Hyperscale() throws Exception {
    // CONTEXTS //
    List<Context> contexts = new ArrayList<Context>();
    if (Configuration.getDefault().has("contexts.definitions")) {
      StringTokenizer tokenizer =
          new StringTokenizer(Configuration.getDefault().get("contexts.definitions"), ",");
      while (tokenizer.hasMoreTokens()) {
        String name = tokenizer.nextToken();
        contexts.add(Context.createAndStart(name.trim().toLowerCase(), Configuration.getDefault()));
      }
    } else if (Configuration.getDefault().getCommandLine("contexts", 1) == 1)
      contexts.add(Context.createAndStart());
    else
      contexts.addAll(
          Context.createAndStart(
              Configuration.getDefault().getCommandLine("contexts", 1),
              "node",
              Configuration.getDefault()));

    // KILL LISTENER //
    for (final Context context : contexts) {
      context
          .getNetwork()
          .getMessaging()
          .register(
              KillMessage.class,
              this.getClass(),
              new MessageProcessor<KillMessage>() {
                @Override
                public void process(
                    final KillMessage killMessage, final AbstractConnection connection) {
                  try {
                    if (killMessage.verify(Universe.getDefault().getCreator().getKey()) == false) {
                      log.error(
                          context.getName()
                              + ": "
                              + killMessage.getClass().getAnnotation(SerializerId2.class)
                              + " has invalid signature from "
                              + connection);
                      return;
                    }

                    final Collection<AbstractConnection> allConnections =
                        context.getNetwork().get(StandardConnectionFilter.build(context));
                    for (final AbstractConnection relayConnection : allConnections) {
                      if (relayConnection.equals(connection)) continue;

                      if (killMessage.includeBoostraps() == false
                          && Universe.getDefault()
                              .getValidators()
                              .contains(context.getNode().getIdentity())) continue;

                      try {
                        context.getNetwork().getMessaging().send(killMessage, relayConnection);
                      } catch (IOException ioex) {
                        log.error(
                            context.getName()
                                + ": Could not relay "
                                + killMessage.getClass().getAnnotation(SerializerId2.class)
                                + " to "
                                + relayConnection,
                            ioex);
                      }
                    }

                    System.exit(-1);
                  } catch (Exception ex) {
                    log.error(
                        context.getName()
                            + ": "
                            + killMessage.getClass().getAnnotation(SerializerId2.class)
                            + " "
                            + connection,
                        ex);
                  }
                }
              });
    }

    // API //
    WebService.create().start();

    // MONITORING
    if (Configuration.getDefault().get("system.monitor.deadlocks", Boolean.TRUE)) {
      // Basic thread lock detector
      final long deadlockDetectionInterval =
          TimeUnit.SECONDS.toMillis(
              Long.parseLong(
                  Configuration.getDefault().get("system.monitor.deadlocks.interval", "5")));
      Thread detectorThread =
          new Thread(
              () -> {
                ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();

                while (true) {
                  long[] deadlockedThreadIds = threadBean.findDeadlockedThreads();
                  if (deadlockedThreadIds != null && deadlockedThreadIds.length > 0) {
                    ThreadInfo[] threadInfos =
                        threadBean.getThreadInfo(deadlockedThreadIds, true, true);
                    log.fatal("DEADLOCK DETECTED!");
                    for (ThreadInfo threadInfo : threadInfos) log.fatal(threadInfo.toString());

                    // Alert user
                    System.err.println("DEADLOCK DETECTED");
                  }

                  try {
                    Thread.sleep(deadlockDetectionInterval);
                  } catch (InterruptedException e) {
                    break;
                  }
                }

                log.warn("Deadlock detector has exited");
              },
              "Deadlock Detector");

      detectorThread.setDaemon(true);
      detectorThread.start();
      log.log("Deadlock detector started with interval: " + deadlockDetectionInterval + "ms");
    }
  }

  public static final void dumpThreadInfo() {
    ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
    ThreadInfo[] threadInfos = threadBean.dumpAllThreads(true, true);
    log.log("THREAD DUMP");
    for (ThreadInfo threadInfo : threadInfos) log.fatal(threadInfo.toString());

    // Alert user
    System.err.println("Thread dump triggered");
  }
}
