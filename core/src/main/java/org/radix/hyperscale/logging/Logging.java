package org.radix.hyperscale.logging;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import com.google.common.collect.Lists;

public class Logging
{
	public static final int OFF		= 0;
	public static final int TRACE 	= 1;
	public static final int INFO	= 2;
	public static final int WARN	= 4;
	public static final int ERROR	= 8;
	public static final int FATAL	= 16;
	public static final int DEBUG	= 32;
	public static final int ALL 	= 255;

	private static Logging logging;

	static
	{
		logging = new Logging();
	}

	public static Logging getInstance()
	{
		return logging;
	}

	public static Logger getLogger()
	{
		return getInstance().get("general");
	}

	public static Logger getLogger(String name)
	{
		return getInstance().get(name);
	}

	private Properties properties;
	private final Map<String, Logger> loggers = new ConcurrentHashMap<>();

	private final Thread loggingThread;
	private final Object flushingLock = new Object();

	private boolean	stdOut = true;
	private final LinkedBlockingQueue<String> stdOutEntries = new LinkedBlockingQueue<>();

	private Logging()
	{
		boolean propertiesLoaded = false;

		try (InputStream loggerPropertiesInput = new FileInputStream(new File("logger.config"))) {
			this.properties = new Properties();
			this.properties.load(loggerPropertiesInput);
			propertiesLoaded = true;
		} catch (IOException ex) {
			try (InputStream loggerPropertiesInput = getClass().getResourceAsStream("/logger.config")) {
				if (loggerPropertiesInput != null) {
					this.properties = new Properties();
					this.properties.load(loggerPropertiesInput);
					propertiesLoaded = true;
				}
			} catch (IOException ex2) {
				// Ignore error loading from resource and fall through to default properties case below
			}
		}

		if (!propertiesLoaded)
		{
			this.properties = new Properties();
			this.properties.put("logger.general.level", String.valueOf((Logging.INFO|Logging.ERROR|Logging.FATAL|Logging.WARN)));
			this.properties.put("logger.general.stdout", "1");
		}

		Runnable loggingProcessor = () -> {
			while (true) {
				long start = System.nanoTime();

				Logging.this.flush();

				if (TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) < 250) {
					try {
						Thread.sleep(Math.max(1, 250 - TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start)));
					} catch (InterruptedException e) {
						// Exit if we are interrupted
						Thread.currentThread().interrupt();
						break;
					}
				}
			}
		};

		this.loggingThread = new Thread(loggingProcessor);
		this.loggingThread.setDaemon(true);
		this.loggingThread.setName("Logging Processor");
		this.loggingThread.setPriority(3);
		this.loggingThread.start();

		Runtime.getRuntime().addShutdownHook(new Thread(this::terminate));
	}

	private Logger get(String name)
	{
		final int loggingLevel;
		
		// System overrides
		if (Boolean.getBoolean("info"))
			loggingLevel = Logging.ERROR | Logging.FATAL | Logging.WARN | Logging.INFO;  
		else if (Boolean.getBoolean("debug"))
			loggingLevel = Logging.ERROR | Logging.FATAL | Logging.WARN | Logging.DEBUG | Logging.INFO;
		else
		    loggingLevel = Integer.parseInt(properties.getProperty("logger."+name+".level", String.valueOf((Logging.FATAL | Logging.ERROR | Logging.WARN))));
		
		final String filename = "stdout".equals(properties.getProperty("logger."+name+".file")) ? null : properties.getProperty("logger."+name+".file", System.getProperty("user.dir")+File.separatorChar+"logs"+File.separatorChar+name+".log");
		final boolean console = Boolean.getBoolean("console") == true ? false : (Integer.parseInt(properties.getProperty("logger."+name+".stdout", "1"))==0?false:stdOut);
		return this.loggers.computeIfAbsent(name, nm -> new Logger(nm, filename, loggingLevel, console));
	}

	void toStdOut(String log)
	{
		this.stdOutEntries.add(log);
	}

	private void terminate() {
		this.loggingThread.interrupt();
		try {
			this.loggingThread.join();
		} catch (InterruptedException e) {
			// Re-interrupt.  Not going to handle this here.
			Thread.currentThread().interrupt();
		}
		flush();
	}

	public void flush() {
		synchronized (this.flushingLock) 
		{
			this.loggers.values().forEach(Logger::flush);
			if (this.stdOutEntries.isEmpty() == false) 
			{
				ArrayList<String> entries = Lists.newArrayList();
				this.stdOutEntries.drainTo(entries);
				// As this is a logging system, it needs to write to System.out
				entries.forEach(System.out::println); //NOSONAR
			}
		}
	}

	public boolean isStdOut()
	{
		return this.stdOut;
	}

	public void setStdOut(boolean stdOut)
	{
		this.stdOut = stdOut;

		for (Logger logger : this.loggers.values())
			logger.setStdOut(stdOut);
	}
}
