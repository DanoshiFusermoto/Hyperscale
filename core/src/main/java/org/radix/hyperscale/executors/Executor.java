package org.radix.hyperscale.executors;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Executor
{
	private static final int MAX_IMMEDIATE_EXECUTION_THREADS = 8;
	private static final int MAX_SCHEDULED_EXECUTION_THREADS = 4;

	private static Executor	instance = null;

	public static Executor getInstance()
	{
		if (instance == null)
			instance = new Executor();

		return instance;
	}

	private final 	ExecutorService immediateExecutor;
	private final   ThreadFactory 	immediateThreadFactory = new ThreadFactory() 
	{
		private final AtomicInteger counter = new AtomicInteger(0);
			
		@Override
		public Thread newThread(Runnable r) 
		{
			Thread thread = new Thread(r, "exec-immediate-"+this.counter.getAndIncrement());
			thread.setDaemon(true);
			return thread;
		}
	};
	
	private final 	ScheduledExecutorService 	scheduledExecutor;
	private final   ThreadFactory 	scheduledThreadFactory = new ThreadFactory() 
	{
		private final AtomicInteger counter = new AtomicInteger(0);
			
		@Override
		public Thread newThread(Runnable r) 
		{
			Thread thread = new Thread(r, "exec-scheduled-"+this.counter.getAndIncrement());
			thread.setDaemon(true);
			return thread;
		}
	};

	private Executor()
	{
		// Shared between contexts so can use all available processors up to max
		int maxImmediateExecutors = Math.min(MAX_IMMEDIATE_EXECUTION_THREADS, Runtime.getRuntime().availableProcessors());
		int maxScheduledExecutors = Math.min(MAX_SCHEDULED_EXECUTION_THREADS, Runtime.getRuntime().availableProcessors()/2);
		this.immediateExecutor = Executors.newFixedThreadPool(maxImmediateExecutors, this.immediateThreadFactory);
		this.scheduledExecutor = Executors.newScheduledThreadPool(maxScheduledExecutors, this.scheduledThreadFactory);
	}

	public Executor(final int numImmediateThreads, final int numScheduledThreads)
	{
		this.immediateExecutor = Executors.newFixedThreadPool(numImmediateThreads);
		this.scheduledExecutor = Executors.newScheduledThreadPool(numScheduledThreads);
	}

	public Executor(final int numImmediateThreads, final ThreadFactory immediateThreadFactory, final int numScheduledThreads, final ThreadFactory scheduledThreadFactory)
	{
		this.immediateExecutor = Executors.newFixedThreadPool(numImmediateThreads, immediateThreadFactory);
		this.scheduledExecutor = Executors.newScheduledThreadPool(numScheduledThreads, scheduledThreadFactory);
	}

	public Future<?> schedule(final ScheduledExecutable executable)
	{
		Objects.requireNonNull(executable, "Executable to schedule is null");
		executable.setFuture(this.scheduledExecutor.schedule(executable, executable.getInitialDelay(), executable.getTimeUnit()));
		return executable.getFuture();
	}

	public Future<?> scheduleWithFixedDelay(final ScheduledExecutable executable)
	{
		Objects.requireNonNull(executable, "Executable to schedule is null");
		executable.setFuture(this.scheduledExecutor.scheduleWithFixedDelay(executable, executable.getInitialDelay(), executable.getRecurrentDelay(), executable.getTimeUnit()));
		return executable.getFuture();
	}

	public Future<?> scheduleAtFixedRate(final ScheduledExecutable executable)
	{
		Objects.requireNonNull(executable, "Executable to schedule is null");
		executable.setFuture(this.scheduledExecutor.scheduleAtFixedRate(executable, executable.getInitialDelay(), executable.getRecurrentDelay(), executable.getTimeUnit()));
		return executable.getFuture();
	}

	public Future<?> schedule(final Executable executable, final int initialDelay, final TimeUnit unit)
	{
		Objects.requireNonNull(executable, "Executable to schedule is null");
		Objects.requireNonNull(unit, "Executable time unit is null");
		executable.setFuture(this.scheduledExecutor.schedule(executable, initialDelay, unit));
		return executable.getFuture();
	}

	public Future<?> schedule(final Runnable runnable, final int initialDelay, final TimeUnit unit)
	{
		Objects.requireNonNull(runnable, "Runnable to schedule is null");
		Objects.requireNonNull(unit, "Executable time unit is null");

		return this.scheduledExecutor.schedule(runnable, initialDelay, unit);
	}

	public Future<?> submit(final Runnable runnable)
	{
		Objects.requireNonNull(runnable, "Runnable to submit is null");
		return this.immediateExecutor.submit(runnable);
	}

	public Future<?> submit(final Callable<?> callable)
	{
		Objects.requireNonNull(callable, "Callable to submit is null");
		return this.immediateExecutor.submit(callable);
	}

	public Future<?> submit(final Executable executable)
	{
		Objects.requireNonNull(executable, "Executable to submit is null");
		Future<?> future = this.immediateExecutor.submit(executable);
		executable.setFuture(future);
		return future;
	}
}
