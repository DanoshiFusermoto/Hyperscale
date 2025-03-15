package org.radix.hyperscale.executors;

import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public abstract class Executable implements Runnable {
  private static final AtomicLong nextID = new AtomicLong(0);

  private final long id = nextID.incrementAndGet();
  private final AtomicReference<Future<?>> future = new AtomicReference<>();
  private final AtomicBoolean terminated = new AtomicBoolean(false);
  private final AtomicBoolean cancelled = new AtomicBoolean(false);
  private final AtomicBoolean finished = new AtomicBoolean(false);

  private CountDownLatch finishLatch;

  public final void terminate(boolean finish) {
    if (this.terminated.compareAndSet(false, true) == true) {
      Future<?> thisFuture = this.future.get();
      if (thisFuture != null && thisFuture.cancel(false) == false && finish) awaitFinish();

      onTerminated();
    } else throw new IllegalStateException("Executable " + this + " is already terminated");
  }

  public final boolean isTerminated() {
    return this.terminated.get();
  }

  public final boolean isCancelled() {
    return this.cancelled.get();
  }

  public final boolean isFinished() {
    return this.finished.get();
  }

  public abstract void execute();

  protected void onCancelled() {
    // Stub function for abstract class
  }

  protected void onTerminated() {
    // Stub function for abstract class
  }

  @Override
  public final void run() {
    try {
      if (this.cancelled.get() == false) {
        if (this.terminated.get() == true)
          throw new IllegalStateException("Executable " + this + " is terminated");

        this.finished.set(false);
        this.finishLatch = new CountDownLatch(1);
        execute();
      }
    }
    // TODO check this isnt needed
    catch (Throwable t) {
      // FIXME weird exception happens here on startup
      Future<?> thisFuture = this.future.get();
      if (thisFuture != null) thisFuture.cancel(false);

      this.terminated.set(true);
    } finally {
      this.finished.set(true);
      this.finishLatch.countDown();
    }
  }

  public final boolean cancel() {
    if (this.cancelled.compareAndSet(false, true) == true) {
      Future<?> thisFuture = this.future.get();
      if (thisFuture != null) {
        if (thisFuture.isDone() == false && thisFuture.cancel(false) == true) {
          onCancelled();
          return true;
        }
      }

      return false;
    } else throw new IllegalStateException("Executable " + this + " is cancelled");
  }

  public final long getID() {
    return this.id;
  }

  public final Future<?> getFuture() {
    return this.future.get();
  }

  public final void setFuture(final Future<?> future) {
    Objects.requireNonNull(future, "Executable future is null");
    this.future.updateAndGet(
        f -> {
          if (f != null)
            throw new IllegalStateException("Future is already set for executable " + this);

          return future;
        });
  }

  @Override
  public boolean equals(final Object other) {
    if (other == null) return false;

    if (other == this) return true;

    if (other instanceof Executable executable) return this.id == executable.id;

    return false;
  }

  @Override
  public int hashCode() {
    return (int) (this.id & 0xFFFFFFFF);
  }

  @Override
  public String toString() {
    return "ID: " + this.id + " Terminated: " + this.terminated;
  }

  private void awaitFinish() {
    try {
      if (this.finishLatch != null) this.finishLatch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
