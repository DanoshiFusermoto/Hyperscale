package org.radix.hyperscale.events;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.eventbus.Subscribe;
import com.google.common.primitives.Primitives;
import com.google.common.reflect.TypeToken;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.radix.hyperscale.Constants;
import org.radix.hyperscale.Context;
import org.radix.hyperscale.Hyperscale;
import org.radix.hyperscale.Service;
import org.radix.hyperscale.exceptions.StartupException;
import org.radix.hyperscale.exceptions.TerminationException;
import org.radix.hyperscale.executors.PollingProcessor;
import org.radix.hyperscale.logging.Logger;
import org.radix.hyperscale.logging.Logging;
import org.radix.hyperscale.utils.MathUtils;

public final class Events implements Service {
  private static final Logger eventLog = Logging.getLogger("events");

  private class MethodInfo {
    private final Method method;
    private final Class<?> parameterType;

    private MethodInfo(final Method method, final Class<?> parameterType) {
      this.method = method;
      this.parameterType = parameterType;
    }

    protected Method getMethod() {
      return this.method;
    }

    protected Class<?> getParameterType() {
      return this.parameterType;
    }
  }

  private class Subscriber {
    private final EventListener listener;
    private final Map<Class<?>, MethodInfo> methods;

    private Subscriber(final EventListener listener, final List<MethodInfo> methods) {
      this.listener = listener;
      this.methods = new HashMap<Class<?>, MethodInfo>(methods.size(), 1.0f);
      for (MethodInfo method : methods) this.methods.put(method.getParameterType(), method);
    }

    protected EventListener getListener() {
      return this.listener;
    }

    protected MethodInfo getMethod(final Class<?> parameterType) {
      return this.methods.get(parameterType);
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + this.listener.hashCode();
      result = prime * result + this.methods.hashCode();
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;

      if (obj == null) return false;

      if (obj instanceof Subscriber other) {
        if (this.listener.equals(other.listener) == false) return false;

        if (this.methods.equals(other.methods) == false) return false;

        return true;
      }

      return false;
    }
  }

  private class Subscribers {
    private final Class<?> eventType;
    private final List<Subscriber> sync;
    private final List<Subscriber> async;
    private final ReentrantReadWriteLock lock;

    private volatile boolean hasSync = false;
    private volatile boolean hasAsync = false;

    private Subscribers(final Class<?> eventType) {
      this.eventType = eventType;
      this.sync = new ArrayList<Subscriber>();
      this.async = new ArrayList<Subscriber>();
      this.lock = new ReentrantReadWriteLock();
    }

    public void register(final Subscriber subscriber) {
      if (isRegistered(subscriber.listener))
        throw new IllegalStateException(
            "Subscriber is already registered: " + subscriber.listener.getClass().getName());

      if (subscriber.listener instanceof SynchronousEventListener) {
        this.sync.add(subscriber);
        this.hasSync = true;
      } else {
        this.async.add(subscriber);
        this.hasAsync = true;
      }
    }

    public boolean isRegistered(final EventListener listener) {
      for (int i = 0; i < this.sync.size(); i++) {
        final Subscriber subscriber = this.sync.get(i);
        if (subscriber.getListener().equals(listener)) return true;
      }

      for (int i = 0; i < this.async.size(); i++) {
        final Subscriber subscriber = this.async.get(i);
        if (subscriber.getListener().equals(listener)) return true;
      }

      return false;
    }

    public boolean unregister(final Subscriber subscriber) {
      boolean removed = false;
      if (subscriber.listener instanceof SynchronousEventListener) {
        removed = this.sync.remove(subscriber);
        this.hasSync = this.sync.isEmpty() == false ? true : false;
      } else {
        removed = this.async.remove(subscriber);
        this.hasAsync = this.async.isEmpty() == false ? true : false;
      }
      return removed;
    }
  }

  private final Context context;
  private final Map<Class<?>, Subscribers> subscribers;
  private final Map<EventListener, Subscriber> listenerToSubscriber;
  private volatile boolean isRunning = false;

  private final BlockingQueue<Event> asyncEventQueue = new ArrayBlockingQueue<Event>(1 << 14);
  private final PollingProcessor asyncEventProcessor =
      new PollingProcessor() {
        private long lastQueueSizeWarn = 0;

        @Override
        public void process() throws InterruptedException {
          if (System.currentTimeMillis() - this.lastQueueSizeWarn > 1000) {
            Events.this
                .context
                .getMetaData()
                .put(
                    "system.events.queue",
                    MathUtils.EWMA(
                        Events.this.context.getMetaData().get("system.events.queue", 0l),
                        Events.this.asyncEventQueue.size(),
                        0.9));

            if (Events.this.asyncEventQueue.size() > Constants.WARN_ON_QUEUE_SIZE)
              eventLog.warn(
                  Events.this.context.getName()
                      + ": System async event queue is "
                      + Events.this.asyncEventQueue.size());

            this.lastQueueSizeWarn = System.currentTimeMillis();
          }

          final Event event = Events.this.asyncEventQueue.poll(1, TimeUnit.SECONDS);
          if (event == null) return;

          final Subscribers subscribers = Events.this.subscribers.get(event.getClass());
          if (subscribers == null) return;

          if (subscribers.hasAsync) processEvent(event, subscribers, true);
        }

        @Override
        public void onError(Throwable thrown) {
          eventLog.fatal(
              Events.this.context.getName() + ": Error processing async event queue", thrown);
        }

        @Override
        public void onTerminated() {
          eventLog.log(Events.this.context.getName() + ": Async event thread is terminated");
        }
      };
  private volatile Thread asyncEventThread;

  public Events(final Context context) {
    this.context = Objects.requireNonNull(context);
    this.subscribers = new ConcurrentHashMap<>();
    this.listenerToSubscriber = new ConcurrentHashMap<>();
  }

  @Override
  public synchronized void start() throws StartupException {
    if (this.isRunning == false) {
      this.asyncEventThread = new Thread(this.asyncEventProcessor);
      this.asyncEventThread.setDaemon(true);
      this.asyncEventThread.setName(this.context.getName() + " Async Events");
      this.asyncEventThread.setPriority(8);
      this.asyncEventThread.start();

      this.isRunning = true;
      eventLog.info(context.getName() + ": Events service started");
    } else throw new StartupException("Events service is already running");
  }

  @Override
  public synchronized void stop() throws TerminationException {
    if (this.isRunning == true) {
      this.isRunning = false;
      this.asyncEventProcessor.terminate(false);

      eventLog.info(context.getName() + ": Events service stopped");
    } else
      eventLog.warn(
          context.getName() + ": Attempted to stop Events service, but it was not running");
  }

  public void post(final Event event) {
    Objects.requireNonNull(event, "Event is null");

    if (this.isRunning == false) throw new IllegalStateException("Events service is not running");

    final Subscribers subscribers = this.subscribers.get(event.getClass());
    if (subscribers == null) return;

    if (subscribers.hasSync) processEvent(event, subscribers, false);

    if (subscribers.hasAsync) {
      try {
        if (this.asyncEventQueue.offer(event, 1, TimeUnit.SECONDS) == false) {
          Hyperscale.dumpThreadInfo();
          throw new TimeoutException();
        }
      } catch (InterruptedException | TimeoutException ex) {
        eventLog.error(this.context.getName() + ": Failed to queue event " + event, ex);
      }
    }
  }

  private void processEvent(final Event event, final Subscribers subscribers, final boolean async) {
    subscribers.lock.readLock().lock();
    try {
      final List<Subscriber> subsByType;
      if (async == false) subsByType = subscribers.sync;
      else subsByType = subscribers.async;

      for (int i = 0; i < subsByType.size(); i++) {
        final Subscriber subscriber = subsByType.get(i);
        processEvent(event, subscriber);
      }
    } finally {
      subscribers.lock.readLock().unlock();
    }
  }

  private void processEvent(final Event event, final Subscriber subscriber) {
    try {
      final MethodInfo methodInfo = subscriber.getMethod(event.getClass());
      methodInfo.getMethod().invoke(subscriber.getListener(), event);
    } catch (Throwable throwable) {
      eventLog.error(context.getName() + ": Error processing event", throwable);
    }
  }

  public synchronized void register(final EventListener listener) {
    Objects.requireNonNull(listener, "Event listener to register is null");

    if (this.listenerToSubscriber.containsKey(listener))
      throw new IllegalStateException(
          "Subscriber is already registered: " + listener.getClass().getName());

    final List<MethodInfo> methodsInfo = getSubscriberMethods(listener.getClass());
    final Subscriber subscriber = new Subscriber(listener, methodsInfo);
    this.listenerToSubscriber.put(listener, subscriber);

    for (final MethodInfo methodInfo : methodsInfo) {
      final Class<?> eventType = methodInfo.getParameterType();
      final Subscribers subscribers =
          this.subscribers.computeIfAbsent(eventType, t -> new Subscribers(t));

      subscribers.lock.writeLock().lock();
      try {
        subscribers.register(subscriber);
      } finally {
        subscribers.lock.writeLock().unlock();
      }
    }

    eventLog.info(
        context.getName() + ": Registered listener " + listener.getClass().getSimpleName());
  }

  public synchronized boolean unregister(final EventListener listener) {
    Objects.requireNonNull(listener, "Event listener to unregister is null");

    boolean removed = false;
    Subscriber subscriber = this.listenerToSubscriber.get(listener);
    if (subscriber != null) {
      for (final MethodInfo methodInfo : subscriber.methods.values()) {
        final Class<?> eventType = methodInfo.getParameterType();
        final Subscribers subscribers = this.subscribers.get(eventType);

        subscribers.lock.writeLock().lock();
        try {
          removed |= subscribers.unregister(subscriber);
        } finally {
          subscribers.lock.writeLock().unlock();
        }
      }
    }

    if (removed == true)
      eventLog.info(
          context.getName() + ": Unregistered listener " + listener.getClass().getSimpleName());
    else
      eventLog.warn(
          context.getName()
              + ": Attempted to unregister non-existent listener "
              + listener.getClass().getSimpleName());

    return removed;
  }

  private List<MethodInfo> getSubscriberMethods(final Class<?> clazz) {
    final Set<? extends Class<?>> supertypes = TypeToken.of(clazz).getTypes().rawTypes();
    final ImmutableList.Builder<MethodInfo> methodsInfo = ImmutableList.builder();
    for (final Class<?> supertype : supertypes) {
      for (final Method method : supertype.getDeclaredMethods()) {
        if (method.isAnnotationPresent(Subscribe.class) && !method.isSynthetic()) {
          // TODO(cgdecker): Should check for a generic parameter type and error out
          final Class<?>[] parameterTypes = method.getParameterTypes();
          Preconditions.checkArgument(
              parameterTypes.length == 1,
              "Method %s has @Subscribe annotation but has %s parameters. Subscriber methods must have exactly 1 parameter.",
              method,
              parameterTypes.length);

          Preconditions.checkArgument(
              parameterTypes[0].isPrimitive() == false,
              "@Subscribe method %s's parameter is %s. Subscriber methods cannot accept primitives. Consider changing the parameter to %s.",
              method,
              parameterTypes[0].getName(),
              Primitives.wrap(parameterTypes[0]).getSimpleName());

          Preconditions.checkArgument(
              Modifier.isAbstract(clazz.getModifiers()) == false,
              "@Subscribe method %s's parameter %s is abstract. Subscriber methods cannot accept primitives.",
              method,
              parameterTypes[0].getName());

          method.setAccessible(true);
          methodsInfo.add(new MethodInfo(method, parameterTypes[0]));
        }
      }
    }

    return methodsInfo.build();
  }
}
