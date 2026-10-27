package com.mahijith.secstream.pipeline;

import com.mahijith.secstream.delivery.DeadLetter;
import com.mahijith.secstream.delivery.DeadLetterChannel;
import com.mahijith.secstream.delivery.DeadLetterReason;
import com.mahijith.secstream.delivery.Delivery;
import com.mahijith.secstream.delivery.SinkDispatcher;
import com.mahijith.secstream.delivery.SinkException;
import com.mahijith.secstream.metrics.PipelineMetrics;
import com.mahijith.secstream.model.EventFormat;
import com.mahijith.secstream.model.RawEvent;
import com.mahijith.secstream.processing.EventProcessor;
import com.mahijith.secstream.processing.ProcessingResult;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bounded ingest queue, worker pool, and the per-sink dispatchers behind it.
 *
 * <pre>
 * sources --submit--> [ingest queue] --take--> workers --process--> [sink queue] --> dispatcher --> sink
 *          (blocks or              (bounded)    (N threads)  (route)    (bounded)    (1 thread/sink)
 *           rejects when full)
 * </pre>
 *
 * <p><b>Admission and shutdown race.</b> A naive "if (!closed) queue.put(e)" has a window where
 * shutdown sets {@code closed}, workers drain the queue and exit, and then the put lands in a queue
 * nobody reads: an accepted event is silently lost. Submitters therefore hold the read lock of a
 * {@link ReentrantReadWriteLock} across check-and-offer; shutdown takes the write lock to set
 * {@code closed}. Once shutdown holds the write lock, every in-progress offer has completed and no
 * new one can start, so the end-of-stream markers it enqueues next are guaranteed to be the last
 * items. A blocking submit waits in bounded offers, releasing the lock in between, so a producer
 * stuck on a full queue can never stall shutdown; it is answered with REJECTED_CLOSED instead.
 *
 * <p><b>Shutdown order</b> (each step waits for the previous one): close admission; enqueue one
 * end-of-stream marker per worker; wait for workers to drain the ingest queue; signal every
 * dispatcher; wait for dispatchers to drain their queues and close their sinks; close the
 * dead-letter store. On timeout the remaining work is interrupted and dead-lettered as {@link
 * DeadLetterReason#SHUTDOWN_ABANDONED}, so accounting still balances.
 */
public final class Pipeline implements EventIngress, AutoCloseable {
  private static final Logger log = LoggerFactory.getLogger(Pipeline.class);
  private static final Duration FORCED_STOP_GRACE = Duration.ofSeconds(2);
  private static final Duration ADMISSION_POLL = Duration.ofMillis(50);

  /** Compared by identity. Never counted, never processed. */
  private static final RawEvent END_OF_STREAM =
      new RawEvent(-1, "end-of-stream", EventFormat.SIGN_IN, "", Instant.EPOCH, 0);

  private enum State {
    NEW,
    RUNNING,
    STOPPING,
    TERMINATED
  }

  private final PipelineSettings settings;
  private final EventProcessor processor;
  private final Map<String, SinkDispatcher> dispatchers;
  private final DeadLetterChannel deadLetters;
  private final PipelineMetrics metrics;
  private final Clock clock;

  private final BlockingQueue<RawEvent> queue;
  private final ReentrantReadWriteLock admissionLock = new ReentrantReadWriteLock();
  private boolean closed; // guarded by admissionLock
  private final AtomicLong sequence = new AtomicLong();
  private final AtomicReference<State> state = new AtomicReference<>(State.NEW);
  private final ExecutorService workerPool;

  public Pipeline(
      PipelineSettings settings,
      EventProcessor processor,
      List<SinkDispatcher> dispatchers,
      DeadLetterChannel deadLetters,
      PipelineMetrics metrics,
      Clock clock) {
    this.settings = settings;
    this.processor = processor;
    Map<String, SinkDispatcher> byName = new LinkedHashMap<>();
    for (SinkDispatcher d : dispatchers) {
      if (byName.put(d.sinkName(), d) != null) {
        throw new IllegalArgumentException("duplicate sink name " + d.sinkName());
      }
    }
    this.dispatchers = Map.copyOf(byName);
    this.deadLetters = deadLetters;
    this.metrics = metrics;
    this.clock = clock;
    this.queue = new ArrayBlockingQueue<>(settings.queueCapacity());
    this.workerPool = Executors.newFixedThreadPool(settings.workers(), namedThreads("worker-"));
    metrics.bindIngestQueue(queue::size, settings.queueCapacity());
  }

  /**
   * Opens every sink and starts the threads. If any sink fails to open, the ones already started
   * are stopped and the error is rethrown: the pipeline never runs with a sink missing.
   */
  public void start() throws SinkException {
    if (!state.compareAndSet(State.NEW, State.RUNNING)) {
      throw new IllegalStateException("pipeline already started");
    }
    List<SinkDispatcher> started = new ArrayList<>();
    try {
      for (SinkDispatcher d : dispatchers.values()) {
        d.start();
        started.add(d);
      }
    } catch (SinkException | RuntimeException e) {
      for (SinkDispatcher d : started) {
        d.forceStop();
      }
      workerPool.shutdownNow();
      state.set(State.TERMINATED);
      throw e;
    }
    for (int i = 0; i < settings.workers(); i++) {
      workerPool.execute(this::workerLoop);
    }
    log.atInfo()
        .setMessage("pipeline started")
        .addKeyValue("workers", settings.workers())
        .addKeyValue("queueCapacity", settings.queueCapacity())
        .addKeyValue("sinks", dispatchers.keySet())
        .log();
  }

  @Override
  public Admission submit(String source, EventFormat format, String payload)
      throws InterruptedException {
    return admit(source, format, payload, null);
  }

  @Override
  public Admission trySubmit(String source, EventFormat format, String payload, Duration timeout)
      throws InterruptedException {
    return admit(source, format, payload, timeout);
  }

  private Admission admit(
      String source, EventFormat format, String payload, @Nullable Duration timeout)
      throws InterruptedException {
    if (payload.length() > settings.maxPayloadChars()) {
      metrics.rejectedTooLarge();
      return Admission.REJECTED_TOO_LARGE;
    }
    boolean blocking = timeout == null;
    // nanoTime() may be negative, so "no deadline" is a flag, never Long.MAX_VALUE arithmetic.
    long deadline = blocking ? 0 : System.nanoTime() + timeout.toNanos();
    RawEvent raw = null;
    while (true) {
      // The read lock is held for one bounded offer at a time, never for an unbounded put, so
      // shutdown can always acquire the write lock within ADMISSION_POLL.
      admissionLock.readLock().lockInterruptibly();
      try {
        if (closed || state.get() != State.RUNNING) {
          metrics.rejectedClosed();
          return Admission.REJECTED_CLOSED;
        }
        if (raw == null) {
          raw =
              new RawEvent(
                  sequence.incrementAndGet(),
                  source,
                  format,
                  payload,
                  clock.instant(),
                  System.nanoTime());
        }
        long wait =
            blocking
                ? ADMISSION_POLL.toNanos()
                : Math.min(ADMISSION_POLL.toNanos(), Math.max(0, deadline - System.nanoTime()));
        // Count before the offer so a worker can never process an event not yet "accepted".
        metrics.accepted();
        boolean queued;
        try {
          queued = queue.offer(raw, wait, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
          metrics.unaccepted();
          throw e;
        }
        if (queued) {
          return Admission.ACCEPTED;
        }
        metrics.unaccepted();
      } finally {
        admissionLock.readLock().unlock();
      }
      if (!blocking && System.nanoTime() - deadline >= 0) {
        metrics.rejectedQueueFull();
        return Admission.REJECTED_QUEUE_FULL;
      }
    }
  }

  private void workerLoop() {
    try {
      while (true) {
        RawEvent raw = queue.take();
        if (raw == END_OF_STREAM) {
          return;
        }
        if (!handle(raw)) {
          return;
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /** Returns false if the worker was interrupted and must stop. */
  private boolean handle(RawEvent raw) {
    long started = System.nanoTime();
    ProcessingResult result;
    try {
      result = processor.process(raw);
    } catch (RuntimeException e) {
      metrics.processingError();
      log.atError()
          .setMessage("processing stage threw; event dead-lettered")
          .addKeyValue("source", raw.sourceName())
          .addKeyValue("sequence", raw.sequence())
          .setCause(e)
          .log();
      deadLetterRaw(raw, DeadLetterReason.PROCESSING_ERROR, e.getClass().getSimpleName());
      return true;
    } finally {
      metrics.processingNanos(System.nanoTime() - started);
    }
    switch (result) {
      case ProcessingResult.Malformed m -> {
        metrics.malformed();
        deadLetterRaw(raw, DeadLetterReason.MALFORMED, m.reason());
      }
      case ProcessingResult.Filtered f -> metrics.filtered();
      case ProcessingResult.Duplicate d -> metrics.duplicate();
      case ProcessingResult.Unrouted u -> metrics.unrouted();
      case ProcessingResult.Routed r -> {
        metrics.routed();
        return fanOut(raw, r);
      }
    }
    return true;
  }

  private boolean fanOut(RawEvent raw, ProcessingResult.Routed routed) {
    List<String> targets = routed.sinks();
    for (int i = 0; i < targets.size(); i++) {
      SinkDispatcher dispatcher = dispatchers.get(targets.get(i));
      Delivery delivery =
          new Delivery(routed.event(), raw.sourceName(), raw.sequence(), raw.admittedNanos());
      try {
        dispatcher.submit(delivery);
      } catch (InterruptedException e) {
        // Forced shutdown while blocked on a full sink queue: account for this and every
        // remaining target so no (event, sink) pair goes missing.
        for (int j = i; j < targets.size(); j++) {
          dispatchers.get(targets.get(j)).abandonUnsubmitted(delivery);
        }
        Thread.currentThread().interrupt();
        return false;
      }
    }
    return true;
  }

  private void deadLetterRaw(RawEvent raw, DeadLetterReason reason, String detail) {
    deadLetters.publish(
        new DeadLetter(
            clock.instant(),
            reason,
            raw.sourceName(),
            raw.sequence(),
            null,
            0,
            detail,
            raw.payload(),
            null));
  }

  /** Graceful shutdown with a deadline; see the class comment for the exact sequence. */
  public ShutdownReport shutdown(Duration timeout) throws InterruptedException {
    if (!state.compareAndSet(State.RUNNING, State.STOPPING)) {
      return new ShutdownReport(true, 0);
    }
    long deadline = System.nanoTime() + timeout.toNanos();
    closeAdmission();
    boolean graceful = stopWorkers(deadline);
    long abandoned = drainUnprocessed();
    graceful &= stopDispatchers(deadline);
    try {
      deadLetters.close();
    } catch (IOException e) {
      log.atError().setMessage("closing dead-letter store failed").setCause(e).log();
    }
    state.set(State.TERMINATED);
    log.atInfo()
        .setMessage("pipeline stopped")
        .addKeyValue("graceful", graceful)
        .addKeyValue("abandoned", abandoned)
        .log();
    return new ShutdownReport(graceful && abandoned == 0, abandoned);
  }

  private void closeAdmission() throws InterruptedException {
    // Submitters hold the read lock for at most ADMISSION_POLL, so this cannot wait for long.
    admissionLock.writeLock().lockInterruptibly();
    try {
      closed = true;
    } finally {
      admissionLock.writeLock().unlock();
    }
  }

  private boolean stopWorkers(long deadline) throws InterruptedException {
    for (int i = 0; i < settings.workers(); i++) {
      if (!queue.offer(END_OF_STREAM, remaining(deadline), TimeUnit.NANOSECONDS)) {
        break; // workers are not draining; fall through to the forced path
      }
    }
    workerPool.shutdown();
    if (workerPool.awaitTermination(remaining(deadline), TimeUnit.NANOSECONDS)) {
      return true;
    }
    log.atWarn().setMessage("workers did not finish before deadline; interrupting").log();
    workerPool.shutdownNow();
    if (!workerPool.awaitTermination(FORCED_STOP_GRACE.toNanos(), TimeUnit.NANOSECONDS)) {
      log.atError().setMessage("workers ignored interruption").log();
    }
    return false;
  }

  /** Dead-letters accepted events still in the ingest queue after the workers have stopped. */
  private long drainUnprocessed() {
    List<RawEvent> left = new ArrayList<>();
    queue.drainTo(left);
    long count = 0;
    for (RawEvent raw : left) {
      if (raw != END_OF_STREAM) {
        metrics.abandoned();
        deadLetterRaw(raw, DeadLetterReason.SHUTDOWN_ABANDONED, "not processed before shutdown");
        count++;
      }
    }
    return count;
  }

  private boolean stopDispatchers(long deadline) throws InterruptedException {
    boolean graceful = true;
    for (SinkDispatcher d : dispatchers.values()) {
      if (!d.finish(Duration.ofNanos(remaining(deadline)))) {
        graceful = false;
      }
    }
    for (SinkDispatcher d : dispatchers.values()) {
      if (!d.awaitTermination(Duration.ofNanos(remaining(deadline)))) {
        log.atWarn()
            .setMessage("sink did not drain before deadline; interrupting")
            .addKeyValue("sink", d.sinkName())
            .log();
        d.forceStop();
        d.awaitTermination(FORCED_STOP_GRACE);
        graceful = false;
      }
    }
    return graceful;
  }

  private static long remaining(long deadline) {
    return Math.max(0, deadline - System.nanoTime());
  }

  public PipelineMetrics metrics() {
    return metrics;
  }

  public boolean isRunning() {
    return state.get() == State.RUNNING;
  }

  @Override
  public void close() {
    try {
      shutdown(Duration.ofSeconds(30));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private static ThreadFactory namedThreads(String prefix) {
    return Thread.ofPlatform().name(prefix, 1).factory();
  }
}
