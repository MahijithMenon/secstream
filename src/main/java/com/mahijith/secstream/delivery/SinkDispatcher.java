package com.mahijith.secstream.delivery;

import com.mahijith.secstream.metrics.SinkMetrics;
import com.mahijith.secstream.model.SecurityEvent;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.random.RandomGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Owns one sink: a bounded queue, one thread, batching, retries and dead-lettering.
 *
 * <p>Why a thread per sink instead of writing from the worker threads:
 *
 * <ul>
 *   <li><b>Batching.</b> The dispatcher drains whatever has accumulated (up to {@code batchSize})
 *       into one write, which is what makes JDBC batch inserts pay off. Under light load batches
 *       are small and latency stays low; under heavy load they grow and throughput rises.
 *   <li><b>Simpler sinks.</b> A single writer thread means sinks need no locking.
 *   <li><b>Retry isolation.</b> Backoff sleeps happen here, not on CPU workers.
 * </ul>
 *
 * <p>Backpressure: {@link #submit} blocks while the queue is full. If a sink is down, its queue
 * fills, workers block on it, the ingest queue fills, and sources block or get rejected. Nothing is
 * buffered without bound. The price is that one stuck sink eventually stalls all routing; the queue
 * capacity is the bulkhead that decides how long it can be absorbed.
 *
 * <p>Shutdown: {@link #finish} enqueues an end-of-stream marker after everything already submitted;
 * the thread delivers all prior items, closes the sink, and exits. {@link #forceStop} interrupts
 * it; whatever was not delivered is dead-lettered as {@link DeadLetterReason#SHUTDOWN_ABANDONED},
 * so even a forced shutdown loses nothing silently.
 */
public final class SinkDispatcher {
  private static final Logger log = LoggerFactory.getLogger(SinkDispatcher.class);

  private final EventSink sink;
  private final BlockingQueue<DispatchItem> queue;
  private final int batchSize;
  private final RetryPolicy retryPolicy;
  private final DeadLetterChannel deadLetters;
  private final SinkMetrics metrics;
  private final Sleeper sleeper;
  private final RandomGenerator random;
  private final Clock clock;
  private final Thread thread;

  public SinkDispatcher(
      EventSink sink,
      int queueCapacity,
      int batchSize,
      RetryPolicy retryPolicy,
      DeadLetterChannel deadLetters,
      SinkMetrics metrics,
      Sleeper sleeper,
      RandomGenerator random,
      Clock clock) {
    if (queueCapacity < 1 || batchSize < 1) {
      throw new IllegalArgumentException("queueCapacity and batchSize must be >= 1");
    }
    this.sink = sink;
    this.queue = new ArrayBlockingQueue<>(queueCapacity);
    this.batchSize = batchSize;
    this.retryPolicy = retryPolicy;
    this.deadLetters = deadLetters;
    this.metrics = metrics;
    this.sleeper = sleeper;
    this.random = random;
    this.clock = clock;
    this.thread = Thread.ofPlatform().name("sink-" + sink.name()).unstarted(this::run);
    metrics.bindQueue(queue::size, queueCapacity);
  }

  public String sinkName() {
    return sink.name();
  }

  /** Opens the sink on the caller's thread (so failures surface at startup) and starts. */
  public void start() throws SinkException {
    sink.open();
    thread.start();
  }

  /**
   * Blocks while the queue is full. Must not be called after {@link #finish}. If interrupted, the
   * delivery was not taken and the caller must account for it via {@link #abandonUnsubmitted}.
   */
  public void submit(Delivery delivery) throws InterruptedException {
    queue.put(delivery);
    // Counted after the hand-off so an interrupted put is never counted twice. A snapshot taken
    // mid-flight may therefore briefly show delivered > routed; the invariant is checked at rest.
    metrics.routed();
  }

  /**
   * Signals that no more deliveries will be submitted. The caller must guarantee that no {@link
   * #submit} is in progress or will follow. Waits up to {@code timeout} for queue space; returns
   * false if there was none (the sink is not draining), in which case use {@link #forceStop}.
   */
  public boolean finish(Duration timeout) throws InterruptedException {
    return queue.offer(DispatchItem.EndOfStream.INSTANCE, timeout.toNanos(), TimeUnit.NANOSECONDS);
  }

  public boolean awaitTermination(Duration timeout) throws InterruptedException {
    return thread.join(timeout);
  }

  public void forceStop() {
    thread.interrupt();
  }

  /** Records that a delivery could not even be submitted because shutdown interrupted it. */
  public void abandonUnsubmitted(Delivery delivery) {
    metrics.routed();
    deadLetter(delivery, DeadLetterReason.SHUTDOWN_ABANDONED, 0, "not submitted before shutdown");
  }

  private void run() {
    List<Delivery> batch = new ArrayList<>(batchSize);
    try {
      boolean endOfStream = false;
      while (!endOfStream) {
        DispatchItem first = queue.take();
        if (!(first instanceof Delivery head)) {
          break;
        }
        batch.add(head);
        List<DispatchItem> more = new ArrayList<>(batchSize - 1);
        queue.drainTo(more, batchSize - 1);
        for (DispatchItem item : more) {
          if (item instanceof Delivery d) {
            batch.add(d);
          } else {
            endOfStream = true;
          }
        }
        deliver(batch);
        batch.clear();
      }
    } catch (InterruptedException e) {
      abandon(batch);
      Thread.currentThread().interrupt();
    } catch (RuntimeException | Error e) {
      log.atError()
          .setMessage("sink dispatcher crashed")
          .addKeyValue("sink", sink.name())
          .setCause(e)
          .log();
      abandon(batch);
      throw e;
    } finally {
      closeSink();
    }
  }

  private void abandon(List<Delivery> inHand) {
    List<DispatchItem> remaining = new ArrayList<>();
    queue.drainTo(remaining);
    int count = 0;
    for (Delivery d : inHand) {
      deadLetter(d, DeadLetterReason.SHUTDOWN_ABANDONED, 0, "dispatcher stopped");
      count++;
    }
    for (DispatchItem item : remaining) {
      if (item instanceof Delivery d) {
        deadLetter(d, DeadLetterReason.SHUTDOWN_ABANDONED, 0, "dispatcher stopped");
        count++;
      }
    }
    if (count > 0) {
      log.atWarn()
          .setMessage("dispatcher stopped with undelivered events; dead-lettered")
          .addKeyValue("sink", sink.name())
          .addKeyValue("count", count)
          .log();
    }
  }

  /**
   * Writes a batch with retries. On a permanent failure of a multi-event batch, each event is
   * retried alone so one poison event cannot dead-letter its innocent neighbours.
   */
  private void deliver(List<Delivery> batch) throws InterruptedException {
    List<SecurityEvent> events = batch.stream().map(Delivery::event).toList();
    SinkException lastFailure = null;
    for (int attempt = 1; attempt <= retryPolicy.maxAttempts(); attempt++) {
      if (Thread.interrupted()) {
        throw new InterruptedException();
      }
      if (attempt > 1) {
        metrics.retry();
        sleeper.sleep(retryPolicy.backoffBefore(attempt - 1, random));
      }
      metrics.writeAttempt();
      try {
        sink.write(events);
        long now = System.nanoTime();
        for (Delivery d : batch) {
          metrics.delivered(d.admittedNanos(), now);
        }
        return;
      } catch (SinkException e) {
        metrics.writeFailure();
        lastFailure = e;
      } catch (RuntimeException e) {
        // An unexpected exception from a sink is most likely a bug; retry boundedly, then DLQ.
        metrics.writeFailure();
        lastFailure = SinkException.transientFailure("unexpected sink exception", e);
      }
      if (Thread.interrupted()) {
        // The write failed because a forced shutdown interrupted it, not because of the event.
        throw new InterruptedException();
      }
      logFailure(lastFailure, attempt, batch.size());
      if (!lastFailure.retryable()) {
        if (batch.size() > 1) {
          for (Delivery d : batch) {
            deliver(List.of(d));
          }
        } else {
          deadLetter(
              batch.get(0), DeadLetterReason.DELIVERY_REJECTED, attempt, describe(lastFailure));
        }
        return;
      }
    }
    for (Delivery d : batch) {
      deadLetter(
          d, DeadLetterReason.RETRIES_EXHAUSTED, retryPolicy.maxAttempts(), describe(lastFailure));
    }
  }

  private void logFailure(SinkException e, int attempt, int batchSize) {
    log.atWarn()
        .setMessage("sink write failed")
        .addKeyValue("sink", sink.name())
        .addKeyValue("attempt", attempt)
        .addKeyValue("maxAttempts", retryPolicy.maxAttempts())
        .addKeyValue("batchSize", batchSize)
        .addKeyValue("retryable", e.retryable())
        .addKeyValue("error", describe(e))
        .log();
  }

  private void deadLetter(Delivery d, DeadLetterReason reason, int attempts, String detail) {
    metrics.deadLettered();
    deadLetters.publish(
        new DeadLetter(
            clock.instant(),
            reason,
            d.source(),
            d.sequence(),
            sink.name(),
            attempts,
            detail,
            null,
            d.event()));
  }

  /** Exception class and message only; sinks are responsible for keeping messages payload-free. */
  private static String describe(Throwable e) {
    Throwable root = e.getCause() != null ? e.getCause() : e;
    return e.getMessage() + " (" + root.getClass().getSimpleName() + ")";
  }

  private void closeSink() {
    try {
      sink.close();
    } catch (RuntimeException e) {
      log.atError()
          .setMessage("sink close failed")
          .addKeyValue("sink", sink.name())
          .setCause(e)
          .log();
    }
  }
}
