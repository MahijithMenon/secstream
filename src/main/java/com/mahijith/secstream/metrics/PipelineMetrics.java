package com.mahijith.secstream.metrics;

import com.mahijith.secstream.delivery.DeadLetterReason;
import java.util.Collection;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.IntSupplier;

/**
 * All pipeline counters.
 *
 * <p>Counters use {@link LongAdder}: they are incremented on every event by many threads and read
 * rarely, which is exactly the trade LongAdder makes (striped cells, no CAS contention on write,
 * O(cells) read). An {@code AtomicLong} would be the right choice for a value that needs an exact
 * read-modify-write result, such as the admission sequence number in the pipeline.
 *
 * <p>A snapshot reads each counter independently, so while events are in flight the counters are
 * not mutually consistent. The accounting invariants in {@link MetricsSnapshot} hold only once the
 * pipeline is idle (all queues drained), which is when tests check them.
 */
public final class PipelineMetrics {
  final LongAdder accepted = new LongAdder();
  final LongAdder rejectedQueueFull = new LongAdder();
  final LongAdder rejectedTooLarge = new LongAdder();
  final LongAdder rejectedClosed = new LongAdder();

  final LongAdder malformed = new LongAdder();
  final LongAdder processingErrors = new LongAdder();
  final LongAdder filtered = new LongAdder();
  final LongAdder duplicates = new LongAdder();
  final LongAdder unrouted = new LongAdder();
  final LongAdder routed = new LongAdder();
  final LongAdder abandoned = new LongAdder();

  final LongAdder deadLetterWriteFailures = new LongAdder();
  private final Map<DeadLetterReason, LongAdder> deadLetters =
      new EnumMap<>(DeadLetterReason.class);

  final LatencyRecorder processingLatency = new LatencyRecorder();
  private final Map<String, SinkMetrics> sinks = new ConcurrentHashMap<>();
  private volatile IntSupplier queueDepth = () -> 0;
  private volatile int queueCapacity;

  public PipelineMetrics() {
    for (DeadLetterReason reason : DeadLetterReason.values()) {
      deadLetters.put(reason, new LongAdder());
    }
  }

  public SinkMetrics sink(String name) {
    return sinks.computeIfAbsent(name, SinkMetrics::new);
  }

  public void bindIngestQueue(IntSupplier depth, int capacity) {
    queueDepth = depth;
    queueCapacity = capacity;
  }

  public void accepted() {
    accepted.increment();
  }

  /** Reverses {@link #accepted} when the queue offer that followed it did not succeed. */
  public void unaccepted() {
    accepted.decrement();
  }

  public void rejectedQueueFull() {
    rejectedQueueFull.increment();
  }

  public void rejectedTooLarge() {
    rejectedTooLarge.increment();
  }

  public void rejectedClosed() {
    rejectedClosed.increment();
  }

  public void malformed() {
    malformed.increment();
  }

  public void processingError() {
    processingErrors.increment();
  }

  public void filtered() {
    filtered.increment();
  }

  public void duplicate() {
    duplicates.increment();
  }

  public void unrouted() {
    unrouted.increment();
  }

  public void routed() {
    routed.increment();
  }

  /** An accepted event that was never processed because a forced shutdown interrupted it. */
  public void abandoned() {
    abandoned.increment();
  }

  public void processingNanos(long nanos) {
    processingLatency.recordNanos(nanos);
  }

  public void deadLetterWritten(DeadLetterReason reason) {
    deadLetters.get(reason).increment();
  }

  public void deadLetterWriteFailed() {
    deadLetterWriteFailures.increment();
  }

  public MetricsSnapshot snapshot() {
    Map<DeadLetterReason, Long> dl = new EnumMap<>(DeadLetterReason.class);
    deadLetters.forEach((reason, adder) -> dl.put(reason, adder.sum()));
    Map<String, SinkMetrics.SinkSnapshot> sinkSnapshots = new LinkedHashMap<>();
    Collection<SinkMetrics> all = sinks.values();
    List<SinkMetrics> ordered = all.stream().sorted((a, b) -> a.name.compareTo(b.name)).toList();
    for (SinkMetrics s : ordered) {
      sinkSnapshots.put(s.name, s.snapshot());
    }
    return new MetricsSnapshot(
        accepted.sum(),
        rejectedQueueFull.sum(),
        rejectedTooLarge.sum(),
        rejectedClosed.sum(),
        malformed.sum(),
        processingErrors.sum(),
        filtered.sum(),
        duplicates.sum(),
        unrouted.sum(),
        routed.sum(),
        abandoned.sum(),
        queueDepth.getAsInt(),
        queueCapacity,
        Map.copyOf(dl),
        deadLetterWriteFailures.sum(),
        processingLatency.snapshot(),
        Map.copyOf(sinkSnapshots));
  }
}
