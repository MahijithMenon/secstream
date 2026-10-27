package com.mahijith.secstream.metrics;

import com.mahijith.secstream.delivery.DeadLetterReason;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

/**
 * Point-in-time copy of every counter. See {@code docs/METRICS.md} for definitions.
 *
 * <p>Accounting boundaries:
 *
 * <ol>
 *   <li><b>Admission.</b> Every submission is either accepted or rejected (full / too large /
 *       closed). Rejected events never enter the pipeline and are the caller's responsibility.
 *   <li><b>Processing.</b> Every accepted event reaches exactly one processing outcome.
 *   <li><b>Delivery.</b> A routed event fans out to one or more sinks; each (event, sink) pair
 *       reaches exactly one delivery outcome: delivered or dead-lettered.
 * </ol>
 */
public record MetricsSnapshot(
    long accepted,
    long rejectedQueueFull,
    long rejectedTooLarge,
    long rejectedClosed,
    long malformed,
    long processingErrors,
    long filtered,
    long duplicates,
    long unrouted,
    long routed,
    long abandoned,
    int queueDepth,
    int queueCapacity,
    Map<DeadLetterReason, Long> deadLetters,
    long deadLetterWriteFailures,
    LatencyRecorder.LatencySnapshot processingLatency,
    Map<String, SinkMetrics.SinkSnapshot> sinks) {

  public MetricsSnapshot {
    deadLetters = Map.copyOf(deadLetters);
    sinks = Collections.unmodifiableMap(new TreeMap<>(sinks));
  }

  public long rejected() {
    return rejectedQueueFull + rejectedTooLarge + rejectedClosed;
  }

  /** Accepted events that have reached a processing outcome. */
  public long processed() {
    return malformed + processingErrors + filtered + duplicates + unrouted + routed + abandoned;
  }

  /** Accepted events not yet processed (queued or on a worker). Zero once idle. */
  public long inFlight() {
    return accepted - processed();
  }

  public long deadLettersTotal() {
    return deadLetters.values().stream().mapToLong(Long::longValue).sum();
  }

  /** Invariant 2: holds exactly once the pipeline is idle. */
  public boolean processingBalanced() {
    return accepted == processed();
  }

  /** Invariant 3, for every sink: holds exactly once that sink's queue is drained. */
  public boolean deliveriesBalanced() {
    return sinks.values().stream().allMatch(s -> s.routed() == s.delivered() + s.deadLettered());
  }
}
