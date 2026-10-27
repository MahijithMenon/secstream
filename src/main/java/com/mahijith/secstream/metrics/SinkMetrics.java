package com.mahijith.secstream.metrics;

import java.util.concurrent.atomic.LongAdder;
import java.util.function.IntSupplier;

/**
 * Per-sink delivery counters. For every sink, once the pipeline is idle:
 *
 * <pre>routed = delivered + deadLettered</pre>
 *
 * {@code writeAttempts} counts batch writes (including retries) and is not part of the invariant.
 */
public final class SinkMetrics {
  final String name;
  final LongAdder routed = new LongAdder();
  final LongAdder delivered = new LongAdder();
  final LongAdder deadLettered = new LongAdder();
  final LongAdder writeAttempts = new LongAdder();
  final LongAdder writeFailures = new LongAdder();
  final LongAdder retries = new LongAdder();
  final LatencyRecorder deliveryLatency = new LatencyRecorder();
  private volatile IntSupplier queueDepth = () -> 0;
  private volatile int queueCapacity;

  SinkMetrics(String name) {
    this.name = name;
  }

  public void bindQueue(IntSupplier depth, int capacity) {
    queueDepth = depth;
    queueCapacity = capacity;
  }

  public void routed() {
    routed.increment();
  }

  public void delivered(long admittedNanos, long nowNanos) {
    delivered.increment();
    deliveryLatency.recordNanos(nowNanos - admittedNanos);
  }

  public void deadLettered() {
    deadLettered.increment();
  }

  public void writeAttempt() {
    writeAttempts.increment();
  }

  public void writeFailure() {
    writeFailures.increment();
  }

  public void retry() {
    retries.increment();
  }

  SinkSnapshot snapshot() {
    return new SinkSnapshot(
        name,
        routed.sum(),
        delivered.sum(),
        deadLettered.sum(),
        writeAttempts.sum(),
        writeFailures.sum(),
        retries.sum(),
        queueDepth.getAsInt(),
        queueCapacity,
        deliveryLatency.snapshot());
  }

  public record SinkSnapshot(
      String name,
      long routed,
      long delivered,
      long deadLettered,
      long writeAttempts,
      long writeFailures,
      long retries,
      int queueDepth,
      int queueCapacity,
      LatencyRecorder.LatencySnapshot deliveryLatency) {}
}
