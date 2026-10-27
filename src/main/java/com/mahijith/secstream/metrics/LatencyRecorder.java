package com.mahijith.secstream.metrics;

import java.util.concurrent.TimeUnit;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;

/**
 * Latency distribution with wait-free recording.
 *
 * <p>Writers (worker and dispatcher threads) call {@link #recordNanos}, which never blocks. Readers
 * call {@link #snapshot}, which swaps out the recorder's interval histogram and folds it into a
 * cumulative one. {@link Recorder} only supports a single concurrent reader, hence the synchronized
 * snapshot.
 */
public final class LatencyRecorder {
  private static final long HIGHEST_TRACKABLE_MICROS = TimeUnit.HOURS.toMicros(1);
  private static final int SIGNIFICANT_DIGITS = 3;

  private final Recorder recorder = new Recorder(HIGHEST_TRACKABLE_MICROS, SIGNIFICANT_DIGITS);
  private final Histogram cumulative = new Histogram(HIGHEST_TRACKABLE_MICROS, SIGNIFICANT_DIGITS);
  private Histogram interval = null;

  public void recordNanos(long nanos) {
    long micros =
        Math.max(0, Math.min(TimeUnit.NANOSECONDS.toMicros(nanos), HIGHEST_TRACKABLE_MICROS));
    recorder.recordValue(micros);
  }

  public synchronized LatencySnapshot snapshot() {
    interval = recorder.getIntervalHistogram(interval);
    cumulative.add(interval);
    return new LatencySnapshot(
        cumulative.getTotalCount(),
        cumulative.getValueAtPercentile(50),
        cumulative.getValueAtPercentile(95),
        cumulative.getValueAtPercentile(99),
        cumulative.getMaxValue());
  }

  /** Cumulative latency since start, in microseconds. */
  public record LatencySnapshot(
      long count, long p50Micros, long p95Micros, long p99Micros, long maxMicros) {}
}
