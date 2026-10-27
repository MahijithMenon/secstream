package com.mahijith.secstream.model;

import java.time.Instant;
import java.util.Objects;

/**
 * An admitted but not yet parsed event. The payload is untrusted text; nothing downstream of the
 * normalizer ever looks at it except the dead-letter queue.
 *
 * @param sequence process-unique admission number, used to correlate logs and dead letters
 * @param admittedNanos {@link System#nanoTime()} at admission, the start of latency measurement
 */
public record RawEvent(
    long sequence,
    String sourceName,
    EventFormat format,
    String payload,
    Instant receivedAt,
    long admittedNanos) {

  public RawEvent {
    Objects.requireNonNull(sourceName, "sourceName");
    Objects.requireNonNull(format, "format");
    Objects.requireNonNull(payload, "payload");
    Objects.requireNonNull(receivedAt, "receivedAt");
  }
}
