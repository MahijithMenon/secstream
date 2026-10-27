package com.mahijith.secstream.processing;

import com.mahijith.secstream.model.InvalidEventException;
import java.time.Duration;
import java.time.Instant;

/** Rejects event times that are implausibly far ahead of when the pipeline received them. */
final class TimestampPolicy {
  static final Duration MAX_FUTURE_SKEW = Duration.ofMinutes(5);

  private TimestampPolicy() {}

  static Instant check(Instant occurredAt, Instant receivedAt) {
    if (occurredAt.isAfter(receivedAt.plus(MAX_FUTURE_SKEW))) {
      throw new InvalidEventException(
          "event timestamp is more than " + MAX_FUTURE_SKEW.toMinutes() + " minutes in the future");
    }
    return occurredAt;
  }
}
