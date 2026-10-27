package com.mahijith.secstream.delivery;

import com.mahijith.secstream.model.SecurityEvent;
import java.time.Instant;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * One undeliverable item. Exactly one of {@code rawPayload} (before normalization) or {@code event}
 * (after) is set, so a dead letter always contains enough to replay it.
 *
 * @param sink the sink that failed, or null when the failure happened before routing
 * @param attempts number of write attempts made for this sink (0 before routing)
 * @param detail a log-safe description of the failure; must not contain payload content
 */
public record DeadLetter(
    Instant timestamp,
    DeadLetterReason reason,
    String source,
    long sequence,
    @Nullable String sink,
    int attempts,
    String detail,
    @Nullable String rawPayload,
    @Nullable SecurityEvent event) {

  public DeadLetter {
    Objects.requireNonNull(timestamp, "timestamp");
    Objects.requireNonNull(reason, "reason");
    Objects.requireNonNull(source, "source");
    Objects.requireNonNull(detail, "detail");
    if ((rawPayload == null) == (event == null)) {
      throw new IllegalArgumentException("exactly one of rawPayload or event must be set");
    }
  }
}
