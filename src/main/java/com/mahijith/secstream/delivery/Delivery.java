package com.mahijith.secstream.delivery;

import com.mahijith.secstream.model.SecurityEvent;
import java.util.Objects;

/** An event on its way to one sink, carrying the admission timestamp for latency metrics. */
public record Delivery(SecurityEvent event, String source, long sequence, long admittedNanos)
    implements DispatchItem {
  public Delivery {
    Objects.requireNonNull(event, "event");
    Objects.requireNonNull(source, "source");
  }
}
