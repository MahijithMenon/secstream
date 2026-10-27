package com.mahijith.secstream.processing;

import com.mahijith.secstream.model.SecurityEvent;
import java.util.List;

/**
 * Terminal outcome of the per-event processing stages (everything before sink delivery). Exactly
 * one result is produced per admitted event; the pipeline's accounting is built on that fact.
 */
public sealed interface ProcessingResult {
  record Routed(SecurityEvent event, List<String> sinks) implements ProcessingResult {
    public Routed {
      sinks = List.copyOf(sinks);
    }
  }

  record Filtered(SecurityEvent event) implements ProcessingResult {}

  record Duplicate(SecurityEvent event) implements ProcessingResult {}

  record Unrouted(SecurityEvent event) implements ProcessingResult {}

  /** The payload could not be parsed or violated a domain constraint. */
  record Malformed(String reason) implements ProcessingResult {}
}
