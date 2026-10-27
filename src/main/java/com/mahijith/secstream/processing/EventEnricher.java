package com.mahijith.secstream.processing;

import com.mahijith.secstream.model.SecurityEvent;

/**
 * Adds derived context to an event. Enrichers run on worker threads and must be thread-safe. They
 * must be local and deterministic: event data is never sent to an external service.
 */
public interface EventEnricher {
  SecurityEvent enrich(SecurityEvent event);
}
