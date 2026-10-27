package com.mahijith.secstream.processing;

import com.mahijith.secstream.model.EventFormat;
import com.mahijith.secstream.model.InvalidEventException;
import com.mahijith.secstream.model.RawEvent;
import com.mahijith.secstream.model.SecurityEvent;
import com.mahijith.secstream.routing.Router;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/**
 * Runs the synchronous per-event stages: normalize, severity filter, dedup, enrich, route.
 *
 * <p>Order matters. Severity filtering runs before dedup so that discarded low-severity events do
 * not consume dedup capacity. Dedup runs before enrichment so duplicates cost no enrichment work.
 *
 * <p>Thread-safe provided every collaborator is; all workers share one instance.
 */
public final class EventProcessor {
  private final Map<EventFormat, EventNormalizer> normalizers;
  private final SeverityFilter severityFilter;
  private final Deduplicator deduplicator;
  private final List<EventEnricher> enrichers;
  private final Router router;

  public EventProcessor(
      List<EventNormalizer> normalizers,
      SeverityFilter severityFilter,
      Deduplicator deduplicator,
      List<EventEnricher> enrichers,
      Router router) {
    Map<EventFormat, EventNormalizer> byFormat = new EnumMap<>(EventFormat.class);
    for (EventNormalizer n : normalizers) {
      if (byFormat.put(n.format(), n) != null) {
        throw new IllegalArgumentException("two normalizers registered for " + n.format());
      }
    }
    this.normalizers = Map.copyOf(byFormat);
    this.severityFilter = severityFilter;
    this.deduplicator = deduplicator;
    this.enrichers = List.copyOf(enrichers);
    this.router = router;
  }

  public static List<EventNormalizer> defaultNormalizers() {
    return List.of(new SignInNormalizer(), new FirewallNormalizer());
  }

  /**
   * Never throws for bad input; malformed events become {@link ProcessingResult.Malformed}. A
   * runtime exception escaping this method indicates a bug in a stage and is handled by the worker.
   */
  public ProcessingResult process(RawEvent raw) {
    EventNormalizer normalizer = normalizers.get(raw.format());
    if (normalizer == null) {
      return new ProcessingResult.Malformed("no normalizer for format " + raw.format());
    }
    SecurityEvent event;
    try {
      event = normalizer.normalize(raw);
    } catch (InvalidEventException e) {
      return new ProcessingResult.Malformed(e.getMessage());
    }
    if (!severityFilter.accepts(event)) {
      return new ProcessingResult.Filtered(event);
    }
    if (!deduplicator.firstSighting(event.id())) {
      return new ProcessingResult.Duplicate(event);
    }
    for (EventEnricher enricher : enrichers) {
      event = enricher.enrich(event);
    }
    List<String> sinks = router.route(event);
    return sinks.isEmpty()
        ? new ProcessingResult.Unrouted(event)
        : new ProcessingResult.Routed(event, sinks);
  }
}
