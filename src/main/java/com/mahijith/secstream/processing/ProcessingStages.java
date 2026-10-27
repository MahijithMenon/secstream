package com.mahijith.secstream.processing;

import java.util.List;
import java.util.Set;

/** Registry of enrichers selectable from configuration by name. */
public final class ProcessingStages {
  public static final Set<String> ENRICHERS = Set.of("network-zone");

  private ProcessingStages() {}

  public static List<EventEnricher> enrichers(List<String> names) {
    return names.stream()
        .map(
            n ->
                switch (n) {
                  case "network-zone" -> (EventEnricher) new NetworkZoneEnricher();
                  default -> throw new IllegalArgumentException("unknown enricher " + n);
                })
        .toList();
  }
}
