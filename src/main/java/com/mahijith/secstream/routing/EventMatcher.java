package com.mahijith.secstream.routing;

import com.mahijith.secstream.model.EventCategory;
import com.mahijith.secstream.model.EventOutcome;
import com.mahijith.secstream.model.SecurityEvent;
import com.mahijith.secstream.model.Severity;
import java.util.Objects;
import java.util.Set;

/**
 * Conjunction of simple conditions. An empty set means "any value"; {@code minSeverity} of INFO
 * matches everything.
 */
public record EventMatcher(
    Severity minSeverity, Set<EventCategory> categories, Set<EventOutcome> outcomes) {

  public static final EventMatcher ANY = new EventMatcher(Severity.INFO, Set.of(), Set.of());

  public EventMatcher {
    Objects.requireNonNull(minSeverity, "minSeverity");
    categories = Set.copyOf(categories);
    outcomes = Set.copyOf(outcomes);
  }

  public boolean matches(SecurityEvent event) {
    return event.severity().isAtLeast(minSeverity)
        && (categories.isEmpty() || categories.contains(event.category()))
        && (outcomes.isEmpty() || outcomes.contains(event.outcome()));
  }
}
