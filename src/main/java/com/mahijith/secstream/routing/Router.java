package com.mahijith.secstream.routing;

import com.mahijith.secstream.model.SecurityEvent;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Fan-out router: every matching rule contributes its sinks, and each sink receives an event at
 * most once even if several rules name it. This is intentionally not Chain of Responsibility (first
 * match wins), because the core use case is "alerts AND archive", not "alerts OR archive".
 *
 * <p>Immutable and therefore thread-safe.
 */
public final class Router {
  private final List<RoutingRule> rules;

  public Router(List<RoutingRule> rules) {
    this.rules = List.copyOf(rules);
  }

  /** Target sink names in rule order; empty means the event is unrouted. */
  public List<String> route(SecurityEvent event) {
    Set<String> targets = new LinkedHashSet<>();
    for (RoutingRule rule : rules) {
      if (rule.matcher().matches(event)) {
        targets.addAll(rule.sinks());
      }
    }
    return List.copyOf(targets);
  }

  public List<RoutingRule> rules() {
    return rules;
  }
}
