package com.mahijith.secstream.routing;

import java.util.List;
import java.util.Objects;

public record RoutingRule(String name, EventMatcher matcher, List<String> sinks) {
  public RoutingRule {
    Objects.requireNonNull(name, "name");
    Objects.requireNonNull(matcher, "matcher");
    sinks = List.copyOf(sinks);
    if (sinks.isEmpty()) {
      throw new IllegalArgumentException("routing rule '" + name + "' has no sinks");
    }
  }
}
