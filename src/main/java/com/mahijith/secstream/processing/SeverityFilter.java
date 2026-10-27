package com.mahijith.secstream.processing;

import com.mahijith.secstream.model.SecurityEvent;
import com.mahijith.secstream.model.Severity;
import java.util.Objects;

/** Keeps events at or above a minimum severity. Stateless and therefore thread-safe. */
public record SeverityFilter(Severity minimum) {
  public SeverityFilter {
    Objects.requireNonNull(minimum, "minimum");
  }

  public boolean accepts(SecurityEvent event) {
    return event.severity().isAtLeast(minimum);
  }
}
