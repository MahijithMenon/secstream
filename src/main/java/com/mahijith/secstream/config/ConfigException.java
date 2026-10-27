package com.mahijith.secstream.config;

import java.util.List;

/** Invalid configuration. Carries every problem found, not just the first. */
public final class ConfigException extends Exception {
  private static final long serialVersionUID = 1L;

  @SuppressWarnings(
      "serial") // an immutable List.copyOf result; exceptions are never serialized here
  private final List<String> problems;

  public ConfigException(List<String> problems) {
    super("invalid configuration:\n  - " + String.join("\n  - ", problems));
    this.problems = List.copyOf(problems);
  }

  public List<String> problems() {
    return problems;
  }
}
