package com.mahijith.secstream.model;

import java.util.Arrays;
import java.util.Locale;

/** Raw wire formats the pipeline can normalize. The config name is used in YAML and HTTP paths. */
public enum EventFormat {
  SIGN_IN("signin"),
  FIREWALL("firewall");

  private final String configName;

  EventFormat(String configName) {
    this.configName = configName;
  }

  public String configName() {
    return configName;
  }

  public static EventFormat fromConfigName(String name) {
    String normalized = name.trim().toLowerCase(Locale.ROOT);
    return Arrays.stream(values())
        .filter(f -> f.configName.equals(normalized))
        .findFirst()
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    "unknown event format '"
                        + name
                        + "', expected one of "
                        + Arrays.stream(values()).map(EventFormat::configName).toList()));
  }
}
