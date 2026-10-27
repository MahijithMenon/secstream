package com.mahijith.secstream.model;

import java.util.Locale;

/** Normalized severity. Declaration order is significant: later constants are more severe. */
public enum Severity {
  INFO,
  LOW,
  MEDIUM,
  HIGH,
  CRITICAL;

  public boolean isAtLeast(Severity threshold) {
    return compareTo(threshold) >= 0;
  }

  /** Case-insensitive lookup used by configuration; throws with the allowed values listed. */
  public static Severity parse(String value) {
    try {
      return valueOf(value.trim().toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          "unknown severity '"
              + value
              + "', expected one of "
              + java.util.Arrays.toString(values()),
          e);
    }
  }
}
