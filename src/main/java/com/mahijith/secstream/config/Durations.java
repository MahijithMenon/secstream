package com.mahijith.secstream.config;

import java.time.Duration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Parses human-friendly durations: {@code 250ms}, {@code 5s}, {@code 2m}. */
public final class Durations {
  private static final Pattern FORMAT = Pattern.compile("^(\\d{1,9})(ms|s|m)$");

  private Durations() {}

  public static Duration parse(String text) {
    Matcher m = FORMAT.matcher(text.trim());
    if (!m.matches()) {
      throw new IllegalArgumentException("'" + text + "' is not a duration like 250ms, 5s or 2m");
    }
    long amount = Long.parseLong(m.group(1));
    return switch (m.group(2)) {
      case "ms" -> Duration.ofMillis(amount);
      case "s" -> Duration.ofSeconds(amount);
      default -> Duration.ofMinutes(amount);
    };
  }
}
