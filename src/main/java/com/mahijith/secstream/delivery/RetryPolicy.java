package com.mahijith.secstream.delivery;

import java.time.Duration;
import java.util.Objects;
import java.util.random.RandomGenerator;

/**
 * Bounded exponential backoff with "equal jitter": the delay before retry {@code n} is uniformly
 * distributed in {@code [cap/2, cap]} where {@code cap = min(maxBackoff, initial *
 * multiplier^(n-1))}.
 *
 * <p>Jitter de-synchronizes retries from several pipeline instances hitting the same database;
 * keeping the lower half avoids the near-zero delays that "full jitter" can produce, which would
 * hammer a sink that is down.
 *
 * @param maxAttempts total attempts including the first; 1 disables retries
 */
public record RetryPolicy(
    int maxAttempts, Duration initialBackoff, Duration maxBackoff, double multiplier) {

  public static final RetryPolicy NONE = new RetryPolicy(1, Duration.ZERO, Duration.ZERO, 1.0);

  public RetryPolicy {
    Objects.requireNonNull(initialBackoff, "initialBackoff");
    Objects.requireNonNull(maxBackoff, "maxBackoff");
    if (maxAttempts < 1) {
      throw new IllegalArgumentException("maxAttempts must be >= 1");
    }
    if (initialBackoff.isNegative() || maxBackoff.compareTo(initialBackoff) < 0) {
      throw new IllegalArgumentException("require 0 <= initialBackoff <= maxBackoff");
    }
    if (!(multiplier >= 1.0) || Double.isInfinite(multiplier)) {
      throw new IllegalArgumentException("multiplier must be a finite value >= 1.0");
    }
  }

  /**
   * @param retry 1 for the delay before the second attempt, 2 before the third, and so on
   */
  public Duration backoffBefore(int retry, RandomGenerator random) {
    if (retry < 1) {
      throw new IllegalArgumentException("retry must be >= 1");
    }
    double capNanos =
        Math.min(maxBackoff.toNanos(), initialBackoff.toNanos() * Math.pow(multiplier, retry - 1));
    long cap = (long) capNanos;
    if (cap <= 0) {
      return Duration.ZERO;
    }
    long half = cap / 2;
    return Duration.ofNanos(half + random.nextLong(cap - half + 1));
  }
}
