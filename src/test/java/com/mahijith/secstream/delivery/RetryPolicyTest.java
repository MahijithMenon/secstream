package com.mahijith.secstream.delivery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.SplittableRandom;
import org.junit.jupiter.api.Test;

class RetryPolicyTest {
  private final RetryPolicy policy =
      new RetryPolicy(6, Duration.ofMillis(100), Duration.ofSeconds(1), 2.0);

  @Test
  void backoffGrowsExponentiallyWithinJitterBoundsAndIsCapped() {
    SplittableRandom random = new SplittableRandom(1);
    long[] caps = {100, 200, 400, 800, 1000, 1000};
    for (int retry = 1; retry <= caps.length; retry++) {
      for (int sample = 0; sample < 1_000; sample++) {
        long ms = policy.backoffBefore(retry, random).toMillis();
        assertThat(ms).isBetween(caps[retry - 1] / 2, caps[retry - 1]);
      }
    }
  }

  @Test
  void jitterActuallyVaries() {
    SplittableRandom random = new SplittableRandom(2);
    long distinct =
        java.util.stream.IntStream.range(0, 100)
            .mapToObj(i -> policy.backoffBefore(3, random))
            .distinct()
            .count();
    assertThat(distinct).isGreaterThan(50);
  }

  @Test
  void hugeRetryNumbersDoNotOverflow() {
    assertThat(policy.backoffBefore(10_000, new SplittableRandom(3)))
        .isLessThanOrEqualTo(Duration.ofSeconds(1));
  }

  @Test
  void validatesArguments() {
    assertThatThrownBy(() -> new RetryPolicy(0, Duration.ZERO, Duration.ZERO, 2))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new RetryPolicy(3, Duration.ofSeconds(2), Duration.ofSeconds(1), 2))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new RetryPolicy(3, Duration.ZERO, Duration.ZERO, Double.NaN))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> policy.backoffBefore(0, new SplittableRandom()))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
