package com.mahijith.secstream.delivery;

import java.time.Duration;

/** Abstraction over {@link Thread#sleep} so retry tests run instantly and deterministically. */
@FunctionalInterface
public interface Sleeper {
  Sleeper SYSTEM = duration -> Thread.sleep(duration);

  void sleep(Duration duration) throws InterruptedException;
}
