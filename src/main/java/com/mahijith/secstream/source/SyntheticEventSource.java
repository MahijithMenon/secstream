package com.mahijith.secstream.source;

import com.mahijith.secstream.pipeline.Admission;
import com.mahijith.secstream.pipeline.EventIngress;
import java.time.Clock;
import java.util.concurrent.locks.LockSupport;

/** Submits generated events, optionally paced to a target rate. */
public final class SyntheticEventSource implements EventSource {
  private final String name;
  private final GeneratorSettings settings;
  private final Clock clock;
  private volatile boolean stopped;

  public SyntheticEventSource(String name, GeneratorSettings settings, Clock clock) {
    this.name = name;
    this.settings = settings;
    this.clock = clock;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public boolean finite() {
    return true;
  }

  @Override
  public void run(EventIngress ingress) throws InterruptedException {
    SyntheticEventGenerator generator = new SyntheticEventGenerator(settings, clock.instant());
    long intervalNanos =
        settings.ratePerSecond() == 0 ? 0 : 1_000_000_000L / settings.ratePerSecond();
    long start = System.nanoTime();
    for (long i = 0; i < settings.count() && !stopped; i++) {
      if (intervalNanos > 0) {
        // Pace against the schedule, not the previous send, so pauses don't accumulate drift.
        long wait = start + i * intervalNanos - System.nanoTime();
        if (wait > 0) {
          LockSupport.parkNanos(wait);
        }
        if (Thread.interrupted()) {
          throw new InterruptedException();
        }
      }
      SyntheticEventGenerator.Generated g = generator.next();
      if (ingress.submit(name, g.format(), g.payload()) == Admission.REJECTED_CLOSED) {
        return;
      }
    }
  }

  @Override
  public void stop() {
    stopped = true;
  }
}
