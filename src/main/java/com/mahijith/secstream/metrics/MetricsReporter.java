package com.mahijith.secstream.metrics;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Logs a one-line metrics summary at a fixed interval. */
public final class MetricsReporter implements AutoCloseable {
  private static final Logger log = LoggerFactory.getLogger(MetricsReporter.class);

  private final PipelineMetrics metrics;
  private final ScheduledExecutorService scheduler =
      Executors.newSingleThreadScheduledExecutor(
          Thread.ofPlatform().name("metrics-reporter").daemon().factory());

  public MetricsReporter(PipelineMetrics metrics) {
    this.metrics = metrics;
  }

  public void start(Duration interval) {
    long millis = interval.toMillis();
    scheduler.scheduleAtFixedRate(this::report, millis, millis, TimeUnit.MILLISECONDS);
  }

  public void report() {
    MetricsSnapshot s = metrics.snapshot();
    var event =
        log.atInfo()
            .setMessage("metrics")
            .addKeyValue("accepted", s.accepted())
            .addKeyValue("rejected", s.rejected())
            .addKeyValue("routed", s.routed())
            .addKeyValue("filtered", s.filtered())
            .addKeyValue("duplicates", s.duplicates())
            .addKeyValue("malformed", s.malformed())
            .addKeyValue("deadLetters", s.deadLettersTotal())
            .addKeyValue("inFlight", s.inFlight())
            .addKeyValue("queueDepth", s.queueDepth())
            .addKeyValue("processingP95us", s.processingLatency().p95Micros());
    for (SinkMetrics.SinkSnapshot k : s.sinks().values()) {
      event =
          event
              .addKeyValue("sink." + k.name() + ".delivered", k.delivered())
              .addKeyValue("sink." + k.name() + ".deadLettered", k.deadLettered())
              .addKeyValue("sink." + k.name() + ".p95us", k.deliveryLatency().p95Micros());
    }
    event.log();
  }

  @Override
  public void close() {
    scheduler.shutdownNow();
  }
}
