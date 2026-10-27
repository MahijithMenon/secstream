package com.mahijith.secstream.pipeline;

import com.mahijith.secstream.delivery.DeadLetterChannel;
import com.mahijith.secstream.delivery.EventSink;
import com.mahijith.secstream.delivery.InMemoryDeadLetterStore;
import com.mahijith.secstream.delivery.RetryPolicy;
import com.mahijith.secstream.delivery.SinkDispatcher;
import com.mahijith.secstream.metrics.MetricsSnapshot;
import com.mahijith.secstream.metrics.PipelineMetrics;
import com.mahijith.secstream.model.Severity;
import com.mahijith.secstream.processing.Deduplicator;
import com.mahijith.secstream.processing.EventEnricher;
import com.mahijith.secstream.processing.EventProcessor;
import com.mahijith.secstream.processing.SeverityFilter;
import com.mahijith.secstream.routing.Router;
import com.mahijith.secstream.routing.RoutingRule;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;

/** Builds an in-memory pipeline with instant retries for tests. */
final class PipelineHarness {
  final PipelineMetrics metrics = new PipelineMetrics();
  final InMemoryDeadLetterStore deadLetters = new InMemoryDeadLetterStore();
  final Pipeline pipeline;

  PipelineHarness(
      int workers,
      int queueCapacity,
      int sinkQueueCapacity,
      RetryPolicy retry,
      List<EventEnricher> enrichers,
      List<RoutingRule> rules,
      EventSink... sinks) {
    DeadLetterChannel channel = new DeadLetterChannel(deadLetters, metrics);
    List<SinkDispatcher> dispatchers = new ArrayList<>();
    for (EventSink sink : sinks) {
      dispatchers.add(
          new SinkDispatcher(
              sink,
              sinkQueueCapacity,
              100,
              retry,
              channel,
              metrics.sink(sink.name()),
              duration -> {},
              new SplittableRandom(1),
              Clock.systemUTC()));
    }
    EventProcessor processor =
        new EventProcessor(
            EventProcessor.defaultNormalizers(),
            new SeverityFilter(Severity.INFO),
            new Deduplicator(1_000_000),
            enrichers,
            new Router(rules));
    pipeline =
        new Pipeline(
            new PipelineSettings(workers, queueCapacity, 4096),
            processor,
            dispatchers,
            channel,
            metrics,
            Clock.systemUTC());
  }

  static RetryPolicy retries(int attempts) {
    return new RetryPolicy(attempts, Duration.ZERO, Duration.ZERO, 1.0);
  }

  MetricsSnapshot snapshot() {
    return metrics.snapshot();
  }
}
