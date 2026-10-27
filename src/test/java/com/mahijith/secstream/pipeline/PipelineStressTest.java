package com.mahijith.secstream.pipeline;

import static com.mahijith.secstream.pipeline.PipelineHarness.retries;
import static org.assertj.core.api.Assertions.assertThat;

import com.mahijith.secstream.metrics.MetricsSnapshot;
import com.mahijith.secstream.model.Severity;
import com.mahijith.secstream.processing.NetworkZoneEnricher;
import com.mahijith.secstream.routing.EventMatcher;
import com.mahijith.secstream.routing.RoutingRule;
import com.mahijith.secstream.source.GeneratorSettings;
import com.mahijith.secstream.source.SyntheticEventGenerator;
import com.mahijith.secstream.testing.FlakySink;
import com.mahijith.secstream.testing.RecordingSink;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * High-volume correctness under contention: many producers, 8 workers, a 30%-failing sink and a
 * healthy sink, small queues so backpressure is constantly engaged. Run with {@code
 * -Dsecstream.stress.events=1000000} for the 1M-event version.
 */
@Timeout(300)
class PipelineStressTest {
  private static final int EVENTS = Integer.getInteger("secstream.stress.events", 200_000);
  private static final int PRODUCERS = 4;

  @Test
  void accountingHoldsUnderConcurrentLoadWithFailingSink() throws Exception {
    // attempt % 10 < 3 fails exactly 30% of write attempts; never more than 3 in a row, so with 5
    // attempts every batch eventually succeeds. Deterministic: no reliance on random chance.
    FlakySink flaky = new FlakySink("flaky", attempt -> attempt % 10 < 3, true);
    RecordingSink healthy = new RecordingSink("healthy");
    List<RoutingRule> rules =
        List.of(
            new RoutingRule("all", EventMatcher.ANY, List.of("healthy")),
            new RoutingRule(
                "medium-up",
                new EventMatcher(Severity.MEDIUM, Set.of(), Set.of()),
                List.of("flaky")));
    PipelineHarness h =
        new PipelineHarness(
            8, 1_024, 256, retries(5), List.of(new NetworkZoneEnricher()), rules, flaky, healthy);
    h.pipeline.start();

    // Each producer gets its own deterministic stream: duplicates within a stream exercise dedup,
    // malformed payloads exercise the dead-letter path.
    int perProducer = EVENTS / PRODUCERS;
    List<List<SyntheticEventGenerator.Generated>> streams = new ArrayList<>();
    for (int p = 0; p < PRODUCERS; p++) {
      SyntheticEventGenerator g =
          new SyntheticEventGenerator(
              new GeneratorSettings(perProducer, 0, 0.5, 0.3, 0.05, 0.01, 1_000 + p),
              Instant.now());
      List<SyntheticEventGenerator.Generated> events = new ArrayList<>(perProducer);
      for (int i = 0; i < perProducer; i++) {
        events.add(g.next());
      }
      streams.add(events);
    }

    CyclicBarrier start = new CyclicBarrier(PRODUCERS);
    ExecutorService producers = Executors.newFixedThreadPool(PRODUCERS);
    List<Future<Long>> results = new ArrayList<>();
    for (List<SyntheticEventGenerator.Generated> stream : streams) {
      results.add(
          producers.submit(
              () -> {
                start.await();
                long accepted = 0;
                for (SyntheticEventGenerator.Generated e : stream) {
                  if (h.pipeline.submit("stress", e.format(), e.payload()) == Admission.ACCEPTED) {
                    accepted++;
                  }
                }
                return accepted;
              }));
    }
    long acceptedByProducers = 0;
    for (Future<Long> f : results) {
      acceptedByProducers += f.get(240, TimeUnit.SECONDS);
    }
    producers.shutdown();
    ShutdownReport report = h.pipeline.shutdown(Duration.ofSeconds(60));

    MetricsSnapshot s = h.snapshot();
    assertThat(report.graceful()).isTrue();
    assertThat(acceptedByProducers).isEqualTo((long) perProducer * PRODUCERS);
    assertThat(s.accepted()).isEqualTo(acceptedByProducers);
    assertThat(s.rejected()).isZero();

    // Invariant 2: every accepted event has exactly one processing outcome.
    assertThat(s.accepted())
        .isEqualTo(
            s.routed()
                + s.filtered()
                + s.duplicates()
                + s.unrouted()
                + s.malformed()
                + s.processingErrors()
                + s.abandoned());
    assertThat(s.abandoned()).isZero();
    assertThat(s.processingErrors()).isZero();
    assertThat(s.duplicates()).isPositive();
    assertThat(s.malformed()).isPositive();
    assertThat(s.deadLetters().values().stream().mapToLong(Long::longValue).sum())
        .isEqualTo(s.malformed());

    // Invariant 3: per sink, routed = delivered + dead-lettered; retries cured every failure.
    assertThat(s.deliveriesBalanced()).isTrue();
    var flakyStats = s.sinks().get("flaky");
    var healthyStats = s.sinks().get("healthy");
    assertThat(flakyStats.deadLettered()).isZero();
    assertThat(flakyStats.retries()).isPositive();
    assertThat(healthyStats.delivered()).isEqualTo(s.routed());

    // No loss and no duplicates inside the sinks: every routed event written exactly once.
    assertThat(healthy.written()).hasSize((int) s.routed());
    assertThat(healthy.distinctIds()).isEqualTo((int) s.routed());
    assertThat(flaky.distinctIds()).isEqualTo(flaky.written().size());
    assertThat(flaky.written()).hasSize((int) flakyStats.delivered());

    Set<String> healthyIds = new HashSet<>();
    healthy.written().forEach(e -> healthyIds.add(e.id().value()));
    assertThat(flaky.written()).allMatch(e -> healthyIds.contains(e.id().value()));
  }
}
