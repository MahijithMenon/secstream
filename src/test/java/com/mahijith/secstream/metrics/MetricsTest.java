package com.mahijith.secstream.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import com.mahijith.secstream.delivery.DeadLetterReason;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import org.junit.jupiter.api.Test;

class MetricsTest {

  @Test
  void prometheusOutputContainsCountersSinksAndLatency() {
    PipelineMetrics m = new PipelineMetrics();
    m.accepted();
    m.accepted();
    m.routed();
    m.filtered();
    m.rejectedQueueFull();
    m.deadLetterWritten(DeadLetterReason.MALFORMED);
    m.processingNanos(1_500_000);
    SinkMetrics s = m.sink("db\"x");
    s.routed();
    s.delivered(System.nanoTime() - 2_000_000, System.nanoTime());

    String text = PrometheusFormat.render(m.snapshot());
    assertThat(text)
        .contains("secstream_events_accepted_total 2")
        .contains("secstream_events_rejected_total{reason=\"queue_full\"} 1")
        .contains("secstream_events_processed_total{outcome=\"routed\"} 1")
        .contains("secstream_dead_letters_total{reason=\"malformed\"} 1")
        .contains("secstream_sink_delivered_total{sink=\"db\\\"x\"} 1")
        .contains("secstream_processing_latency_microseconds_count 1")
        .contains("# TYPE secstream_sink_delivery_latency_microseconds summary");
  }

  @Test
  void snapshotInvariantsDetectImbalance() {
    PipelineMetrics m = new PipelineMetrics();
    m.accepted();
    assertThat(m.snapshot().processingBalanced()).isFalse();
    assertThat(m.snapshot().inFlight()).isEqualTo(1);
    m.duplicate();
    assertThat(m.snapshot().processingBalanced()).isTrue();
  }

  @Test
  void latencyPercentilesAreReasonable() {
    LatencyRecorder r = new LatencyRecorder();
    for (int i = 1; i <= 1_000; i++) {
      r.recordNanos(i * 1_000L); // 1..1000 µs
    }
    var s = r.snapshot();
    assertThat(s.count()).isEqualTo(1_000);
    assertThat(s.p50Micros()).isBetween(495L, 505L);
    assertThat(s.p99Micros()).isBetween(985L, 995L);
    r.recordNanos(-5); // clock weirdness must not throw
    assertThat(r.snapshot().count()).isEqualTo(1_001);
  }

  @Test
  void httpEndpointServesMetricsAndHealth() throws Exception {
    PipelineMetrics m = new PipelineMetrics();
    try (MetricsHttpServer server = new MetricsHttpServer("127.0.0.1", 0, m, () -> true)) {
      server.start();
      HttpClient client = HttpClient.newHttpClient();
      var metrics =
          client.send(
              HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + server.port() + "/metrics"))
                  .build(),
              HttpResponse.BodyHandlers.ofString());
      assertThat(metrics.statusCode()).isEqualTo(200);
      assertThat(metrics.body()).contains("secstream_events_accepted_total 0");
      var health =
          client.send(
              HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + server.port() + "/health"))
                  .build(),
              HttpResponse.BodyHandlers.ofString());
      assertThat(health.body()).isEqualTo("ok\n");
    }
  }
}
