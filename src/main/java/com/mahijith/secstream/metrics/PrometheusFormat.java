package com.mahijith.secstream.metrics;

import com.mahijith.secstream.delivery.DeadLetterReason;
import java.util.Locale;

/**
 * Renders a snapshot in the Prometheus text exposition format (version 0.0.4). Label values are
 * limited to sink names from configuration and fixed enums, so cardinality is bounded by config,
 * never by event content.
 */
public final class PrometheusFormat {
  private PrometheusFormat() {}

  public static String render(MetricsSnapshot s) {
    StringBuilder b = new StringBuilder(4096);
    counter(
        b,
        "secstream_events_accepted_total",
        "Events accepted into the ingest queue.",
        s.accepted());

    header(b, "secstream_events_rejected_total", "counter", "Events refused at admission.");
    sample(b, "secstream_events_rejected_total", "reason=\"queue_full\"", s.rejectedQueueFull());
    sample(b, "secstream_events_rejected_total", "reason=\"too_large\"", s.rejectedTooLarge());
    sample(b, "secstream_events_rejected_total", "reason=\"closed\"", s.rejectedClosed());

    header(
        b, "secstream_events_processed_total", "counter", "Accepted events by processing outcome.");
    sample(b, "secstream_events_processed_total", "outcome=\"routed\"", s.routed());
    sample(b, "secstream_events_processed_total", "outcome=\"filtered\"", s.filtered());
    sample(b, "secstream_events_processed_total", "outcome=\"duplicate\"", s.duplicates());
    sample(b, "secstream_events_processed_total", "outcome=\"unrouted\"", s.unrouted());
    sample(b, "secstream_events_processed_total", "outcome=\"malformed\"", s.malformed());
    sample(
        b,
        "secstream_events_processed_total",
        "outcome=\"processing_error\"",
        s.processingErrors());
    sample(b, "secstream_events_processed_total", "outcome=\"abandoned\"", s.abandoned());

    gauge(b, "secstream_ingest_queue_depth", "Events waiting in the ingest queue.", s.queueDepth());
    gauge(b, "secstream_ingest_queue_capacity", "Ingest queue bound.", s.queueCapacity());

    header(b, "secstream_dead_letters_total", "counter", "Dead letters written, by reason.");
    for (DeadLetterReason r : DeadLetterReason.values()) {
      sample(
          b,
          "secstream_dead_letters_total",
          "reason=\"" + r.name().toLowerCase(Locale.ROOT) + "\"",
          s.deadLetters().getOrDefault(r, 0L));
    }
    counter(
        b,
        "secstream_dead_letter_write_failures_total",
        "Dead letters that could not be stored (lost).",
        s.deadLetterWriteFailures());

    summary(
        b,
        "secstream_processing_latency_microseconds",
        "Worker time per event.",
        "",
        s.processingLatency());

    header(b, "secstream_sink_routed_total", "counter", "Deliveries routed to the sink.");
    s.sinks().values().forEach(k -> sample(b, "secstream_sink_routed_total", sink(k), k.routed()));
    header(b, "secstream_sink_delivered_total", "counter", "Events written successfully.");
    s.sinks()
        .values()
        .forEach(k -> sample(b, "secstream_sink_delivered_total", sink(k), k.delivered()));
    header(b, "secstream_sink_dead_lettered_total", "counter", "Deliveries given up on.");
    s.sinks()
        .values()
        .forEach(k -> sample(b, "secstream_sink_dead_lettered_total", sink(k), k.deadLettered()));
    header(
        b,
        "secstream_sink_write_attempts_total",
        "counter",
        "Batch writes attempted, including retries.");
    s.sinks()
        .values()
        .forEach(k -> sample(b, "secstream_sink_write_attempts_total", sink(k), k.writeAttempts()));
    header(b, "secstream_sink_write_failures_total", "counter", "Batch writes that failed.");
    s.sinks()
        .values()
        .forEach(k -> sample(b, "secstream_sink_write_failures_total", sink(k), k.writeFailures()));
    header(b, "secstream_sink_retries_total", "counter", "Batch write retries.");
    s.sinks()
        .values()
        .forEach(k -> sample(b, "secstream_sink_retries_total", sink(k), k.retries()));
    header(b, "secstream_sink_queue_depth", "gauge", "Deliveries waiting for the sink.");
    s.sinks()
        .values()
        .forEach(k -> sample(b, "secstream_sink_queue_depth", sink(k), k.queueDepth()));
    header(b, "secstream_sink_queue_capacity", "gauge", "Sink queue bound.");
    s.sinks()
        .values()
        .forEach(k -> sample(b, "secstream_sink_queue_capacity", sink(k), k.queueCapacity()));
    header(
        b,
        "secstream_sink_delivery_latency_microseconds",
        "summary",
        "Admission to successful sink write.");
    s.sinks()
        .values()
        .forEach(
            k ->
                summarySamples(
                    b,
                    "secstream_sink_delivery_latency_microseconds",
                    sink(k) + ",",
                    k.deliveryLatency()));
    return b.toString();
  }

  private static String sink(SinkMetrics.SinkSnapshot s) {
    return "sink=\"" + escape(s.name()) + "\"";
  }

  static String escape(String labelValue) {
    return labelValue.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n");
  }

  private static void counter(StringBuilder b, String name, String help, long value) {
    header(b, name, "counter", help);
    sample(b, name, "", value);
  }

  private static void gauge(StringBuilder b, String name, String help, long value) {
    header(b, name, "gauge", help);
    sample(b, name, "", value);
  }

  private static void summary(
      StringBuilder b, String name, String help, String labels, LatencyRecorder.LatencySnapshot l) {
    header(b, name, "summary", help);
    summarySamples(b, name, labels, l);
  }

  private static void summarySamples(
      StringBuilder b, String name, String labelPrefix, LatencyRecorder.LatencySnapshot l) {
    sample(b, name, labelPrefix + "quantile=\"0.5\"", l.p50Micros());
    sample(b, name, labelPrefix + "quantile=\"0.95\"", l.p95Micros());
    sample(b, name, labelPrefix + "quantile=\"0.99\"", l.p99Micros());
    String bare =
        labelPrefix.endsWith(",")
            ? labelPrefix.substring(0, labelPrefix.length() - 1)
            : labelPrefix;
    sample(b, name + "_count", bare, l.count());
  }

  private static void header(StringBuilder b, String name, String type, String help) {
    b.append("# HELP ").append(name).append(' ').append(help).append('\n');
    b.append("# TYPE ").append(name).append(' ').append(type).append('\n');
  }

  private static void sample(StringBuilder b, String name, String labels, long value) {
    b.append(name);
    if (!labels.isEmpty()) {
      b.append('{').append(labels).append('}');
    }
    b.append(' ').append(value).append('\n');
  }
}
