# Metrics

Exposed at `GET http://127.0.0.1:9090/metrics` (Prometheus text format 0.0.4), summarized in a log line every `metrics.logInterval`, and as a final accounting line at shutdown. `GET /health` returns 200 while the pipeline runs, 503 once it is stopping.

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `secstream_events_accepted_total` | counter | – | Events placed on the ingest queue. The pipeline owns these. |
| `secstream_events_rejected_total` | counter | `reason` = `queue_full` \| `too_large` \| `closed` | Refused at admission; never entered the pipeline. |
| `secstream_events_processed_total` | counter | `outcome` = `routed` \| `filtered` \| `duplicate` \| `unrouted` \| `malformed` \| `processing_error` \| `abandoned` | Exactly one per accepted event. |
| `secstream_ingest_queue_depth` / `_capacity` | gauge | – | Saturation = depth / capacity. |
| `secstream_dead_letters_total` | counter | `reason` (see `DeadLetterReason`) | Dead letters successfully stored. |
| `secstream_dead_letter_write_failures_total` | counter | – | Dead letters that could not be stored: **lost events**. Alert on > 0. |
| `secstream_processing_latency_microseconds` | summary | `quantile` | Worker CPU time per event (normalize→route), cumulative since start. |
| `secstream_sink_routed_total` | counter | `sink` | (event, sink) pairs handed to the sink queue. |
| `secstream_sink_delivered_total` | counter | `sink` | Successfully written. |
| `secstream_sink_dead_lettered_total` | counter | `sink` | Given up on (rejected, retries exhausted, shutdown). |
| `secstream_sink_write_attempts_total` | counter | `sink` | Batch writes, including retries. |
| `secstream_sink_write_failures_total` | counter | `sink` | Failed batch writes. |
| `secstream_sink_retries_total` | counter | `sink` | Retry attempts (each preceded by a backoff). |
| `secstream_sink_queue_depth` / `_capacity` | gauge | `sink` | Per-sink saturation; a full queue means this sink is the bottleneck. |
| `secstream_sink_delivery_latency_microseconds` | summary | `sink`, `quantile` | Admission → successful write, cumulative since start. Includes queueing. |

## Invariants (hold when idle; see ADR 0003)

```
accepted = Σ processed{outcome}
sink_routed{sink} = sink_delivered{sink} + sink_dead_lettered{sink}
```

## Limitations

- Counters are `LongAdder`s read independently; a scrape during load is not an atomic cut, so the invariants can be off by in-flight work.
- Quantiles are cumulative since process start (HdrHistogram, 3 significant digits, values clamped at 1 h). They are not windowed and cannot be aggregated across instances; a production system would export histograms instead.
- Label cardinality is bounded by configuration (sink names) and enums. No label ever contains event data.
