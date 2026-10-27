# SecStream architecture

## Data flow

```
                 submit() / trySubmit()            take()                       put()            take()+drainTo()
 FileSource ─┐   (blocks | bounded wait)   ┌──────────────────┐   ┌───────────┐  per target   ┌────────────┐   ┌──────────────┐
 Generator  ─┼──► admission (RW lock) ───► │ ingest queue     │──►│ N workers │ ─────────────►│ sink queue │──►│ dispatcher   │──► EventSink
 HttpSource ─┘   size check, sequence #    │ ArrayBlockingQ.  │   │ (fixed    │   (blocks)    │ (bounded)  │   │ 1 thread/sink│    write(batch)
                                           │ bounded          │   │  pool)    │               └────────────┘   │ batch, retry │
                                           └──────────────────┘   └─────┬─────┘                                └──────┬───────┘
                                                                        │ normalize → severity filter → dedup          │ permanent / exhausted /
                                                                        │ → enrich → route (fan-out)                   │ shutdown-abandoned
                                                                        ▼                                              ▼
                                                     malformed / processing error ───────────► DeadLetterChannel ──► FileDeadLetterStore (JSONL, 0600)
                                                                                                    │
                                                                  PipelineMetrics (LongAdder, HdrHistogram) ◄── every stage
                                                                        │
                                                          /metrics (Prometheus text), /health, periodic log line
```

## Packages (`com.mahijith.secstream`)

| Package | Responsibility | Key types |
|---|---|---|
| `model` | Immutable domain values and validation | `SecurityEvent` (record + builder), `RawEvent`, `EventId`, `IpAddress`, `Severity`, `EventFormat`, `InvalidEventException` |
| `processing` | Stateless/thread-safe per-event stages | `EventNormalizer` (+ `SignInNormalizer`, `FirewallNormalizer`), `SeverityFilter`, `Deduplicator`, `EventEnricher` (+ `NetworkZoneEnricher`), `EventProcessor`, `ProcessingResult` |
| `routing` | Rule-based fan-out | `Router`, `RoutingRule`, `EventMatcher` |
| `pipeline` | Admission, ingest queue, workers, shutdown | `Pipeline`, `EventIngress`, `Admission`, `PipelineSettings`, `ShutdownReport` |
| `delivery` | Per-sink delivery, retries, dead letters | `SinkDispatcher`, `EventSink`, `SinkException`, `RetryPolicy`, `Sleeper`, `DeadLetter*` |
| `sink` | Concrete sinks and output formats | `JsonLinesFileSink`, `ConsoleSink`, `PostgresSink`, `DiscardingSink`, `FileDeadLetterStore`, `EventJson` |
| `source` | Event sources | `EventSource`, `JsonLinesFileSource`, `SyntheticEventSource`, `HttpEventSource` |
| `metrics` | Counters, latency, exposition | `PipelineMetrics`, `SinkMetrics`, `MetricsSnapshot`, `LatencyRecorder`, `PrometheusFormat`, `MetricsHttpServer`, `MetricsReporter` |
| `config` | YAML model, validation, wiring | `SecStreamConfig`, `ConfigLoader`, `ConfigValidator`, `PipelineAssembler` |
| root | CLI lifecycle | `App` |

## Threads

| Thread(s) | Count | Blocks on | Owns |
|---|---|---|---|
| `source-<name>` | 1 per source | ingest admission (backpressure) | the source |
| `http-<name>-N` | configurable (default 4) | bounded admission (`trySubmit`) | HTTP exchanges |
| `worker-N` | `pipeline.workers` | ingest `take()`, sink queue `put()` | nothing shared mutable except via thread-safe collaborators |
| `sink-<name>` | 1 per sink | sink queue `take()`, sink I/O, backoff sleep | the sink instance (not thread-safe by contract) |
| `metrics-http`, `metrics-reporter` | 1 each | — | — |

## Concurrency invariants

1. **Bounded memory.** Every queue is bounded (ingest, each sink queue). Payload size, HTTP body size, events per request, line length, attributes per event and dedup window are all capped.
2. **Admission is atomic with respect to shutdown.** Submitters hold the read lock across "check closed + bounded offer"; shutdown takes the write lock to set `closed`. Once `closed` is set, no further event can enter the ingest queue, so end-of-stream markers enqueued afterwards are strictly last. A blocking submit never holds the lock for longer than one 50 ms offer, so shutdown cannot be stalled by a producer stuck on a full queue.
3. **Exactly one processing outcome per accepted event.** `accepted = routed + filtered + duplicates + unrouted + malformed + processingErrors + abandoned` once idle.
4. **Exactly one delivery outcome per (event, sink).** For every sink, `routed = delivered + deadLettered` once its queue is drained.
5. **Single writer per sink.** Only the sink's dispatcher thread calls `write`; `open` happens-before thread start; `close` runs on the same thread after the last write.
6. **Dedup check-and-insert is atomic per id** (same stripe, same lock).
7. **Shutdown order:** close admission → end-of-stream per worker → join workers → drain leftovers to DLQ (forced case) → end-of-stream per dispatcher → join dispatchers → close DLQ. Every blocking step has a deadline; on expiry work is interrupted and dead-lettered as `SHUTDOWN_ABANDONED`.

## Delivery guarantees

- **While the process runs:** every accepted event reaches a terminal outcome; every routed (event, sink) pair is delivered or dead-lettered. Nothing is dropped silently except when the dead-letter store itself fails, which is counted (`secstream_dead_letter_write_failures_total`) and logged.
- **Per sink: at-least-once.** A batch may be written again after an ambiguous failure. The PostgreSQL sink is idempotent (`ON CONFLICT (event_id) DO NOTHING`, one transaction per batch), so its rows are effectively-once per event id. File sinks can contain duplicate lines after a mid-batch I/O error.
- **Not exactly-once end to end**, and not durable: see "Crash behaviour".
- **Ordering:** none guaranteed across workers. Within one sink, events are written in the order workers enqueued them.
- **Partial delivery:** sinks fail independently. An event can be delivered to `archive` and dead-lettered for `alerts`; the dead letter names the sink so it can be replayed to that sink only.

## Crash behaviour (process killed, power loss)

Lost: everything in the ingest queue, on workers, and in sink queues, i.e. up to `queueCapacity + workers + Σ sink queueCapacity + Σ batchSize` events, plus file/DLQ data not yet flushed by the OS (unless `fsync: true`). HTTP clients that received 202 for those events are not told. Durable acceptance would need a write-ahead log (or Kafka / Event Hubs) written before answering 202, with offsets committed only after every target sink acknowledged; see `docs/INTERVIEW_GUIDE.md` §14.

## Decisions

See `adr/`. Summary: Maven + Java 21 (0001); per-sink dispatcher threads (0002); accounting model (0003); admission lock + shutdown protocol (0004); bounded striped dedup (0005); stage order, fan-out router, patterns (0006); strict YAML + env-var secrets (0007).
