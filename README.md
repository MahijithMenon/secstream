# SecStream

A multithreaded security-event ingestion pipeline in Java 21: sign-in and firewall logs come in from files, a synthetic generator or HTTP; are parsed, validated, normalized into one immutable `SecurityEvent` model, filtered, deduplicated, enriched and routed to console, JSON Lines files or PostgreSQL — with bounded queues, backpressure, retries, dead-lettering, graceful shutdown, metrics, and event accounting that is verified under concurrent load.

> Status: functional and tested; single-process, in-memory (not durable). See [Limitations](#limitations).

```
FileSource ─┐                     ┌─────────────┐   ┌───────────┐   ┌──────────┐   ┌────────────┐
Generator  ─┼─► admission ──────► │ ingest queue│──►│ N workers │──►│sink queue│──►│ dispatcher │──► sink
HttpSource ─┘  (block / 503)      │  (bounded)  │   │ normalize │   │(bounded) │   │ batch+retry│
                                  └─────────────┘   │ filter    │   └──────────┘   └─────┬──────┘
                                                    │ dedup     │  one queue + thread    │
                                                    │ enrich    │  per sink              ▼
                                                    │ route     │──────────────► dead-letter file
                                                    └───────────┘
```

Details: [architecture](docs/architecture/ARCHITECTURE.md) · [ADRs](docs/architecture/adr/) · [metrics](docs/METRICS.md) · [benchmarks](docs/BENCHMARKS.md) · [interview guide](docs/INTERVIEW_GUIDE.md) · [status](docs/PROJECT_STATUS.md)

## Prerequisites

- JDK 21, Maven 3.9+
- Docker (only for PostgreSQL and the integration tests; everything else runs without it)

## Quick start

```bash
mvn -q package -DskipTests
```

```bash
java -jar target/secstream-0.1.0-SNAPSHOT-all.jar --config config/pipeline.yaml
```

This reads `samples/signin.jsonl` and `samples/firewall.jsonl`, prints alerts (failed sign-ins, blocked admin-port traffic) as JSON lines to stdout, archives every valid event to `out/archive.jsonl`, writes malformed lines to `out/dead-letter.jsonl`, and exits after a final accounting log line. Logs go to stderr.

Validate a config without running it:

```bash
java -jar target/secstream-0.1.0-SNAPSHOT-all.jar --config config/pipeline.yaml --validate
```

## Configuration

YAML, strict (unknown keys are errors; all problems are reported at once, exit code 2). Abbreviated:

```yaml
pipeline:   {workers: 4, queueCapacity: 10000, maxPayloadBytes: 65536, shutdownTimeout: 30s}
processing: {minSeverity: INFO, dedupCapacity: 100000, enrichers: [network-zone]}
sources:
  - {name: signin-file, type: file, path: samples/signin.jsonl, format: signin}
  - {name: gen, type: generator, count: 50000, ratePerSecond: 2000, seed: 7}
  - {name: http, type: http, host: 127.0.0.1, port: 8080, authTokenEnv: SECSTREAM_HTTP_TOKEN}
sinks:
  - {name: alerts, type: console}
  - name: db
    type: postgres
    url: jdbc:postgresql://localhost:5432/secstream
    user: secstream
    passwordEnv: SECSTREAM_PG_PASSWORD       # the env var's NAME; never the secret
    delivery: {queueCapacity: 20000, batchSize: 500,
               retry: {maxAttempts: 8, initialBackoff: 200ms, maxBackoff: 10s, multiplier: 2.0}}
routes:
  - {name: failed-signins, minSeverity: MEDIUM, categories: [AUTHENTICATION], outcomes: [FAILURE], sinks: [alerts]}
  - {name: store-all, sinks: [db]}
deadLetter: {path: out/dead-letter.jsonl, fsync: true}
metrics:    {enabled: true, host: 127.0.0.1, port: 9090, logInterval: 10s}
```

Full examples: [`config/pipeline.yaml`](config/pipeline.yaml), [`config/pipeline-full.yaml`](config/pipeline-full.yaml). Environment variables: `SECSTREAM_PG_PASSWORD`, `SECSTREAM_HTTP_TOKEN` (optional), `SECSTREAM_LOG_LEVEL` (optional). See [`.env.example`](.env.example).

### Event formats

Sign-in (`format: signin`): `{"eventId"?, "timestamp": ISO-8601, "userId", "ip", "country"?, "result": "SUCCESS|FAILURE", "app", "deviceId"?, "failureReason"?}`

Firewall (`format: firewall`): `{"id"?, "ts": epoch-millis, "src_ip", "dst_ip", "dst_port", "proto": "TCP|UDP|ICMP", "action": "ALLOW|DENY", "rule_id"?, "bytes"?}`

Both normalize to `SecurityEvent` (category, outcome, severity, user, IPs, port, bounded attribute map). Type mismatches, unknown enum values, non-literal IPs (no DNS lookups), control characters, oversized fields and timestamps more than 5 minutes in the future are rejected and dead-lettered.

## PostgreSQL

```bash
export SECSTREAM_PG_PASSWORD=change-me-local-only
```

```bash
docker compose up -d
```

```bash
java -jar target/secstream-0.1.0-SNAPSHOT-all.jar --config config/pipeline-full.yaml
```

The sink applies Flyway migrations on startup (`src/main/resources/db/migration`), writes one transaction per batch with JDBC batching, and is idempotent on `event_id`.

## HTTP ingestion

With `config/pipeline-full.yaml` running:

```bash
curl -i -X POST http://127.0.0.1:8080/v1/events/signin -H 'Content-Type: application/x-ndjson' --data-binary @samples/signin.jsonl
```

| Status | Meaning |
|---|---|
| 202 | All events accepted (validation happens asynchronously; invalid ones are dead-lettered). Body: `{"accepted":n,"rejectedTooLarge":0,"notAttempted":0}` |
| 503 + `Retry-After` | Queue full or shutting down. `accepted` = the prefix that was taken; resend the rest. |
| 400 / 401 / 404 / 405 / 413 / 415 | Bad body or UTF-8 / missing bearer token / unknown format / not POST / body or batch too large / wrong content type. Nothing accepted. |

Limits: `maxBodyBytes` (default 1 MiB), `maxEventsPerRequest` (1000), fixed handler pool, JDK server request/response timeouts of 30 s. Binds to `127.0.0.1` by default; optional bearer token compared in constant time. No TLS.

## Metrics and logs

```bash
curl -s http://127.0.0.1:9090/metrics
```

Prometheus text: accepted/rejected/processed-by-outcome counters, ingest and per-sink queue depth, per-sink delivered/dead-lettered/attempts/failures/retries, processing and delivery latency quantiles, dead letters by reason. `/health` returns 200 / 503. A logfmt summary line is logged every `logInterval`. Definitions and invariants: [docs/METRICS.md](docs/METRICS.md).

## Failure handling and delivery guarantees

| Situation | Behaviour |
|---|---|
| Queue full | File/generator sources block; HTTP answers 503 after `admissionTimeout`. |
| Malformed event | Dead-lettered (`MALFORMED`) with raw payload (truncated at 16 KiB). Never retried. |
| Stage throws (bug) | Dead-lettered (`PROCESSING_ERROR`); worker continues. |
| Sink transient failure | Retried with exponential backoff + jitter; then `RETRIES_EXHAUSTED`. |
| Sink permanent failure | Batch split; only the offending event is dead-lettered (`DELIVERY_REJECTED`). |
| One of several sinks fails | Others unaffected; dead letter names the failed sink. |
| Sink down for long | Its queue fills → backpressure up to sources. Nothing is buffered without bound. |
| SIGTERM | Graceful drain within `shutdownTimeout`; remaining work dead-lettered as `SHUTDOWN_ABANDONED`. |
| Process crash | Queued and in-flight events are lost (in-memory). |

Guarantee: **at-least-once per sink while the process runs**; PostgreSQL rows are effectively-once per event id. Not exactly-once; not durable. Accounting invariants (`accepted = Σ processing outcomes`, per sink `routed = delivered + dead-lettered`) are asserted by the tests. See [ADR 0003](docs/architecture/adr/0003-event-accounting.md).

## Testing

```bash
mvn verify
```

Runs 125 unit/concurrency tests, 4 PostgreSQL integration tests (Testcontainers; skipped when Docker is unavailable), Spotless and SpotBugs. Highlights: barrier-released concurrent dedup; queue saturation and blocked producers; graceful and forced shutdown with exact accounting; deterministic 30%-failing sink under 8 workers × 4 producers × 200k events; HTTP limits and auth; SQL-injection literal storage; config error reporting; CLI end-to-end. One-million-event run:

```bash
mvn test -Dtest=PipelineStressTest -Dsecstream.stress.events=1000000
```

## Benchmarks

Measured on an Apple M3 (8 cores), JDK 21.0.10; methodology, raw numbers and analysis in [docs/BENCHMARKS.md](docs/BENCHMARKS.md). Results are noisy on a laptop; read the min/max columns.

## Limitations

- **Not durable:** in-memory queues; crash loses accepted events. No WAL, no broker.
- **Single process:** no partitioning, clustering or Kafka/Event Hubs integration.
- **Security:** no TLS, no authorization beyond an optional shared bearer token, no encryption at rest, no retention policy for output/DLQ files (they are created `0600`). Not production-secure.
- Dedup window is in-memory and bounded; duplicates after eviction or restart pass through (PostgreSQL PK catches them).
- One writer thread per sink: a single sink's throughput is bounded by one thread (visible in the file-sink benchmark).
- Latency quantiles are cumulative since start, not windowed.
