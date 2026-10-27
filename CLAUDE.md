# CLAUDE.md — SecStream

Entry point for any Claude Code session. Detailed status: `docs/PROJECT_STATUS.md`. Design: `docs/architecture/ARCHITECTURE.md` and `docs/architecture/adr/`.

## Purpose
Multithreaded security-event ingestion pipeline in Java 21, built as an interview-grade portfolio project (Microsoft-style backend interviews). Goal is defensible engineering: explicit guarantees, tested concurrency, measured performance. The owner must be able to explain every line — prefer simple, explicit code over clever code.

## Build & tooling
- Java 21 (`release 21`, enforcer `[21,22)`), Maven 3.9+. No wrapper yet.
- `mvn verify` — compile (`-Xlint:all -Werror`), unit tests (Surefire), integration tests `*IT` (Failsafe, Testcontainers; skipped without Docker), Spotless check, SpotBugs, enforcer. This is the CI gate (`.github/workflows/ci.yml`).
- `mvn spotless:apply` — format (google-java-format **1.36.0**; 1.37.0 is broken under Spotless 3.10.4).
- `mvn -Dskip.quality=true test` — fast inner loop (skips Spotless/SpotBugs only).
- `mvn test -Dtest=PipelineStressTest -Dsecstream.stress.events=1000000` — 1M-event correctness run.
- `mvn -Pbench -Dskip.quality=true test-compile exec:java -Dbench.sink=discard|file` — benchmark; writes `target/bench-<sink>.md`. Never put perf thresholds in CI.
- `mvn package` → `target/secstream-0.1.0-SNAPSHOT-all.jar`; run `java -jar target/secstream-0.1.0-SNAPSHOT-all.jar --config config/pipeline.yaml` (add `--validate` to only check config).

## Layout (`src/main/java/com/mahijith/secstream`)
`model` (immutable records, validation) · `processing` (normalizers, severity filter, dedup, enrichers, `EventProcessor`) · `routing` (fan-out `Router`) · `pipeline` (`Pipeline`: admission, ingest queue, workers, shutdown) · `delivery` (`SinkDispatcher`, `EventSink`, retries, dead letters) · `sink` (file, console, postgres, discard, DLQ file) · `source` (file, generator, HTTP) · `metrics` (LongAdder counters, HdrHistogram, Prometheus endpoint) · `config` (YAML model, validator, `PipelineAssembler`) · `App` (CLI lifecycle).

## Invariants you must not break
1. All queues bounded; all untrusted input size-limited.
2. Admission: read lock around check-closed + one bounded offer; shutdown takes write lock. Never hold the read lock across an unbounded `put`.
3. `accepted = routed + filtered + duplicates + unrouted + malformed + processingErrors + abandoned` (when idle).
4. Per sink: `routed = delivered + deadLettered` (when idle). Count `routed` only after a successful hand-off.
5. Exactly one dispatcher thread writes to a sink; sinks are not thread-safe.
6. Forced shutdown dead-letters unfinished work as `SHUTDOWN_ABANDONED`; nothing disappears silently (except DLQ store failures, which are counted).
7. Never claim exactly-once or durability: at-least-once per sink while running; in-memory queues are lost on crash.
Tests that guard these: `PipelineTest`, `PipelineStressTest`, `SinkDispatcherTest`, `DeduplicatorTest`.

## Config & secrets
- YAML (`config/pipeline.yaml` demo, `config/pipeline-full.yaml` HTTP+Postgres). Unknown keys fail. All problems reported at once; exit code 2.
- Secrets only via env vars named in config: `SECSTREAM_PG_PASSWORD`, `SECSTREAM_HTTP_TOKEN` (see `.env.example`). Never commit real values; never log tokens, passwords, payloads.
- `SECSTREAM_LOG_LEVEL` overrides root log level. Logs → stderr (logfmt); console sink → stdout.
- Local DB: `SECSTREAM_PG_PASSWORD=... docker compose up -d` (PostgreSQL 18, bound to 127.0.0.1).

## Rules for future sessions
- Start: read this file, `docs/PROJECT_STATUS.md`, relevant ADRs; run `git status` and `mvn -q -Dskip.quality=true test`; verify docs match code.
- Work milestone by milestone; add tests with every change; run `mvn verify` before claiming done.
- Record exact commands/results in `docs/PROJECT_STATUS.md`; add an ADR for any architectural change.
- Do not commit/push without the user's explicit OK. Do not introduce Go or frameworks (Spring etc.) without an ADR.
- Benchmarks: report measured numbers only, with environment and variance.
- Use synthetic data only (RFC 5737 / RFC 1918 addresses).

## Current state (2026-10-27)
Milestones 0–8 implemented and verified; milestone 9 docs written. Nothing committed yet (repo initialised, no commits). Next concrete task: see "Next steps" in `docs/PROJECT_STATUS.md`.
