# Project status

Last updated: 2026-10-27 (session 1).

## Initial assessment (session 1)

- Local directory contained only `.claude/settings.local.json`; not a git repository. `git ls-remote https://github.com/MahijithMenon/secstream` returned no refs: the GitHub repository exists but is empty. Nothing to preserve or migrate.
- Tooling: OpenJDK 21.0.10 (Homebrew), Maven 3.9.14, Docker 28.3.3 (daemon started during the session), git 2.53.0. No Gradle → Maven chosen (ADR 0001).
- Input: build guide PDF (package `com.mahijith.secstream`, five-stage pipeline, sign-in + firewall formats, YAML config, Postgres, retries/DLQ, metrics, 1/2/4/8-worker benchmark).
- Design changes vs the guide, each with an ADR: per-sink dispatcher threads (0002); three-boundary accounting instead of `ingested = routed + dropped + dead-lettered` (0003); RW-lock admission + deadline-bounded shutdown (0004); bounded striped dedup instead of a bare `ConcurrentHashMap` (0005); merged parse/normalize stage and fan-out router instead of Chain of Responsibility, no Observer (0006); strict YAML + env-var secrets (0007).

## Milestones

| # | Milestone | State | Evidence |
|---|---|---|---|
| 0 | Foundation: Maven, enforcer, Spotless, SpotBugs, CI, docs | done | `mvn verify` green; `.github/workflows/ci.yml` (not yet run on GitHub: nothing pushed) |
| 1 | Domain model | done | `DomainModelTest` (23) |
| 2 | Parsing, normalization, severity filter, dedup | done | `NormalizerTest` (24), `DeduplicatorTest` (4), `EventProcessorTest` (4), `NetworkZoneEnricherTest` (9) |
| 3 | Routing, console + JSONL sinks, runnable pipeline | done | `RouterTest`, `FileOutputsTest`, `AppTest` (CLI end to end on samples) |
| 4 | Bounded queue, workers, backpressure, shutdown | done | `PipelineTest` (8), `SinkDispatcherTest` (8) |
| 5 | Retries, DLQ, partial delivery | done | `RetryPolicyTest`, `SinkDispatcherTest`, `PipelineTest.partialSink…`, `PipelineStressTest` |
| 6 | PostgreSQL (Flyway, batching) + HTTP source | done | `PostgresSinkIT` (4, real PostgreSQL 18 via Testcontainers), `PostgresSinkClassificationTest` (10), `HttpEventSourceTest` (5) |
| 7 | YAML config, structured logs, metrics endpoint | done | `ConfigLoaderTest` (7), `MetricsTest` (4) |
| 8 | High-volume correctness + benchmarks | done | `PipelineStressTest` (200k default, 1M verified), `docs/BENCHMARKS.md` |
| 9 | README, ADRs, interview guide | done (first version) | `README.md`, `docs/` |

## Verification log (session 1)

All commands run from the repository root on Apple M3 / macOS 26.6.2 / JDK 21.0.10.

| Command | Result |
|---|---|
| `mvn -B verify` | BUILD SUCCESS. Surefire: 125 tests, 0 failures, 0 skipped. Failsafe: 4 tests, 0 failures, 0 skipped (Docker running). Spotless check clean. SpotBugs: 0 bugs. |
| `mvn -B verify` (before Docker started) | BUILD SUCCESS with the 4 `PostgresSinkIT` tests **skipped** (Testcontainers `disabledWithoutDocker`). |
| `mvn test -Dtest=PipelineStressTest -Dsecstream.stress.events=1000000` | pass, 8.3 s (8 workers, 4 producers, 30%-failing sink; accounting exact, no loss, no duplicates in sinks) |
| `java -jar target/secstream-0.1.0-SNAPSHOT-all.jar --config config/pipeline.yaml` | exit 0; 5 alert lines on stdout, 9 lines in `out/archive.jsonl`, 3 in `out/dead-letter.jsonl` (both `-rw-------`); final accounting accepted=13 routed=9 duplicates=1 malformed=3 balanced=true |
| `docker compose up -d --wait` + `--config config/pipeline-full.yaml`, `curl` POST of `samples/signin.jsonl`, SIGTERM | HTTP 202 `{"accepted":8,...}`; GET → 405; `/metrics` and final log: accepted=50008 = routed 9754 + filtered 39570 + duplicates 199 + malformed 485; `security_events` row count 9754 = `sink_delivered{db}`; graceful shutdown, exit code 143 (JVM convention for SIGTERM) |
| Benchmarks | three series recorded in `docs/BENCHMARKS.md` |

Bugs found by the tests during the session and fixed:
1. `GatedSink.open()` (test double) accidentally overrode `EventSink.open()`, so the gate opened at dispatcher start; renamed `release()`.
2. Per-sink `routed` was counted before `queue.put`; an interrupted put was then counted again by `abandonUnsubmitted` → `routed ≠ delivered + deadLettered` after forced shutdown. Now counted after the hand-off.
3. A batch whose write failed because a forced shutdown interrupted it was dead-lettered as `RETRIES_EXHAUSTED`; now `SHUTDOWN_ABANDONED`.
4. SpotBugs flagged nullable-annotated config fields that are always defaulted; nullability tightened, validated-required fields go through `PipelineAssembler.required`.
5. google-java-format 1.37.0 crashes inside Spotless 3.10.4; pinned 1.36.0.
6. Blocking `submit` used `Long.MAX_VALUE` as its deadline; `nanoTime() - deadline` overflows if `nanoTime()` is negative (allowed by the JDK), which would have rejected a blocking submit as queue-full. Blocking submits now have no deadline arithmetic at all. Found by code review, not by a test (cannot be provoked on this platform).

## Known limitations / open decisions

- Not durable; single process (see README "Limitations"). Durable acceptance via WAL or Kafka/Event Hubs is the main open design item.
- One stuck sink eventually stalls all routing (blocking backpressure by design, ADR 0002). Per-sink overflow policies (spill to DLQ) are not implemented.
- File sink throughput is bounded by one thread doing JSON serialization + write (benchmark shows no worker scaling with the file sink). Possible fix: serialize on workers and pass bytes to the dispatcher.
- Latency quantiles are cumulative since start.
- Jackson 2.x chosen over 3.x; migration not done.
- On SIGTERM the process exits with 143 (shutdown-hook path), not 0, even when the drain was graceful; `App.run`'s exit code only applies when all sources are finite.
- No Maven wrapper (would need `mvn wrapper:wrapper`; easy follow-up).
- CI has never run on GitHub (nothing pushed). The workflow uses `actions/checkout@v7`, `actions/setup-java@v6`, `actions/upload-artifact@v7` (latest major tags as of 2026-10-27).
- Benchmark variance on a laptop is high (Docker Desktop was running; M3 has 4 performance + 4 efficiency cores).

## Next concrete tasks

1. With the owner's approval: first commit(s) and push to `MahijithMenon/secstream`; confirm CI passes on GitHub (Testcontainers on `ubuntu-latest`).
2. Add the Maven wrapper.
3. Benchmark on a quieter machine (Docker stopped, fixed CPU governor) and optionally profile the file sink with JFR; move serialization off the dispatcher thread if confirmed.
4. Optional stretch from the guide: sliding-window anomaly rule (>5 failed sign-ins per user per minute), per-user partitioned queues, Kafka/Event Hubs source.
