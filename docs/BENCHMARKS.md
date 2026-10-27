# Benchmarks

Correctness evidence (accounting, no loss, no duplicates) lives in the tests (`PipelineStressTest`, `PipelineTest`). This file is about **performance** only. All numbers below were measured in session 1; none are estimates.

## Environment

| | |
|---|---|
| Machine | Apple M3 (4 performance + 4 efficiency cores; `hw.ncpu` = 8), 8 GiB RAM, macOS 26.6.2 |
| JDK | OpenJDK 21.0.10 (Homebrew), HotSpot server VM, default GC (G1), max heap 2048 MiB (JVM default = ¼ RAM) |
| Background load | Docker Desktop running (PostgreSQL ITs earlier); normal desktop apps. Not an isolated machine. |
| Code | uncommitted working tree of 2026-10-27. Runs predate one fix in `Pipeline.admit` (deadline overflow if `nanoTime()` is negative), which cannot trigger on macOS where `nanoTime()` is positive. |

## Method

Harness: `src/test/java/com/mahijith/secstream/bench/PipelineBenchmark.java`.

```bash
mvn -q -B -Dskip.quality=true test-compile
```

```bash
mvn -q -B -Pbench -Dskip.quality=true exec:java -Dbench.sink=discard
```

- **Workload:** synthetic events pre-generated before timing (so generation is not measured): 50% sign-in / 50% firewall, 20% failures, 2% exact duplicates, 1% malformed; mean payload 173 characters. Fixed seeds, identical stream for every run.
- **Pipeline:** real `Pipeline` with the production stages (both normalizers, severity filter at INFO, dedup with 1M capacity, network-zone enricher, one rule routing everything to one sink), ingest queue 10,000, sink queue 10,000, batch size 500, no retries.
- **Sinks:** `discard` (accepts and drops: measures the pipeline itself) and `file` (JSON Lines to a temp file, no fsync: page cache only).
- **Producers:** 2 (or 4) threads released by a barrier, blocking `submit` (backpressure engaged throughout).
- **Throughput** = accepted events ÷ wall time from the barrier until `shutdown()` returns, i.e. until the last event is written and the sink closed.
- **Latency** = admission → successful sink write per event (HdrHistogram, µs). This is a closed-loop, saturating load, so latency is dominated by **queueing** in the 10,000-slot queues, not by processing time; it is not the latency a lightly loaded system would show.
- **Repetitions:** for each worker count, W warm-up runs (discarded) then R measured runs, each with a fresh pipeline in the same JVM. Reported: median, min and max throughput across measured runs; latency percentiles from the median-throughput run. "Accounting balanced" checks, for every run, `accepted = events`, `accepted = Σ processing outcomes`, and `routed = delivered + dead-lettered`. Dead letters are the 1% malformed payloads, summed over measured runs.

## Results

### Discard sink, 500k events, 2 producers, 2 warm-ups, 5 runs

| workers | median events/s | min | max | p50 µs | p95 µs | p99 µs | accounting balanced | dead letters |
|---:|---:|---:|---:|---:|---:|---:|:---:|---:|
| 1 | 162,587 | 120,424 | 179,288 | 54,015 | 82,303 | 267,519 | yes | 25,795 |
| 2 | 215,731 | 171,578 | 362,944 | 30,623 | 175,999 | 198,143 | yes | 25,795 |
| 4 | 260,204 | 176,221 | 374,552 | 24,863 | 93,055 | 360,191 | yes | 25,795 |
| 8 | 541,764 | 401,027 | 577,173 | 12,967 | 34,079 | 99,071 | yes | 25,795 |

### File sink, 500k events, 2 producers, 2 warm-ups, 5 runs

| workers | median events/s | min | max | p50 µs | p95 µs | p99 µs | accounting balanced | dead letters |
|---:|---:|---:|---:|---:|---:|---:|:---:|---:|
| 1 | 176,627 | 112,553 | 210,393 | 51,967 | 92,031 | 128,319 | yes | 25,795 |
| 2 | 165,550 | 160,834 | 317,502 | 97,023 | 246,655 | 261,119 | yes | 25,795 |
| 4 | 216,862 | 143,722 | 343,582 | 78,271 | 184,447 | 236,543 | yes | 25,795 |
| 8 | 167,188 | 99,067 | 298,911 | 101,759 | 264,447 | 318,463 | yes | 25,795 |

### Discard sink, 1M events, 4 producers, 3 warm-ups, 9 runs

```bash
mvn -q -B -Pbench -Dskip.quality=true exec:java -Dbench.sink=discard -Dbench.events=1000000 -Dbench.runs=9 -Dbench.warmups=3 -Dbench.producers=4
```

| workers | median events/s | min | max | p50 µs | p95 µs | p99 µs | accounting balanced | dead letters |
|---:|---:|---:|---:|---:|---:|---:|:---:|---:|
| 1 | 78,501 | 60,602 | 100,620 | 90,559 | 310,783 | 902,655 | yes | 91,863 |
| 2 | 117,485 | 85,639 | 274,656 | 59,679 | 228,351 | 605,183 | yes | 91,863 |
| 4 | 209,484 | 131,302 | 273,390 | 33,823 | 182,911 | 305,407 | yes | 91,863 |
| 8 | 276,750 | 151,484 | 309,505 | 20,943 | 97,983 | 435,455 | yes | 91,863 |

Same monotonic scaling (≈3.5× from 1 to 8 workers by median), but lower absolute throughput than the 500k runs. Not investigated; plausible causes are four producers contending on the ingest lock while the workers also take from it, and more GC work (1M pre-generated payloads plus a fuller dedup map inside a 2 GiB heap). This is exactly the kind of gap that should be profiled before any optimization.

## Analysis

- **Variance is large** (min and max often differ by 2×). Five runs on a shared laptop with heterogeneous cores are not enough to separate adjacent worker counts with confidence. Treat the medians as indicative; the min/max columns are the honest error bars.
- **Discard sink:** throughput rises with workers in both series, so the CPU-bound stages (JSON parsing, SHA-256 ids, validation, enrichment) are the bottleneck and do parallelize. Scaling is sub-linear and uneven; contributors are the 4 efficiency cores, the 2 producer threads (each also builds a `RawEvent` and contends on the ingest lock), the single ingest `ArrayBlockingQueue` lock shared by producers and workers, and the single sink thread.
- **File sink:** no meaningful scaling with workers. The single dispatcher thread both serializes each event to JSON and writes it, so it is the bottleneck: adding workers only makes the sink queue fill faster, which raises queueing latency (p50 grows from ~52 ms to ~100 ms) without raising throughput. This is the expected cost of the one-writer-per-sink design (ADR 0002). The obvious next step, **not implemented**, is to serialize on the workers and hand the dispatcher bytes; it should be confirmed with a profiler (JFR) first.
- **Latency** numbers are queueing delay under saturation (Little's law: ~10–20k queued events ÷ throughput ≈ tens of ms), not per-event processing cost.
- No result here supports a "zero event loss" claim on its own; that claim is backed by the accounting checks in every run and by `PipelineStressTest`.

## Not measured

PostgreSQL sink throughput, HTTP ingestion throughput, retry-heavy workloads, GC tuning, latency at fixed sub-saturation rates (would need an open-loop, rate-controlled harness).
