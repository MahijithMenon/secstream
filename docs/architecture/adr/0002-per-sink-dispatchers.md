# ADR 0002: One dispatcher thread and bounded queue per sink

Status: accepted (2026-10-27)

## Context
The original design had workers run the whole chain including the sink write. Problems: no natural batching (JDBC batch inserts need many rows per write), retry backoff sleeps would block CPU workers, and every sink would need to be thread-safe.

## Decision
Workers stop at routing and `put` one `Delivery` per target sink into that sink's bounded queue. Each sink has exactly one dispatcher thread that takes one item, `drainTo`s up to `batchSize - 1` more, and writes them as one batch with retries.

## Consequences
- Batches grow with load (latency stays low when idle; throughput rises when busy) without a linger timer.
- Sinks are single-threaded by contract: `PostgresSink` needs no pool and no locks.
- Backpressure is end to end: slow sink → its queue fills → workers block in `put` → ingest queue fills → sources block / HTTP answers 503.
- Trade-off: one stalled sink eventually stalls all routing (head-of-line blocking at the worker). The sink queue capacity is the bulkhead. An alternative (drop or spill to DLQ when a sink queue is full) would isolate sinks but turns backpressure into data loss; we chose to block. Per-sink overflow policies are a possible extension.
- Throughput ceiling of one sink = one thread. For PostgreSQL, batching makes that acceptable; partitioned multi-writer sinks are future work.
