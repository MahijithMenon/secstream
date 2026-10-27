# SecStream interview guide

Every section describes what the code actually does, with file and test references. Things that are *not* implemented are labelled **Not implemented**.

---

## 1. Bounded queues and backpressure

**Problem.** If producers are faster than consumers, an unbounded queue grows until the JVM dies, and latency grows without limit long before that.

**Implementation.** Every hand-off is an `ArrayBlockingQueue` with a fixed capacity: the ingest queue in `Pipeline` (`pipeline.queueCapacity`) and one queue per sink in `SinkDispatcher` (`delivery.queueCapacity`). When the ingest queue is full:
- pull sources (file, generator) call `submit`, which waits: reading pauses, nothing is lost;
- the HTTP source calls `trySubmit` with `admissionTimeout` and answers **503 + Retry-After** with the number of events accepted, so the client resends the suffix.

A slow sink fills its own queue, workers block in `put`, the ingest queue fills, and the pressure reaches the sources: backpressure all the way through.

**Why `ArrayBlockingQueue`.** One pre-allocated array, a single lock, no per-element allocation; with a bound it is the textbook choice. `LinkedBlockingQueue` has separate put/take locks (more throughput under contention) but allocates a node per element; I'd switch only if a benchmark showed the ingest lock as the bottleneck.

**Trade-offs / failure cases.** Blocking turns a stuck sink into a stuck pipeline (head-of-line blocking, ADR 0002). Rejecting instead would isolate sinks but lose data. The ingest capacity also bounds what a crash loses.

**Tests.** `PipelineTest.backpressureBlocksProducersAndRejectsTimedSubmits` (fills the chain, asserts a bounded number accepted, a `REJECTED_QUEUE_FULL`, a producer parked in `TIMED_WAITING`, then release); `SinkDispatcherTest.submitBlocksWhenQueueIsFull`; `HttpEventSourceTest.queueFullReturns503WithAcceptedPrefix`.

## 2. Producer–consumer coordination

**Implementation.** Sources produce `RawEvent`s; `N` workers `take()`; each worker produces `Delivery`s for sink dispatchers that `take()` + `drainTo()` for batching. End of stream is a sentinel object per consumer (poison pill), compared by identity (`Pipeline.END_OF_STREAM`, `DispatchItem.EndOfStream`). FIFO order guarantees every real event before a pill is processed first.

**The subtle part** is the race between producers and shutdown (ADR 0004): a read-write lock makes "is it closed? then offer" atomic with respect to "close". See `Pipeline.admit` and `Pipeline.closeAdmission`.

**Tests.** `PipelineTest.gracefulShutdownDrainsQueuedAndInFlightWork`, `PipelineStressTest` (4 producers released by a `CyclicBarrier`, 8 workers).

## 3. `ExecutorService` and thread-pool sizing

**Implementation.** Workers: `Executors.newFixedThreadPool(workers)` running long-lived loops. Sink dispatchers: one dedicated platform thread each. HTTP: a fixed pool (`threads`). Metrics: single-thread executors.

**Sizing.** Worker stages are CPU-bound (JSON parsing, SHA-256, validation), so `workers ≈ cores` is the starting point; more threads only add context switching. I/O-bound work (sinks) is isolated on its own threads, where the rule would be `threads ≈ cores × (1 + wait/compute)` (Goetz). Measured scaling is in `docs/BENCHMARKS.md`.

**Why not virtual threads for workers?** They help when threads mostly wait; these mostly compute. Virtual threads would be a reasonable choice for the HTTP handlers, but a fixed pool also bounds concurrent request memory.

**Failure cases.** A task that throws in a fixed pool kills only that run; worker loops catch `RuntimeException` per event (`Pipeline.handle`) so one bad event cannot silently shrink the pool.

## 4. `HashMap` vs `ConcurrentHashMap`

**Problem.** Dedup is shared by all workers.

**Answer.** `HashMap` under concurrent writes can lose entries and corrupt itself during resize; it gives no visibility guarantees. `ConcurrentHashMap` is safe and `putIfAbsent`/`add` is atomic, but it is **unbounded**. `Deduplicator` therefore uses 64 lock-striped access-ordered `LinkedHashMap`s: bounded (LRU per stripe), atomic per id, low contention (ADR 0005). Elsewhere `ConcurrentHashMap` is used where its semantics fit (`PipelineMetrics.sinks`, `computeIfAbsent`).

**Test.** `DeduplicatorTest.concurrentCallersSeeEachIdAsNewExactlyOnce`: 16 threads released together race over 1,000 ids; exactly 1,000 "first sightings" must result. A non-atomic `contains` + `put` fails this reliably. `memoryIsBoundedByEvictingOldestIds` documents the eviction trade-off.

## 5. `AtomicLong` vs `LongAdder`

**Implementation.** Counters (`PipelineMetrics`, `SinkMetrics`) are `LongAdder`: written on every event by many threads, read rarely. `LongAdder` spreads increments over cells so writers don't fight over one CAS; `sum()` is O(cells) and not an atomic snapshot. The admission sequence number is an `AtomicLong` (`Pipeline.sequence`): we need each caller to get a **unique** value from `incrementAndGet`, which `LongAdder` cannot provide.

**Consequence.** A `/metrics` scrape under load can be momentarily inconsistent across counters; invariants are asserted only at quiescence (docs/METRICS.md).

## 6. `synchronized` vs `ReentrantLock`

**Where each is used.**
- `ReentrantReadWriteLock` in `Pipeline`: many concurrent submitters (readers) vs one closer (writer) — impossible with `synchronized`.
- `ReentrantLock` per dedup stripe and in `FileDeadLetterStore`: a private lock object (no outsider can synchronize on our monitor), and on Java 21 `synchronized` pins a virtual thread's carrier during blocking I/O while `ReentrantLock` does not (JEP 444; fixed in JDK 24 by JEP 491).
- `synchronized` in `LatencyRecorder.snapshot`: a tiny critical section, single reader — the simplest correct tool.

**Features `ReentrantLock` adds:** `tryLock(timeout)`, `lockInterruptibly` (used in `Pipeline.admit`), fairness, multiple conditions.

## 7. `volatile` and happens-before

**Uses in the code.**
- `volatile boolean stopped` in sources and `volatile int boundPort` in `HttpEventSource`: a flag written by one thread and read by another; no compound action, so `volatile` is enough.
- `closed` in `Pipeline` is **not** volatile: it is only read and written under `admissionLock`, and lock release/acquire provides the happens-before edge.
- `BlockingQueue.put` → `take` is a happens-before edge, so a `RawEvent` built by a producer is fully visible to the worker without further synchronization (and the record is immutable anyway).
- `Thread.start` / `Thread.join` order `sink.open()` before the dispatcher's writes and the writes before the test's reads (`EventSink` contract).

**Failure case avoided.** `RecordingSink` in tests uses `Collections.synchronizedList` for its contents and asserts it is only ever written by one thread.

## 8. Race conditions, deadlocks, livelocks

- **Race found and fixed:** the admission/shutdown race (ADR 0004). Another found by tests: a worker interrupted in `put` was counted as routed twice (before the `put`, then again when abandoned). `SinkDispatcher.submit` now counts after the hand-off (ADR 0003).
- **Deadlock avoided:** the obvious one would be shutdown holding the write lock while a submitter holds the read lock blocked forever on a full queue. Submitters never block while holding the lock for more than one 50 ms bounded `offer`, and release it between attempts.
- **Lock ordering:** there is no code path that holds two locks at once (dedup stripe, DLQ lock, admission lock are all leaf locks).
- **Livelock:** synchronized retries from many pipeline instances against one recovering database can keep knocking it over. Jittered backoff (`RetryPolicy`) de-synchronizes them.

## 9. Retries, idempotency, deduplication, delivery guarantees

**Implementation.** `RetryPolicy`: bounded attempts, exponential growth, cap, *equal jitter* (`[cap/2, cap]`). Backoff goes through `Sleeper` so tests are instant and deterministic. Classification is the sink's job (`SinkException.retryable`); `PostgresSink.classify` maps SQLState classes 22/23/42 to permanent and everything else (connection, serialization, timeouts, shutdown) to transient. Unexpected `RuntimeException`s are retried boundedly.

On a **permanent** failure of a multi-event batch the dispatcher retries each event alone, so one poison event doesn't dead-letter 499 good ones (`SinkDispatcherTest.permanentFailureIsolatesThePoisonEventAndDeliversTheRest`).

**Guarantees.** At-least-once per sink while running; PostgreSQL inserts are idempotent (`ON CONFLICT (event_id) DO NOTHING`, one transaction per batch) → effectively-once rows. **Not** exactly-once end to end; not durable across crashes.

**Tests.** `RetryPolicyTest`, `SinkDispatcherTest` (success after retries, exhaustion, isolation, interrupt), `PipelineStressTest` with a deterministic 30%-failing sink (`attempt % 10 < 3`): every event delivered exactly once, zero dead letters, retries > 0. `PostgresSinkIT.migratesBatchInsertsAndIsIdempotentOnRetry` re-writes a batch and checks the row count.

## 10. Partial sink failures and event accounting

An event routed to `alerts` and `archive` creates two independent (event, sink) pairs. If `alerts` is down, `archive` still gets it, and the dead letter records `sink = alerts`, so replay targets one sink. Accounting has three boundaries (ADR 0003): admission, processing (one outcome per accepted event), delivery (one outcome per pair). The guide's `ingested = routed + dropped + dead-lettered` is wrong for fan-out and for in-flight work.

**Tests.** `PipelineTest.partialSinkFailureDoesNotAffectOtherSinks`, `PipelineTest.forcedShutdownStillAccountsForEveryAcceptedEvent`, `PipelineStressTest`.

## 11. Graceful shutdown and crash recovery

**Graceful** (`Pipeline.shutdown`): close admission → pills → join workers → pills to dispatchers → join → close DLQ, all against one deadline. SIGTERM triggers it via a shutdown hook (`App.stop`, idempotent). Health endpoint flips to 503.

**Forced** (deadline expired): interrupt, and dead-letter everything unfinished as `SHUTDOWN_ABANDONED` with enough data to replay. Accounting still balances.

**Crash:** in-memory queues are lost (bounded by total queue capacity). **Not implemented:** durable acceptance. It would need a WAL or a log broker before the 202, offsets committed after all sinks ack, and idempotent sinks to absorb redelivery.

## 12. JDBC batching and transactions

`PostgresSink.write`: one `PreparedStatement`, `addBatch` per event, `executeBatch`, `commit`; rollback on any `SQLException`. `reWriteBatchedInserts=true` lets the driver turn N single-row inserts into multi-row inserts. Autocommit off, statement timeout via `setQueryTimeout`, connection re-opened lazily after a transient failure. Inputs are bind parameters only (`PostgresSinkIT.hostileValuesAreStoredLiterallyViaBindParameters`). Schema is versioned with Flyway (`db/migration/V1__create_security_events.sql`).

Trade-off: bigger batches → fewer round trips but larger rollback units and higher latency per event; `batchSize` is configurable (default 500).

## 13. Throughput, tail latency, bottleneck analysis

Measured, not estimated: `docs/BENCHMARKS.md` lists hardware, JDK, workload, warm-ups, repetitions and the numbers. How to read them:
- Throughput = accepted events / wall time from first submit until the last sink write finished.
- Latency = admission → successful sink write, which under a saturating closed-loop load is dominated by **queueing** (Little's law: wait ≈ queue depth / throughput), not processing. That's why p99 is far above per-event processing time.
- Bottleneck identification: compare queue depths. Full ingest queue + empty sink queue → workers are the limit; full sink queue → that sink is. Then profile (e.g. JFR) before optimizing.

## 14. Horizontal scaling, Kafka / Azure Event Hubs

**Not implemented.** The pipeline is a single process. To scale out:
- Put a partitioned log (Kafka topic or Event Hubs) in front; partition by a key that needs ordering or locality (e.g. `userId` for per-user rules).
- Run N stateless instances in one consumer group; each owns some partitions.
- Commit offsets only after every target sink acknowledged the batch → at-least-once with durable acceptance and crash recovery by replay.
- Dedup per partition (state stays local), with the database primary key as the global backstop.
- Backpressure becomes consumer lag, the key autoscaling signal.

## 15. Java threads vs the Node.js event loop

Node runs JavaScript on one thread and achieves concurrency by never blocking it: I/O is asynchronous, callbacks/promises resume work. That's excellent for I/O-bound services but CPU-heavy work (parsing and hashing millions of events) blocks the loop for everyone unless moved to worker threads.

SecStream uses real parallelism: N worker threads parse on N cores simultaneously, and blocking calls (`put`, JDBC) simply park one thread. The cost is shared-memory concurrency — which is exactly what the locks, atomics and invariants in this guide manage. Backpressure in Node is explicit (`stream.write()` returning false, `'drain'` events); here it is implicit in the bounded blocking queues.
