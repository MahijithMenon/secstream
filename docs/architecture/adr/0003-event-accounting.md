# ADR 0003: Event accounting and delivery guarantees

Status: accepted (2026-10-27)

## Context
The build guide proposed `ingested = routed + dropped + dead-lettered`. That breaks as soon as an event goes to two sinks (counted once or twice?), when one sink succeeds and another fails, while retries are in flight, or while events are still queued.

## Decision
Three boundaries, each with its own invariant:

1. **Admission:** each submission is `ACCEPTED` or rejected (`queue_full`, `too_large`, `closed`). Rejections never enter the pipeline; the caller keeps responsibility (HTTP 503/413).
2. **Processing:** each accepted event gets exactly one of `routed, filtered, duplicate, unrouted, malformed, processing_error, abandoned`.
   `accepted = Σ outcomes` once idle; `inFlight = accepted − Σ outcomes` while running.
3. **Delivery:** each (routed event, target sink) pair gets exactly one of `delivered` or `dead_lettered`.
   Per sink, `routed = delivered + deadLettered` once its queue is drained. Retries and write attempts are reported but are not outcomes.

Dead letters are counted by reason; `Σ dead letters = malformed + processing_error + abandoned + Σ sink deadLettered` (only if the store never fails; failures are counted separately).

Counting details that make the invariants exact:
- `accepted` is incremented before the queue offer and decremented if the offer fails, so a worker can never process an event that is not yet accepted.
- A sink's `routed` is incremented after a successful `put`; an interrupted `put` is accounted through `abandonUnsubmitted`, which counts it and dead-letters it. (Counting before the put double-counted on interrupt; found by `PipelineTest.forcedShutdownStillAccountsForEveryAcceptedEvent`.)
- A failed write whose thread was interrupted is classified `SHUTDOWN_ABANDONED`, not `RETRIES_EXHAUSTED`.

## Consequences
Invariants are checked in tests at quiescence only; `LongAdder` snapshots are not atomic across counters, so a live `/metrics` scrape can be momentarily inconsistent (documented in `docs/METRICS.md`).
