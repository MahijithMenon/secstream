# ADR 0004: Admission lock and shutdown protocol

Status: accepted (2026-10-27)

## Context
With `if (!closed) queue.put(e)` an event can be put after workers have seen "closed and empty" and exited: accepted but never processed. Poison pills alone don't fix it because producers are external threads that may still be mid-put.

## Decision
- `ReentrantReadWriteLock`: submitters hold the **read** lock across the closed-check and one bounded `offer` (≤ 50 ms); shutdown takes the **write** lock to set `closed`. After that no put can be in progress or start.
- Blocking `submit` loops over bounded offers, releasing the lock in between, so a producer waiting on a full queue never prevents shutdown; it receives `REJECTED_CLOSED`.
- Then one end-of-stream marker per worker (identity-compared sentinel `RawEvent`), FIFO guarantees all real events are processed first. Same pattern per dispatcher (`DispatchItem.EndOfStream`).
- Each step has a deadline. On expiry: `shutdownNow()` interrupts workers (all their blocking calls are interruptible), leftovers in the ingest queue are dead-lettered with their raw payload as `SHUTDOWN_ABANDONED`, and dispatchers are interrupted and dead-letter what they hold.

## Alternatives considered
- `volatile closed` + `poll(timeout)` loops: also correct given the lock, but adds wake-ups and latency to every idle worker.
- `Phaser` counting active submitters: equivalent, less familiar to reviewers.

## Consequences
Graceful shutdown loses nothing. Forced shutdown loses nothing either, but converts unfinished work into dead letters that need replay.
