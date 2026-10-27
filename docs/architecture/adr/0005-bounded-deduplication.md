# ADR 0005: Bounded, lock-striped deduplication

Status: accepted (2026-10-27)

## Context
`ConcurrentHashMap.newKeySet().add(id)` is atomic and correct, but unbounded: a long-running pipeline would grow until OOM. A `HashMap` is not safe for concurrent writers at all (lost updates, corrupted buckets during resize).

## Decision
`Deduplicator`: 64 stripes, each an access-ordered `LinkedHashMap` with `removeEldestEntry` and its own `ReentrantLock`. `id.hashCode()` picks the stripe, so check-and-insert per id is atomic; different ids rarely contend. Capacity is configurable (`processing.dedupCapacity`).

Event identity (`EventId`) = SHA-256 (first 128 bits) over the format plus the upstream id when present, otherwise over canonical normalized fields, so whitespace or key-order differences do not defeat dedup and identical ids from different formats cannot collide.

## Consequences
- Window is approximate (per-stripe LRU) and in-memory: duplicates after eviction or restart pass through. The PostgreSQL sink's primary key on `event_id` is the durable second line of defence.
- Dedup runs after the severity filter, so discarded events don't consume capacity. It marks an id seen even if delivery later fails, so a replay in the same process lifetime is dropped as duplicate: replay tooling must target the sink directly (or restart).
