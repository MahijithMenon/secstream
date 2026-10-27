package com.mahijith.secstream.processing;

import com.mahijith.secstream.model.EventId;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Bounded, thread-safe "have I seen this id recently?" check.
 *
 * <p>A plain {@code ConcurrentHashMap.newKeySet().add(id)} is atomic and correct, but grows without
 * bound: a long-running pipeline would eventually run out of memory. This class instead keeps the
 * most recent {@code capacity} ids, split across independently locked stripes:
 *
 * <ul>
 *   <li>Each stripe is an access-ordered {@link LinkedHashMap} that evicts its least recently seen
 *       id, so memory is bounded by {@code capacity} entries.
 *   <li>An id always maps to the same stripe, so check-and-insert for a given id is atomic under
 *       that stripe's lock; two workers racing on the same id cannot both see it as new.
 *   <li>Different ids usually hit different stripes, so workers rarely contend.
 * </ul>
 *
 * <p>Trade-off: the window is approximate (per stripe, not global LRU) and in-memory only, so a
 * duplicate arriving after eviction or after a restart is not detected. Sinks that need stronger
 * guarantees must be idempotent on {@link EventId} (the PostgreSQL sink is).
 */
public final class Deduplicator {
  private static final int STRIPES = 64;

  private final Stripe[] stripes;

  public Deduplicator(int capacity) {
    if (capacity < STRIPES) {
      throw new IllegalArgumentException("dedup capacity must be at least " + STRIPES);
    }
    stripes = new Stripe[STRIPES];
    for (int i = 0; i < STRIPES; i++) {
      stripes[i] = new Stripe(capacity / STRIPES);
    }
  }

  /** Returns true exactly once per id within the retention window. */
  public boolean firstSighting(EventId id) {
    Stripe stripe = stripes[Math.floorMod(id.hashCode(), STRIPES)];
    stripe.lock.lock();
    try {
      return stripe.seen.put(id, Boolean.TRUE) == null;
    } finally {
      stripe.lock.unlock();
    }
  }

  private static final class Stripe {
    final ReentrantLock lock = new ReentrantLock();
    final LinkedHashMap<EventId, Boolean> seen;

    Stripe(int capacity) {
      seen =
          new LinkedHashMap<>(16, 0.75f, true) {
            private static final long serialVersionUID = 1L;

            @Override
            protected boolean removeEldestEntry(Map.Entry<EventId, Boolean> eldest) {
              return size() > capacity;
            }
          };
    }
  }
}
