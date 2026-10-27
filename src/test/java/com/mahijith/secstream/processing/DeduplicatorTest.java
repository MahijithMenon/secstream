package com.mahijith.secstream.processing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.mahijith.secstream.model.EventFormat;
import com.mahijith.secstream.model.EventId;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class DeduplicatorTest {

  @Test
  void firstSightingOnlyOnce() {
    Deduplicator d = new Deduplicator(1024);
    EventId id = EventId.derive(EventFormat.SIGN_IN, "a");
    assertThat(d.firstSighting(id)).isTrue();
    assertThat(d.firstSighting(id)).isFalse();
    assertThat(d.firstSighting(EventId.derive(EventFormat.SIGN_IN, "b"))).isTrue();
  }

  @Test
  void memoryIsBoundedByEvictingOldestIds() {
    Deduplicator d = new Deduplicator(64); // one id per stripe
    List<EventId> ids = new ArrayList<>();
    for (int i = 0; i < 10_000; i++) {
      EventId id = EventId.derive(EventFormat.SIGN_IN, "k" + i);
      ids.add(id);
      d.firstSighting(id);
    }
    // The first id has long since been evicted from its stripe, so it is "new" again: the
    // documented trade-off of a bounded window.
    assertThat(d.firstSighting(ids.get(0))).isTrue();
  }

  @Test
  void rejectsTooSmallCapacity() {
    assertThatThrownBy(() -> new Deduplicator(10)).isInstanceOf(IllegalArgumentException.class);
  }

  /**
   * 16 threads released simultaneously by a barrier all race on the same 1,000 ids. Exactly one
   * thread may win each id. With a non-atomic check-then-put this fails reliably.
   */
  @Test
  void concurrentCallersSeeEachIdAsNewExactlyOnce() throws Exception {
    int threads = 16;
    int idsCount = 1_000;
    Deduplicator d = new Deduplicator(100_000);
    List<EventId> ids = new ArrayList<>();
    for (int i = 0; i < idsCount; i++) {
      ids.add(EventId.derive(EventFormat.FIREWALL, "id" + i));
    }
    CyclicBarrier start = new CyclicBarrier(threads);
    AtomicInteger firstSightings = new AtomicInteger();
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    try {
      List<Future<?>> futures = new ArrayList<>();
      for (int t = 0; t < threads; t++) {
        futures.add(
            pool.submit(
                () -> {
                  start.await();
                  for (EventId id : ids) {
                    if (d.firstSighting(id)) {
                      firstSightings.incrementAndGet();
                    }
                  }
                  return null;
                }));
      }
      for (Future<?> f : futures) {
        f.get(30, TimeUnit.SECONDS);
      }
    } finally {
      pool.shutdownNow();
    }
    assertThat(firstSightings).hasValue(idsCount);
  }
}
