package com.mahijith.secstream.delivery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.mahijith.secstream.metrics.PipelineMetrics;
import com.mahijith.secstream.metrics.SinkMetrics.SinkSnapshot;
import com.mahijith.secstream.model.SecurityEvent;
import com.mahijith.secstream.testing.FlakySink;
import com.mahijith.secstream.testing.GatedSink;
import com.mahijith.secstream.testing.RecordingSink;
import com.mahijith.secstream.testing.TestEvents;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.ZoneOffset;
import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(30)
class SinkDispatcherTest {
  private final PipelineMetrics metrics = new PipelineMetrics();
  private final InMemoryDeadLetterStore dlq = new InMemoryDeadLetterStore();
  private final List<Duration> sleeps = new CopyOnWriteArrayList<>();
  private final Sleeper recordingSleeper = sleeps::add;
  private static final RetryPolicy THREE_ATTEMPTS =
      new RetryPolicy(3, Duration.ofMillis(100), Duration.ofSeconds(1), 2.0);

  private SinkDispatcher dispatcher(EventSink sink, int capacity, int batch, RetryPolicy retry) {
    return new SinkDispatcher(
        sink,
        capacity,
        batch,
        retry,
        new DeadLetterChannel(dlq, metrics),
        metrics.sink(sink.name()),
        recordingSleeper,
        new SplittableRandom(7),
        Clock.fixed(TestEvents.NOW, ZoneOffset.UTC));
  }

  private static Delivery delivery(int i) {
    return new Delivery(
        TestEvents.event("e" + i).userId("user" + i).build(), "test", i, System.nanoTime());
  }

  private SinkSnapshot stats(String sink) {
    return metrics.snapshot().sinks().get(sink);
  }

  private void finishAndAwait(SinkDispatcher d) throws InterruptedException {
    assertThat(d.finish(Duration.ofSeconds(5))).isTrue();
    assertThat(d.awaitTermination(Duration.ofSeconds(10))).isTrue();
  }

  @Test
  void deliversEverythingInBatchesThenClosesSink() throws Exception {
    GatedSink sink = new GatedSink("s");
    SinkDispatcher d = dispatcher(sink, 1_000, 50, THREE_ATTEMPTS);
    d.start();
    d.submit(delivery(0));
    sink.awaitFirstWrite(); // dispatcher holds the first batch; the rest accumulate
    for (int i = 1; i <= 120; i++) {
      d.submit(delivery(i));
    }
    sink.release();
    finishAndAwait(d);

    assertThat(sink.written()).hasSize(121);
    assertThat(sink.batchSizes()).containsExactly(1, 50, 50, 20);
    assertThat(sink.closed()).isTrue();
    assertThat(stats("s").delivered()).isEqualTo(121);
    assertThat(stats("s").routed()).isEqualTo(121);
  }

  @Test
  void retriesTransientFailuresWithBackoffThenSucceeds() throws Exception {
    FlakySink sink = new FlakySink("s", attempt -> attempt < 2, true);
    SinkDispatcher d = dispatcher(sink, 10, 10, THREE_ATTEMPTS);
    d.start();
    d.submit(delivery(1));
    finishAndAwait(d);

    assertThat(sink.written()).hasSize(1);
    assertThat(sink.attempts()).isEqualTo(3);
    assertThat(stats("s").retries()).isEqualTo(2);
    assertThat(stats("s").writeFailures()).isEqualTo(2);
    assertThat(sleeps).hasSize(2);
    assertThat(sleeps.get(0)).isBetween(Duration.ofMillis(50), Duration.ofMillis(100));
    assertThat(sleeps.get(1)).isBetween(Duration.ofMillis(100), Duration.ofMillis(200));
    assertThat(dlq.entries()).isEmpty();
  }

  @Test
  void exhaustedRetriesDeadLetterEveryEventInTheBatch() throws Exception {
    FlakySink sink = new FlakySink("s", attempt -> true, true);
    SinkDispatcher d = dispatcher(sink, 10, 10, THREE_ATTEMPTS);
    for (int i = 0; i < 4; i++) {
      d.submit(delivery(i)); // queued before start, so they form one batch
    }
    d.start();
    finishAndAwait(d);

    assertThat(sink.attempts()).isEqualTo(3);
    assertThat(dlq.entries())
        .hasSize(4)
        .allSatisfy(
            dl -> {
              assertThat(dl.reason()).isEqualTo(DeadLetterReason.RETRIES_EXHAUSTED);
              assertThat(dl.sink()).isEqualTo("s");
              assertThat(dl.attempts()).isEqualTo(3);
              assertThat(dl.event()).isNotNull();
              assertThat(dl.detail()).contains("IOException");
            });
    SinkSnapshot s = stats("s");
    assertThat(s.routed()).isEqualTo(s.delivered() + s.deadLettered());
    assertThat(s.deadLettered()).isEqualTo(4);
  }

  @Test
  void permanentFailureIsolatesThePoisonEventAndDeliversTheRest() throws Exception {
    RecordingSink sink =
        new RecordingSink("s") {
          @Override
          protected void beforeWrite(List<SecurityEvent> batch) throws SinkException {
            if (batch.stream().anyMatch(e -> "user2".equals(e.userId()))) {
              throw SinkException.permanentFailure(
                  "constraint violation", new IOException("23505"));
            }
          }
        };
    SinkDispatcher d = dispatcher(sink, 10, 10, THREE_ATTEMPTS);
    for (int i = 0; i < 5; i++) {
      d.submit(delivery(i));
    }
    d.start();
    finishAndAwait(d);

    assertThat(sink.written())
        .extracting(SecurityEvent::userId)
        .containsExactly("user0", "user1", "user3", "user4");
    assertThat(dlq.entries())
        .singleElement()
        .satisfies(
            dl -> {
              assertThat(dl.reason()).isEqualTo(DeadLetterReason.DELIVERY_REJECTED);
              assertThat(dl.event().userId()).isEqualTo("user2");
            });
    assertThat(sleeps).as("permanent failures are not retried with backoff").isEmpty();
  }

  @Test
  void unexpectedRuntimeExceptionFromSinkIsRetriedThenDeadLettered() throws Exception {
    RecordingSink sink =
        new RecordingSink("s") {
          @Override
          protected void beforeWrite(List<SecurityEvent> batch) {
            throw new IllegalStateException("bug");
          }
        };
    SinkDispatcher d = dispatcher(sink, 10, 10, THREE_ATTEMPTS);
    d.start();
    d.submit(delivery(1));
    finishAndAwait(d);
    assertThat(dlq.entries())
        .singleElement()
        .extracting(DeadLetter::reason)
        .isEqualTo(DeadLetterReason.RETRIES_EXHAUSTED);
  }

  @Test
  void submitBlocksWhenQueueIsFull() throws Exception {
    GatedSink sink = new GatedSink("s");
    SinkDispatcher d = dispatcher(sink, 2, 1, THREE_ATTEMPTS);
    d.start();
    d.submit(delivery(0));
    sink.awaitFirstWrite(); // item 0 is in the sink; queue is empty
    d.submit(delivery(1));
    d.submit(delivery(2)); // queue now full (capacity 2)

    Thread producer =
        Thread.ofPlatform()
            .start(
                () -> {
                  try {
                    d.submit(delivery(3));
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                  }
                });
    await().atMost(Duration.ofSeconds(5)).until(() -> producer.getState() == Thread.State.WAITING);
    assertThat(producer.isAlive()).isTrue();

    sink.release();
    producer.join(5_000);
    assertThat(producer.isAlive()).isFalse();
    finishAndAwait(d);
    assertThat(sink.written()).hasSize(4);
  }

  @Test
  void forcedStopDeadLettersEverythingUndelivered() throws Exception {
    GatedSink sink = new GatedSink("s");
    SinkDispatcher d = dispatcher(sink, 100, 10, THREE_ATTEMPTS);
    d.start();
    d.submit(delivery(0));
    sink.awaitFirstWrite();
    for (int i = 1; i <= 20; i++) {
      d.submit(delivery(i));
    }
    d.forceStop();
    assertThat(d.awaitTermination(Duration.ofSeconds(5))).isTrue();

    assertThat(sink.written()).isEmpty();
    assertThat(dlq.entries())
        .hasSize(21)
        .allMatch(dl -> dl.reason() == DeadLetterReason.SHUTDOWN_ABANDONED);
    SinkSnapshot s = stats("s");
    assertThat(s.routed()).isEqualTo(21);
    assertThat(s.deadLettered()).isEqualTo(21);
    assertThat(sink.closed()).isTrue();
  }

  @Test
  void writesHappenOnOneThreadOnly() throws Exception {
    RecordingSink sink = new RecordingSink("s"); // throws AssertionError on a second writer thread
    SinkDispatcher d = dispatcher(sink, 10_000, 7, THREE_ATTEMPTS);
    d.start();
    Thread[] producers = new Thread[8];
    for (int p = 0; p < producers.length; p++) {
      int base = p * 1_000;
      producers[p] =
          Thread.ofPlatform()
              .start(
                  () -> {
                    for (int i = 0; i < 1_000; i++) {
                      try {
                        d.submit(delivery(base + i));
                      } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                      }
                    }
                  });
    }
    for (Thread t : producers) {
      t.join();
    }
    finishAndAwait(d);
    assertThat(sink.written()).hasSize(8_000);
    assertThat(sink.distinctIds()).isEqualTo(8_000);
  }
}
