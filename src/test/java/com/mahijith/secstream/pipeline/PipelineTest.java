package com.mahijith.secstream.pipeline;

import static com.mahijith.secstream.pipeline.PipelineHarness.retries;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import com.mahijith.secstream.delivery.DeadLetter;
import com.mahijith.secstream.delivery.DeadLetterReason;
import com.mahijith.secstream.delivery.SinkException;
import com.mahijith.secstream.metrics.MetricsSnapshot;
import com.mahijith.secstream.model.EventCategory;
import com.mahijith.secstream.model.EventFormat;
import com.mahijith.secstream.model.EventOutcome;
import com.mahijith.secstream.model.Severity;
import com.mahijith.secstream.processing.EventEnricher;
import com.mahijith.secstream.routing.EventMatcher;
import com.mahijith.secstream.routing.RoutingRule;
import com.mahijith.secstream.testing.FlakySink;
import com.mahijith.secstream.testing.GatedSink;
import com.mahijith.secstream.testing.RecordingSink;
import com.mahijith.secstream.testing.TestEvents;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(60)
class PipelineTest {
  private static final List<RoutingRule> ALERTS_AND_ARCHIVE =
      List.of(
          new RoutingRule(
              "failed-signins",
              new EventMatcher(
                  Severity.MEDIUM,
                  Set.of(EventCategory.AUTHENTICATION),
                  Set.of(EventOutcome.FAILURE)),
              List.of("alerts")),
          new RoutingRule("archive", EventMatcher.ANY, List.of("archive")));

  private static final List<RoutingRule> ALL_TO_ARCHIVE =
      List.of(new RoutingRule("all", EventMatcher.ANY, List.of("archive")));

  @Test
  void endToEndRoutingAndAccounting() throws Exception {
    RecordingSink alerts = new RecordingSink("alerts");
    RecordingSink archive = new RecordingSink("archive");
    PipelineHarness h =
        new PipelineHarness(
            4, 100, 100, retries(3), List.of(), ALERTS_AND_ARCHIVE, alerts, archive);
    h.pipeline.start();

    Pipeline p = h.pipeline;
    assertThat(p.submit("t", EventFormat.SIGN_IN, TestEvents.signIn("a", "FAILURE")))
        .isEqualTo(Admission.ACCEPTED);
    p.submit("t", EventFormat.SIGN_IN, TestEvents.signIn("b", "SUCCESS"));
    p.submit("t", EventFormat.SIGN_IN, TestEvents.signIn("a", "FAILURE")); // duplicate
    p.submit("t", EventFormat.FIREWALL, TestEvents.firewall("f", 443, "ALLOW"));
    p.submit("t", EventFormat.SIGN_IN, "{broken");
    assertThat(p.submit("t", EventFormat.SIGN_IN, "x".repeat(5_000)))
        .isEqualTo(Admission.REJECTED_TOO_LARGE);

    assertThat(p.shutdown(Duration.ofSeconds(10)).graceful()).isTrue();

    MetricsSnapshot s = h.snapshot();
    assertThat(s.accepted()).isEqualTo(5);
    assertThat(s.rejectedTooLarge()).isEqualTo(1);
    assertThat(s.routed()).isEqualTo(3);
    assertThat(s.duplicates()).isEqualTo(1);
    assertThat(s.malformed()).isEqualTo(1);
    assertThat(s.processingBalanced()).isTrue();
    assertThat(s.deliveriesBalanced()).isTrue();
    assertThat(alerts.written()).hasSize(1);
    assertThat(archive.written()).hasSize(3);
    assertThat(s.sinks().get("archive").delivered()).isEqualTo(3);
    assertThat(h.deadLetters.entries())
        .singleElement()
        .satisfies(
            dl -> {
              assertThat(dl.reason()).isEqualTo(DeadLetterReason.MALFORMED);
              assertThat(dl.rawPayload()).isEqualTo("{broken");
            });
    assertThat(alerts.closed()).isTrue();
  }

  @Test
  void admissionIsRefusedAfterShutdownAndBeforeStart() throws Exception {
    PipelineHarness h =
        new PipelineHarness(
            1, 10, 10, retries(1), List.of(), ALL_TO_ARCHIVE, new RecordingSink("archive"));
    assertThat(h.pipeline.submit("t", EventFormat.SIGN_IN, "{}"))
        .isEqualTo(Admission.REJECTED_CLOSED);
    h.pipeline.start();
    h.pipeline.shutdown(Duration.ofSeconds(5));
    assertThat(h.pipeline.submit("t", EventFormat.SIGN_IN, "{}"))
        .isEqualTo(Admission.REJECTED_CLOSED);
    assertThat(h.snapshot().rejectedClosed()).isEqualTo(2);
    assertThat(h.snapshot().accepted()).isZero();
  }

  @Test
  void workerExceptionIsDeadLetteredAndWorkerKeepsGoing() throws Exception {
    EventEnricher buggy =
        e -> {
          if ("boom".equals(e.sourceEventId())) {
            throw new IllegalStateException("enricher bug");
          }
          return e;
        };
    RecordingSink archive = new RecordingSink("archive");
    PipelineHarness h =
        new PipelineHarness(1, 10, 10, retries(1), List.of(buggy), ALL_TO_ARCHIVE, archive);
    h.pipeline.start();
    h.pipeline.submit("t", EventFormat.SIGN_IN, TestEvents.signIn("boom", "SUCCESS"));
    h.pipeline.submit("t", EventFormat.SIGN_IN, TestEvents.signIn("fine", "SUCCESS"));
    h.pipeline.shutdown(Duration.ofSeconds(5));

    assertThat(archive.written()).hasSize(1);
    assertThat(h.snapshot().processingErrors()).isEqualTo(1);
    assertThat(h.snapshot().processingBalanced()).isTrue();
    assertThat(h.deadLetters.entries())
        .singleElement()
        .extracting(DeadLetter::reason)
        .isEqualTo(DeadLetterReason.PROCESSING_ERROR);
  }

  @Test
  void partialSinkFailureDoesNotAffectOtherSinks() throws Exception {
    FlakySink broken = new FlakySink("alerts", attempt -> true, false);
    RecordingSink archive = new RecordingSink("archive");
    PipelineHarness h =
        new PipelineHarness(
            2, 100, 100, retries(3), List.of(), ALERTS_AND_ARCHIVE, broken, archive);
    h.pipeline.start();
    for (int i = 0; i < 20; i++) {
      h.pipeline.submit("t", EventFormat.SIGN_IN, TestEvents.signIn("e" + i, "FAILURE"));
    }
    h.pipeline.shutdown(Duration.ofSeconds(10));

    MetricsSnapshot s = h.snapshot();
    assertThat(archive.written()).hasSize(20);
    assertThat(s.sinks().get("alerts").deadLettered()).isEqualTo(20);
    assertThat(s.sinks().get("archive").delivered()).isEqualTo(20);
    assertThat(s.deliveriesBalanced()).isTrue();
    assertThat(h.deadLetters.entries()).hasSize(20).allMatch(dl -> "alerts".equals(dl.sink()));
  }

  @Test
  void backpressureBlocksProducersAndRejectsTimedSubmits() throws Exception {
    GatedSink archive = new GatedSink("archive");
    // 1 worker, ingest capacity 2, sink capacity 1: the whole chain fills after a few events.
    PipelineHarness h =
        new PipelineHarness(1, 2, 1, retries(1), List.of(), ALL_TO_ARCHIVE, archive);
    h.pipeline.start();
    h.pipeline.submit("t", EventFormat.SIGN_IN, TestEvents.signIn("e0", "SUCCESS"));
    archive.awaitFirstWrite();

    // Fill: worker blocks on the full sink queue, ingest queue fills behind it.
    int accepted = 1;
    while (h.pipeline.trySubmit(
            "t",
            EventFormat.SIGN_IN,
            TestEvents.signIn("e" + accepted, "SUCCESS"),
            Duration.ofMillis(100))
        == Admission.ACCEPTED) {
      accepted++;
      assertThat(accepted).as("queue must be bounded").isLessThan(10);
    }
    assertThat(h.snapshot().rejectedQueueFull()).isEqualTo(1);
    assertThat(h.snapshot().queueDepth()).isEqualTo(2);

    final int next = accepted;
    Thread blockedProducer =
        Thread.ofPlatform()
            .start(
                () -> {
                  try {
                    h.pipeline.submit(
                        "t", EventFormat.SIGN_IN, TestEvents.signIn("e" + next, "SUCCESS"));
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                  }
                });
    await()
        .atMost(Duration.ofSeconds(5))
        .until(() -> blockedProducer.getState() == Thread.State.TIMED_WAITING);
    assertThat(blockedProducer.isAlive()).isTrue();

    archive.release();
    blockedProducer.join(5_000);
    assertThat(blockedProducer.isAlive()).isFalse();
    assertThat(h.pipeline.shutdown(Duration.ofSeconds(10)).graceful()).isTrue();
    assertThat(archive.written()).hasSize(accepted + 1);
    assertThat(h.snapshot().processingBalanced()).isTrue();
  }

  @Test
  void gracefulShutdownDrainsQueuedAndInFlightWork() throws Exception {
    GatedSink archive = new GatedSink("archive");
    PipelineHarness h =
        new PipelineHarness(2, 1_000, 1_000, retries(1), List.of(), ALL_TO_ARCHIVE, archive);
    h.pipeline.start();
    for (int i = 0; i < 500; i++) {
      h.pipeline.submit("t", EventFormat.SIGN_IN, TestEvents.signIn("e" + i, "SUCCESS"));
    }
    archive.awaitFirstWrite(); // one batch in flight inside the sink, the rest queued

    Thread stopper =
        Thread.ofPlatform()
            .start(
                () -> {
                  try {
                    h.pipeline.shutdown(Duration.ofSeconds(30));
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                  }
                });
    await()
        .atMost(Duration.ofSeconds(5))
        .until(
            () ->
                h.pipeline.submit("t", EventFormat.SIGN_IN, TestEvents.signIn("late", "SUCCESS"))
                    == Admission.REJECTED_CLOSED);

    archive.release();
    stopper.join(30_000);
    assertThat(stopper.isAlive()).isFalse();
    assertThat(archive.written()).hasSize(500);
    assertThat(h.deadLetters.entries()).isEmpty();
    MetricsSnapshot s = h.snapshot();
    assertThat(s.processingBalanced()).isTrue();
    assertThat(s.deliveriesBalanced()).isTrue();
  }

  @Test
  void forcedShutdownStillAccountsForEveryAcceptedEvent() throws Exception {
    GatedSink archive = new GatedSink("archive"); // never opened: the sink is stuck
    PipelineHarness h =
        new PipelineHarness(2, 50, 5, retries(1), List.of(), ALL_TO_ARCHIVE, archive);
    h.pipeline.start();
    int accepted = 0;
    for (int i = 0; i < 100; i++) {
      if (h.pipeline.trySubmit(
              "t",
              EventFormat.SIGN_IN,
              TestEvents.signIn("e" + i, "SUCCESS"),
              Duration.ofMillis(20))
          == Admission.ACCEPTED) {
        accepted++;
      }
    }
    archive.awaitFirstWrite();

    ShutdownReport report = h.pipeline.shutdown(Duration.ofMillis(500));

    assertThat(report.graceful()).isFalse();
    MetricsSnapshot s = h.snapshot();
    assertThat(s.accepted()).isEqualTo(accepted);
    assertThat(s.processingBalanced())
        .as("accepted == every processing outcome incl. abandoned")
        .isTrue();
    assertThat(s.sinks().get("archive"))
        .satisfies(
            k ->
                assertThat(k.routed())
                    .as(k.toString())
                    .isEqualTo(k.delivered() + k.deadLettered()));
    assertThat(archive.written()).isEmpty();
    long sinkAbandoned = s.sinks().get("archive").deadLettered();
    assertThat(s.abandoned() + sinkAbandoned).isEqualTo(accepted);
    assertThat(h.deadLetters.entries())
        .hasSize(accepted)
        .allMatch(dl -> dl.reason() == DeadLetterReason.SHUTDOWN_ABANDONED);
  }

  @Test
  void startFailsIfAnySinkCannotOpen() {
    RecordingSink unopenable =
        new RecordingSink("archive") {
          @Override
          public void open() throws SinkException {
            throw SinkException.permanentFailure("cannot open", new IOException("disk"));
          }
        };
    PipelineHarness h =
        new PipelineHarness(1, 10, 10, retries(1), List.of(), ALL_TO_ARCHIVE, unopenable);
    assertThatThrownBy(h.pipeline::start).isInstanceOf(SinkException.class);
    assertThat(h.pipeline.isRunning()).isFalse();
  }
}
