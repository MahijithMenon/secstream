package com.mahijith.secstream.processing;

import static org.assertj.core.api.Assertions.assertThat;

import com.mahijith.secstream.model.EventCategory;
import com.mahijith.secstream.model.EventFormat;
import com.mahijith.secstream.model.EventOutcome;
import com.mahijith.secstream.model.Severity;
import com.mahijith.secstream.routing.EventMatcher;
import com.mahijith.secstream.routing.Router;
import com.mahijith.secstream.routing.RoutingRule;
import com.mahijith.secstream.testing.TestEvents;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class EventProcessorTest {

  private EventProcessor processor(Severity min, List<RoutingRule> rules) {
    return new EventProcessor(
        EventProcessor.defaultNormalizers(),
        new SeverityFilter(min),
        new Deduplicator(1024),
        List.of(new NetworkZoneEnricher()),
        new Router(rules));
  }

  private static final List<RoutingRule> ALERTS_AND_ARCHIVE =
      List.of(
          new RoutingRule(
              "alerts",
              new EventMatcher(
                  Severity.MEDIUM,
                  Set.of(EventCategory.AUTHENTICATION),
                  Set.of(EventOutcome.FAILURE)),
              List.of("alerts")),
          new RoutingRule("archive", EventMatcher.ANY, List.of("archive")));

  @Test
  void routesFailedSignInToAlertsAndArchive() {
    var result =
        processor(Severity.INFO, ALERTS_AND_ARCHIVE)
            .process(TestEvents.raw(EventFormat.SIGN_IN, TestEvents.signIn("a", "FAILURE")));
    assertThat(result)
        .isInstanceOfSatisfying(
            ProcessingResult.Routed.class,
            r -> {
              assertThat(r.sinks()).containsExactly("alerts", "archive");
              assertThat(r.event().attributes())
                  .containsEntry(NetworkZoneEnricher.ATTRIBUTE, "external");
            });
  }

  @Test
  void successfulSignInGoesToArchiveOnly() {
    var result =
        processor(Severity.INFO, ALERTS_AND_ARCHIVE)
            .process(TestEvents.raw(EventFormat.SIGN_IN, TestEvents.signIn("a", "SUCCESS")));
    assertThat(result)
        .isInstanceOfSatisfying(
            ProcessingResult.Routed.class, r -> assertThat(r.sinks()).containsExactly("archive"));
  }

  @Test
  void eachOutcomeIsReported() {
    EventProcessor p =
        processor(
            Severity.LOW,
            List.of(
                new RoutingRule(
                    "high", new EventMatcher(Severity.HIGH, Set.of(), Set.of()), List.of("x"))));
    assertThat(p.process(TestEvents.raw(EventFormat.SIGN_IN, "garbage")))
        .isInstanceOf(ProcessingResult.Malformed.class);
    assertThat(p.process(TestEvents.raw(EventFormat.SIGN_IN, TestEvents.signIn("s", "SUCCESS"))))
        .isInstanceOf(ProcessingResult.Filtered.class);
    assertThat(p.process(TestEvents.raw(EventFormat.SIGN_IN, TestEvents.signIn("f", "FAILURE"))))
        .isInstanceOf(ProcessingResult.Unrouted.class);
    assertThat(p.process(TestEvents.raw(EventFormat.SIGN_IN, TestEvents.signIn("f", "FAILURE"))))
        .isInstanceOf(ProcessingResult.Duplicate.class);
    assertThat(
            p.process(TestEvents.raw(EventFormat.FIREWALL, TestEvents.firewall("x", 22, "DENY"))))
        .isInstanceOf(ProcessingResult.Routed.class);
  }

  @Test
  void filteredEventsDoNotConsumeDedupCapacity() {
    EventProcessor p = processor(Severity.MEDIUM, ALERTS_AND_ARCHIVE);
    String success = TestEvents.signIn("same", "SUCCESS");
    assertThat(p.process(TestEvents.raw(EventFormat.SIGN_IN, success)))
        .isInstanceOf(ProcessingResult.Filtered.class);
    assertThat(p.process(TestEvents.raw(EventFormat.SIGN_IN, success)))
        .isInstanceOf(ProcessingResult.Filtered.class);
  }
}
