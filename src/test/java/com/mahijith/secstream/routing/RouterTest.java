package com.mahijith.secstream.routing;

import static org.assertj.core.api.Assertions.assertThat;

import com.mahijith.secstream.model.EventCategory;
import com.mahijith.secstream.model.EventOutcome;
import com.mahijith.secstream.model.Severity;
import com.mahijith.secstream.testing.TestEvents;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class RouterTest {
  private final Router router =
      new Router(
          List.of(
              new RoutingRule(
                  "critical",
                  new EventMatcher(Severity.HIGH, Set.of(), Set.of()),
                  List.of("pager", "archive")),
              new RoutingRule(
                  "auth-fail",
                  new EventMatcher(
                      Severity.INFO,
                      Set.of(EventCategory.AUTHENTICATION),
                      Set.of(EventOutcome.FAILURE)),
                  List.of("alerts")),
              new RoutingRule("all", EventMatcher.ANY, List.of("archive"))));

  @Test
  void fansOutToEveryMatchingRuleWithoutDuplicateSinks() {
    var event = TestEvents.event("a").severity(Severity.HIGH).build();
    assertThat(router.route(event)).containsExactly("pager", "archive", "alerts");
  }

  @Test
  void conditionsAreConjunctive() {
    var blockedNetwork =
        TestEvents.event("b")
            .category(EventCategory.NETWORK)
            .outcome(EventOutcome.BLOCKED)
            .severity(Severity.LOW)
            .build();
    assertThat(router.route(blockedNetwork)).containsExactly("archive");
  }

  @Test
  void noMatchMeansUnrouted() {
    Router narrow =
        new Router(
            List.of(
                new RoutingRule(
                    "x", new EventMatcher(Severity.CRITICAL, Set.of(), Set.of()), List.of("s"))));
    assertThat(narrow.route(TestEvents.event("c").build())).isEmpty();
  }
}
