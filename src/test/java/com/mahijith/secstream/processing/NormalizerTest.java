package com.mahijith.secstream.processing;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.mahijith.secstream.model.EventCategory;
import com.mahijith.secstream.model.EventFormat;
import com.mahijith.secstream.model.EventOutcome;
import com.mahijith.secstream.model.InvalidEventException;
import com.mahijith.secstream.model.SecurityEvent;
import com.mahijith.secstream.model.Severity;
import com.mahijith.secstream.testing.TestEvents;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class NormalizerTest {
  private final SignInNormalizer signIn = new SignInNormalizer();
  private final FirewallNormalizer firewall = new FirewallNormalizer();

  @Test
  void normalizesSignIn() {
    SecurityEvent e =
        signIn.normalize(TestEvents.raw(EventFormat.SIGN_IN, TestEvents.signIn("e1", "FAILURE")));
    assertThat(e.category()).isEqualTo(EventCategory.AUTHENTICATION);
    assertThat(e.outcome()).isEqualTo(EventOutcome.FAILURE);
    assertThat(e.severity()).isEqualTo(Severity.MEDIUM);
    assertThat(e.userId()).isEqualTo("alice");
    assertThat(e.sourceIp().value()).isEqualTo("198.51.100.10");
    assertThat(e.sourceEventId()).isEqualTo("e1");
    assertThat(e.attributes()).containsEntry("signin.country", "IN");
    assertThat(e.destinationIp()).isNull();
  }

  @Test
  void signInSeverityEscalatesOnAttackSignals() {
    assertThat(SignInNormalizer.severityOf(EventOutcome.SUCCESS, null)).isEqualTo(Severity.INFO);
    assertThat(SignInNormalizer.severityOf(EventOutcome.FAILURE, "BAD_PASSWORD"))
        .isEqualTo(Severity.MEDIUM);
    assertThat(SignInNormalizer.severityOf(EventOutcome.FAILURE, "ACCOUNT_LOCKED"))
        .isEqualTo(Severity.HIGH);
  }

  @Test
  void normalizesFirewallWithDifferentShape() {
    SecurityEvent e =
        firewall.normalize(
            TestEvents.raw(EventFormat.FIREWALL, TestEvents.firewall("f1", 3389, "DENY")));
    assertThat(e.category()).isEqualTo(EventCategory.NETWORK);
    assertThat(e.outcome()).isEqualTo(EventOutcome.BLOCKED);
    assertThat(e.severity()).isEqualTo(Severity.HIGH);
    assertThat(e.destinationPort()).isEqualTo(3389);
    assertThat(e.userId()).isNull();
    assertThat(e.attributes())
        .containsEntry("network.protocol", "TCP")
        .containsEntry("firewall.ruleId", "r1");
  }

  @Test
  void idUsesUpstreamIdWhenPresentOtherwiseContent() {
    String withId = TestEvents.signIn("same", "SUCCESS");
    String sameIdDifferentContent = withId.replace("alice", "bob");
    assertThat(norm(withId).id()).isEqualTo(norm(sameIdDifferentContent).id());

    String noId =
        "{\"timestamp\":\"2026-10-01T11:00:00Z\",\"userId\":\"u\",\"ip\":\"10.0.0.1\",\"result\":\"SUCCESS\",\"app\":\"a\"}";
    String reformatted = noId.replace(",", " , ");
    assertThat(norm(noId).id()).isEqualTo(norm(reformatted).id());
    assertThat(norm(noId).id()).isNotEqualTo(norm(noId.replace("\"u\"", "\"v\"")).id());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "",
        "not json",
        "[]",
        "\"string\"",
        "{\"userId\":\"a\"}",
        "{\"timestamp\":\"2026-10-01T11:00:00Z\",\"userId\":\"u\",\"ip\":\"10.0.0.1\",\"result\":\"MAYBE\",\"app\":\"a\"}",
        "{\"timestamp\":\"yesterday\",\"userId\":\"u\",\"ip\":\"10.0.0.1\",\"result\":\"SUCCESS\",\"app\":\"a\"}",
        "{\"timestamp\":\"2026-10-01T11:00:00Z\",\"userId\":7,\"ip\":\"10.0.0.1\",\"result\":\"SUCCESS\",\"app\":\"a\"}",
        "{\"timestamp\":\"2026-10-01T11:00:00Z\",\"userId\":\"u\",\"ip\":\"host.example\",\"result\":\"SUCCESS\",\"app\":\"a\"}",
        "{\"timestamp\":\"2026-10-01T11:00:00Z\",\"userId\":\"u\",\"userId\":\"v\",\"ip\":\"10.0.0.1\",\"result\":\"SUCCESS\",\"app\":\"a\"}",
        "{\"timestamp\":\"2026-10-01T11:00:00Z\",\"userId\":\"u\",\"ip\":\"10.0.0.1\",\"result\":\"SUCCESS\",\"app\":\"a\"} {}",
        "{\"timestamp\":\"2026-10-01T13:00:00Z\",\"userId\":\"u\",\"ip\":\"10.0.0.1\",\"result\":\"SUCCESS\",\"app\":\"a\"}"
      })
  void rejectsMalformedSignIns(String payload) {
    assertThatThrownBy(() -> norm(payload)).isInstanceOf(InvalidEventException.class);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "{\"ts\":1790000000000,\"src_ip\":\"1.1.1.1\",\"dst_ip\":\"10.0.0.1\",\"dst_port\":\"22\",\"proto\":\"TCP\",\"action\":\"DENY\"}",
        "{\"ts\":1790000000000,\"src_ip\":\"1.1.1.1\",\"dst_ip\":\"10.0.0.1\",\"dst_port\":70000,\"proto\":\"TCP\",\"action\":\"DENY\"}",
        "{\"ts\":1790000000000,\"src_ip\":\"1.1.1.1\",\"dst_ip\":\"10.0.0.1\",\"dst_port\":22,\"proto\":\"SCTP\",\"action\":\"DENY\"}",
        "{\"ts\":1790000000000,\"src_ip\":\"1.1.1.1\",\"dst_ip\":\"10.0.0.1\",\"dst_port\":22,\"proto\":\"TCP\",\"action\":\"DROP\"}",
        "{\"ts\":1.5,\"src_ip\":\"1.1.1.1\",\"dst_ip\":\"10.0.0.1\",\"dst_port\":22,\"proto\":\"TCP\",\"action\":\"DENY\"}",
        "{\"ts\":1790000000000,\"src_ip\":\"1.1.1.1\",\"dst_ip\":\"10.0.0.1\",\"dst_port\":22,\"proto\":\"TCP\",\"action\":\"DENY\",\"bytes\":-1}"
      })
  void rejectsMalformedFirewallEvents(String payload) {
    assertThatThrownBy(() -> firewall.normalize(TestEvents.raw(EventFormat.FIREWALL, payload)))
        .isInstanceOf(InvalidEventException.class);
  }

  @Test
  void rejectsDeeplyNestedPayloadWithoutStackOverflow() {
    String deep = "{\"a\":".repeat(10_000) + "1" + "}".repeat(10_000);
    assertThatThrownBy(() -> norm(deep)).isInstanceOf(InvalidEventException.class);
  }

  @Test
  void errorMessagesDoNotEchoPayloadValues() {
    String secret =
        "{\"timestamp\":\"2026-10-01T11:00:00Z\",\"userId\":\"u\",\"ip\":\"10.0.0.1\",\"result\":\"SECRET-VALUE\",\"app\":\"a\"}";
    assertThatThrownBy(() -> norm(secret)).message().doesNotContain("SECRET-VALUE");
    assertThatThrownBy(() -> norm("{\"x\": SECRET-VALUE}"))
        .message()
        .doesNotContain("SECRET-VALUE");
  }

  private SecurityEvent norm(String payload) {
    return signIn.normalize(TestEvents.raw(EventFormat.SIGN_IN, payload));
  }
}
