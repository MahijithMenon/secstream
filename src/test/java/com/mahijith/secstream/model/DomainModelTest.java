package com.mahijith.secstream.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.mahijith.secstream.testing.TestEvents;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class DomainModelTest {

  @Test
  void severityOrderingAndParsing() {
    assertThat(Severity.HIGH.isAtLeast(Severity.MEDIUM)).isTrue();
    assertThat(Severity.MEDIUM.isAtLeast(Severity.MEDIUM)).isTrue();
    assertThat(Severity.LOW.isAtLeast(Severity.MEDIUM)).isFalse();
    assertThat(Severity.parse(" critical ")).isEqualTo(Severity.CRITICAL);
    assertThatThrownBy(() -> Severity.parse("urgent")).hasMessageContaining("expected one of");
  }

  @Test
  void eventFormatConfigNames() {
    assertThat(EventFormat.fromConfigName("SignIn")).isEqualTo(EventFormat.SIGN_IN);
    assertThatThrownBy(() -> EventFormat.fromConfigName("syslog")).hasMessageContaining("signin");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {"198.51.100.7", "10.0.0.1", "0.0.0.0", "::1", "2001:db8::1", "::ffff:10.0.0.1"})
  void acceptsIpLiterals(String literal) {
    assertThat(IpAddress.parse(literal).value()).isNotBlank();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "",
        "localhost",
        "example.com",
        "256.1.1.1",
        "1.2.3",
        "01.2.3.4",
        "1.2.3.4 ",
        "gggg::1",
        "1.2.3.4/24"
      })
  void rejectsNonLiteralsWithoutDnsLookup(String literal) {
    assertThatThrownBy(() -> IpAddress.parse(literal)).isInstanceOf(InvalidEventException.class);
  }

  @Test
  void ipv6IsCanonicalised() {
    assertThat(IpAddress.parse("2001:0db8:0000:0000:0000:0000:0000:0001"))
        .isEqualTo(IpAddress.parse("2001:db8::1"));
  }

  @Test
  void eventIdIsStableAndNamespacedByFormat() {
    EventId a = EventId.derive(EventFormat.SIGN_IN, "id:42");
    assertThat(EventId.derive(EventFormat.SIGN_IN, "id:42")).isEqualTo(a);
    assertThat(EventId.derive(EventFormat.FIREWALL, "id:42")).isNotEqualTo(a);
    assertThat(a.value()).hasSize(32).matches("[0-9a-f]+");
    assertThatThrownBy(() -> new EventId("XYZ")).isInstanceOf(InvalidEventException.class);
  }

  @Test
  void securityEventRequiresMandatoryFields() {
    assertThatThrownBy(() -> TestEvents.event("a").id(null).build())
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> TestEvents.event("a").source(" ").build())
        .isInstanceOf(InvalidEventException.class)
        .hasMessageContaining("source");
  }

  @Test
  void securityEventRejectsControlCharactersAndOversizedText() {
    assertThatThrownBy(() -> TestEvents.event("a").userId("bob\nlevel=ERROR").build())
        .hasMessageContaining("control characters");
    assertThatThrownBy(
            () -> TestEvents.event("a").userId("x".repeat(EventLimits.MAX_TEXT_LENGTH + 1)).build())
        .hasMessageContaining("exceeds");
    assertThatThrownBy(() -> TestEvents.event("a").destinationPort(70_000).build())
        .hasMessageContaining("destinationPort");
  }

  @Test
  void attributesAreBoundedAndImmutable() {
    Map<String, String> many = new HashMap<>();
    for (int i = 0; i <= EventLimits.MAX_ATTRIBUTES; i++) {
      many.put("k" + i, "v");
    }
    assertThatThrownBy(() -> TestEvents.event("a").attributes(many).build())
        .hasMessageContaining("attributes");

    SecurityEvent e = TestEvents.event("a").attribute("k", "v").build();
    assertThatThrownBy(() -> e.attributes().put("x", "y"))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void withAttributeReturnsModifiedCopy() {
    SecurityEvent original = TestEvents.event("a").build();
    SecurityEvent enriched = original.withAttribute("zone", "external");
    assertThat(original.attributes()).isEmpty();
    assertThat(enriched.attributes()).containsEntry("zone", "external");
    assertThat(enriched.id()).isEqualTo(original.id());
  }
}
