package com.mahijith.secstream.processing;

import com.mahijith.secstream.model.EventCategory;
import com.mahijith.secstream.model.EventFormat;
import com.mahijith.secstream.model.EventId;
import com.mahijith.secstream.model.EventOutcome;
import com.mahijith.secstream.model.InvalidEventException;
import com.mahijith.secstream.model.IpAddress;
import com.mahijith.secstream.model.RawEvent;
import com.mahijith.secstream.model.SecurityEvent;
import com.mahijith.secstream.model.Severity;
import java.time.Instant;
import java.util.Locale;
import java.util.Set;

/**
 * Firewall log format (deliberately a different shape from sign-in logs):
 *
 * <pre>{"id"?, "ts": epoch-millis, "src_ip", "dst_ip", "dst_port", "proto": "TCP|UDP|ICMP",
 * "action": "ALLOW|DENY", "rule_id"?, "bytes"?}</pre>
 *
 * <p>Severity: allowed traffic is INFO; denied traffic is LOW, or HIGH when it targets a port
 * commonly used for remote administration.
 */
public final class FirewallNormalizer implements EventNormalizer {
  static final Set<Integer> SENSITIVE_PORTS = Set.of(22, 23, 3389, 5900);
  private static final Set<String> PROTOCOLS = Set.of("TCP", "UDP", "ICMP");

  @Override
  public EventFormat format() {
    return EventFormat.FIREWALL;
  }

  @Override
  public SecurityEvent normalize(RawEvent raw) {
    JsonFields json = JsonFields.parseObject(raw.payload());

    String sourceEventId = json.optionalText("id");
    Instant occurredAt = TimestampPolicy.check(json.requiredEpochMillis("ts"), raw.receivedAt());
    IpAddress src = IpAddress.parse(json.requiredText("src_ip"));
    IpAddress dst = IpAddress.parse(json.requiredText("dst_ip"));
    long port = json.requiredLong("dst_port");
    if (port < 0 || port > 65_535) {
      throw new InvalidEventException("field 'dst_port' must be within 0..65535");
    }
    String proto = json.requiredText("proto").toUpperCase(Locale.ROOT);
    if (!PROTOCOLS.contains(proto)) {
      throw new InvalidEventException("field 'proto' must be one of " + PROTOCOLS);
    }
    EventOutcome outcome =
        switch (json.requiredText("action")) {
          case "ALLOW" -> EventOutcome.ALLOWED;
          case "DENY" -> EventOutcome.BLOCKED;
          default -> throw new InvalidEventException("field 'action' must be ALLOW or DENY");
        };
    String ruleId = json.optionalText("rule_id");
    Long bytes = json.optionalLong("bytes");
    if (bytes != null && bytes < 0) {
      throw new InvalidEventException("field 'bytes' must not be negative");
    }

    String identity =
        sourceEventId != null
            ? "id:" + sourceEventId
            : String.join(
                "|",
                "c",
                occurredAt.toString(),
                src.value(),
                dst.value(),
                Long.toString(port),
                proto,
                outcome.name());

    SecurityEvent.Builder event =
        SecurityEvent.builder()
            .id(EventId.derive(format(), identity))
            .sourceEventId(sourceEventId)
            .source(raw.sourceName())
            .format(format())
            .category(EventCategory.NETWORK)
            .outcome(outcome)
            .severity(severityOf(outcome, (int) port))
            .occurredAt(occurredAt)
            .receivedAt(raw.receivedAt())
            .sourceIp(src)
            .destinationIp(dst)
            .destinationPort((int) port)
            .attribute("network.protocol", proto);
    if (ruleId != null) {
      event.attribute("firewall.ruleId", ruleId);
    }
    if (bytes != null) {
      event.attribute("network.bytes", Long.toString(bytes));
    }
    return event.build();
  }

  static Severity severityOf(EventOutcome outcome, int port) {
    if (outcome == EventOutcome.ALLOWED) {
      return Severity.INFO;
    }
    return SENSITIVE_PORTS.contains(port) ? Severity.HIGH : Severity.LOW;
  }
}
