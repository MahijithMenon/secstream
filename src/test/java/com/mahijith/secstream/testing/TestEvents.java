package com.mahijith.secstream.testing;

import com.mahijith.secstream.model.EventCategory;
import com.mahijith.secstream.model.EventFormat;
import com.mahijith.secstream.model.EventId;
import com.mahijith.secstream.model.EventOutcome;
import com.mahijith.secstream.model.IpAddress;
import com.mahijith.secstream.model.RawEvent;
import com.mahijith.secstream.model.SecurityEvent;
import com.mahijith.secstream.model.Severity;
import java.time.Instant;

/** Synthetic fixtures. No real user data. */
public final class TestEvents {
  public static final Instant NOW = Instant.parse("2026-10-01T12:00:00Z");

  private TestEvents() {}

  public static SecurityEvent.Builder event(String key) {
    return SecurityEvent.builder()
        .id(EventId.derive(EventFormat.SIGN_IN, key))
        .source("test")
        .format(EventFormat.SIGN_IN)
        .category(EventCategory.AUTHENTICATION)
        .outcome(EventOutcome.FAILURE)
        .severity(Severity.MEDIUM)
        .occurredAt(NOW)
        .receivedAt(NOW)
        .userId("user-" + key)
        .application("mail")
        .sourceIp(IpAddress.parse("198.51.100.7"));
  }

  public static RawEvent raw(EventFormat format, String payload) {
    return new RawEvent(1, "test", format, payload, NOW, System.nanoTime());
  }

  public static String signIn(String eventId, String result) {
    return """
        {"eventId":"%s","timestamp":"2026-10-01T11:59:00Z","userId":"alice","ip":"198.51.100.10",\
        "country":"IN","result":"%s","app":"mail","deviceId":"dev-1"}"""
        .formatted(eventId, result);
  }

  public static String firewall(String id, int port, String action) {
    return """
        {"id":"%s","ts":1790000000000,"src_ip":"203.0.113.50","dst_ip":"10.0.0.5","dst_port":%d,\
        "proto":"TCP","action":"%s","rule_id":"r1","bytes":10}"""
        .formatted(id, port, action);
  }
}
