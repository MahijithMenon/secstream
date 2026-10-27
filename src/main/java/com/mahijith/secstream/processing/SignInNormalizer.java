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

/**
 * Sign-in log format:
 *
 * <pre>{"eventId"?, "timestamp": ISO-8601, "userId", "ip", "country"?, "result": "SUCCESS|FAILURE",
 * "app", "deviceId"?, "failureReason"?}</pre>
 *
 * <p>Severity: success is INFO; failure is MEDIUM, raised to HIGH when the failure reason indicates
 * an attack signal (locked account, MFA rejected).
 */
public final class SignInNormalizer implements EventNormalizer {

  @Override
  public EventFormat format() {
    return EventFormat.SIGN_IN;
  }

  @Override
  public SecurityEvent normalize(RawEvent raw) {
    JsonFields json = JsonFields.parseObject(raw.payload());

    String sourceEventId = json.optionalText("eventId");
    Instant occurredAt =
        TimestampPolicy.check(json.requiredIsoInstant("timestamp"), raw.receivedAt());
    String userId = json.requiredText("userId");
    IpAddress ip = IpAddress.parse(json.requiredText("ip"));
    String app = json.requiredText("app");
    String deviceId = json.optionalText("deviceId");
    String country = json.optionalText("country");
    String failureReason = json.optionalText("failureReason");
    EventOutcome outcome =
        switch (json.requiredText("result")) {
          case "SUCCESS" -> EventOutcome.SUCCESS;
          case "FAILURE" -> EventOutcome.FAILURE;
          default -> throw new InvalidEventException("field 'result' must be SUCCESS or FAILURE");
        };

    String identity =
        sourceEventId != null
            ? "id:" + sourceEventId
            : String.join("|", "c", occurredAt.toString(), userId, ip.value(), app, outcome.name());

    SecurityEvent.Builder event =
        SecurityEvent.builder()
            .id(EventId.derive(format(), identity))
            .sourceEventId(sourceEventId)
            .source(raw.sourceName())
            .format(format())
            .category(EventCategory.AUTHENTICATION)
            .outcome(outcome)
            .severity(severityOf(outcome, failureReason))
            .occurredAt(occurredAt)
            .receivedAt(raw.receivedAt())
            .userId(userId)
            .deviceId(deviceId)
            .application(app)
            .sourceIp(ip);
    if (country != null) {
      event.attribute("signin.country", country);
    }
    if (failureReason != null) {
      event.attribute("signin.failureReason", failureReason);
    }
    return event.build();
  }

  static Severity severityOf(EventOutcome outcome, String failureReason) {
    if (outcome == EventOutcome.SUCCESS) {
      return Severity.INFO;
    }
    if ("ACCOUNT_LOCKED".equals(failureReason) || "MFA_DENIED".equals(failureReason)) {
      return Severity.HIGH;
    }
    return Severity.MEDIUM;
  }
}
