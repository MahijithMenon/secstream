package com.mahijith.secstream.model;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * The normalized, immutable event every pipeline stage after the normalizer works with.
 *
 * <p>Common security attributes are typed fields. Anything format-specific that is still useful
 * downstream (firewall rule id, sign-in country, byte counts) goes into {@link #attributes}, which
 * is bounded and immutable so a hostile source cannot make one event arbitrarily large.
 *
 * <p>Fields that not every format provides are {@code @Nullable}; there are no "empty string means
 * absent" conventions.
 */
public record SecurityEvent(
    EventId id,
    @Nullable String sourceEventId,
    String source,
    EventFormat format,
    EventCategory category,
    EventOutcome outcome,
    Severity severity,
    Instant occurredAt,
    Instant receivedAt,
    @Nullable String userId,
    @Nullable String deviceId,
    @Nullable String application,
    @Nullable IpAddress sourceIp,
    @Nullable IpAddress destinationIp,
    @Nullable Integer destinationPort,
    Map<String, String> attributes) {

  public SecurityEvent {
    Objects.requireNonNull(id, "id");
    requireText("source", source);
    Objects.requireNonNull(format, "format");
    Objects.requireNonNull(category, "category");
    Objects.requireNonNull(outcome, "outcome");
    Objects.requireNonNull(severity, "severity");
    Objects.requireNonNull(occurredAt, "occurredAt");
    Objects.requireNonNull(receivedAt, "receivedAt");
    optionalText("sourceEventId", sourceEventId);
    optionalText("userId", userId);
    optionalText("deviceId", deviceId);
    optionalText("application", application);
    if (destinationPort != null && (destinationPort < 0 || destinationPort > 65_535)) {
      throw new InvalidEventException("destinationPort must be within 0..65535");
    }
    attributes = validatedAttributes(attributes);
  }

  public static Builder builder() {
    return new Builder();
  }

  public Builder toBuilder() {
    return new Builder(this);
  }

  /** Returns a copy with one attribute added or replaced; used by enrichers. */
  public SecurityEvent withAttribute(String key, String value) {
    Map<String, String> merged = new LinkedHashMap<>(attributes);
    merged.put(key, value);
    return toBuilder().attributes(merged).build();
  }

  private static void requireText(String field, @Nullable String value) {
    if (value == null || value.isBlank()) {
      throw new InvalidEventException(field + " is required");
    }
    optionalText(field, value);
  }

  private static void optionalText(String field, @Nullable String value) {
    if (value == null) {
      return;
    }
    if (value.isBlank()) {
      throw new InvalidEventException(field + " must not be blank when present");
    }
    if (value.length() > EventLimits.MAX_TEXT_LENGTH) {
      throw new InvalidEventException(
          field + " exceeds " + EventLimits.MAX_TEXT_LENGTH + " characters");
    }
    if (value.chars().anyMatch(Character::isISOControl)) {
      // Control characters enable log injection (forged newlines) in every text-based sink.
      throw new InvalidEventException(field + " contains control characters");
    }
  }

  private static Map<String, String> validatedAttributes(@Nullable Map<String, String> raw) {
    if (raw == null || raw.isEmpty()) {
      return Map.of();
    }
    if (raw.size() > EventLimits.MAX_ATTRIBUTES) {
      throw new InvalidEventException(
          "more than " + EventLimits.MAX_ATTRIBUTES + " attributes supplied");
    }
    for (Map.Entry<String, String> e : raw.entrySet()) {
      String key = e.getKey();
      if (key == null || key.isBlank() || key.length() > EventLimits.MAX_ATTRIBUTE_KEY_LENGTH) {
        throw new InvalidEventException("attribute key is blank or too long");
      }
      if (e.getValue() == null
          || e.getValue().length() > EventLimits.MAX_ATTRIBUTE_VALUE_LENGTH
          || e.getValue().chars().anyMatch(Character::isISOControl)) {
        throw new InvalidEventException("attribute '" + key + "' has an invalid value");
      }
    }
    // Map.copyOf is unmodifiable but unordered; a LinkedHashMap copy keeps sink output stable.
    return Collections.unmodifiableMap(new LinkedHashMap<>(raw));
  }

  /** Mutable builder; not thread-safe. Validation happens once, in {@link #build()}. */
  public static final class Builder {
    private @Nullable EventId id;
    private @Nullable String sourceEventId;
    private @Nullable String source;
    private @Nullable EventFormat format;
    private @Nullable EventCategory category;
    private EventOutcome outcome = EventOutcome.UNKNOWN;
    private Severity severity = Severity.INFO;
    private @Nullable Instant occurredAt;
    private @Nullable Instant receivedAt;
    private @Nullable String userId;
    private @Nullable String deviceId;
    private @Nullable String application;
    private @Nullable IpAddress sourceIp;
    private @Nullable IpAddress destinationIp;
    private @Nullable Integer destinationPort;
    private Map<String, String> attributes = new LinkedHashMap<>();

    private Builder() {}

    private Builder(SecurityEvent e) {
      id = e.id;
      sourceEventId = e.sourceEventId;
      source = e.source;
      format = e.format;
      category = e.category;
      outcome = e.outcome;
      severity = e.severity;
      occurredAt = e.occurredAt;
      receivedAt = e.receivedAt;
      userId = e.userId;
      deviceId = e.deviceId;
      application = e.application;
      sourceIp = e.sourceIp;
      destinationIp = e.destinationIp;
      destinationPort = e.destinationPort;
      attributes = new LinkedHashMap<>(e.attributes);
    }

    public Builder id(EventId value) {
      id = value;
      return this;
    }

    public Builder sourceEventId(@Nullable String value) {
      sourceEventId = value;
      return this;
    }

    public Builder source(String value) {
      source = value;
      return this;
    }

    public Builder format(EventFormat value) {
      format = value;
      return this;
    }

    public Builder category(EventCategory value) {
      category = value;
      return this;
    }

    public Builder outcome(EventOutcome value) {
      outcome = value;
      return this;
    }

    public Builder severity(Severity value) {
      severity = value;
      return this;
    }

    public Builder occurredAt(Instant value) {
      occurredAt = value;
      return this;
    }

    public Builder receivedAt(Instant value) {
      receivedAt = value;
      return this;
    }

    public Builder userId(@Nullable String value) {
      userId = value;
      return this;
    }

    public Builder deviceId(@Nullable String value) {
      deviceId = value;
      return this;
    }

    public Builder application(@Nullable String value) {
      application = value;
      return this;
    }

    public Builder sourceIp(@Nullable IpAddress value) {
      sourceIp = value;
      return this;
    }

    public Builder destinationIp(@Nullable IpAddress value) {
      destinationIp = value;
      return this;
    }

    public Builder destinationPort(@Nullable Integer value) {
      destinationPort = value;
      return this;
    }

    public Builder attribute(String key, String value) {
      attributes.put(key, value);
      return this;
    }

    public Builder attributes(Map<String, String> value) {
      attributes = new LinkedHashMap<>(value);
      return this;
    }

    /** Throws {@link InvalidEventException} if a required field is missing or invalid. */
    public SecurityEvent build() {
      return new SecurityEvent(
          id,
          sourceEventId,
          source,
          format,
          category,
          outcome,
          severity,
          occurredAt,
          receivedAt,
          userId,
          deviceId,
          application,
          sourceIp,
          destinationIp,
          destinationPort,
          attributes);
    }
  }
}
