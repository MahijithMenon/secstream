package com.mahijith.secstream.processing;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.databind.JsonNode;
import com.mahijith.secstream.model.InvalidEventException;
import java.time.DateTimeException;
import java.time.Instant;
import org.jspecify.annotations.Nullable;

/**
 * Strict field accessors over a parsed JSON object. Type mismatches are errors, never coercions:
 * {@code "port": "443"} is rejected rather than silently converted.
 */
final class JsonFields {
  private final JsonNode node;

  private JsonFields(JsonNode node) {
    this.node = node;
  }

  static JsonFields parseObject(String payload) {
    JsonNode node;
    try {
      node = Json.MAPPER.readTree(payload);
    } catch (JacksonException e) {
      // Jackson messages can echo payload fragments; keep only the location.
      throw new InvalidEventException("payload is not valid JSON at " + location(e));
    }
    if (node == null || !node.isObject()) {
      throw new InvalidEventException("payload must be a JSON object");
    }
    return new JsonFields(node);
  }

  String requiredText(String field) {
    String value = optionalText(field);
    if (value == null) {
      throw new InvalidEventException("missing required field '" + field + "'");
    }
    return value;
  }

  @Nullable String optionalText(String field) {
    JsonNode value = node.get(field);
    if (value == null || value.isNull()) {
      return null;
    }
    if (!value.isTextual()) {
      throw new InvalidEventException("field '" + field + "' must be a string");
    }
    return value.textValue();
  }

  long requiredLong(String field) {
    Long value = optionalLong(field);
    if (value == null) {
      throw new InvalidEventException("missing required field '" + field + "'");
    }
    return value;
  }

  @Nullable Long optionalLong(String field) {
    JsonNode value = node.get(field);
    if (value == null || value.isNull()) {
      return null;
    }
    if (!value.isIntegralNumber() || !value.canConvertToLong()) {
      throw new InvalidEventException("field '" + field + "' must be an integer");
    }
    return value.longValue();
  }

  Instant requiredIsoInstant(String field) {
    String text = requiredText(field);
    try {
      return Instant.parse(text);
    } catch (DateTimeException e) {
      throw new InvalidEventException("field '" + field + "' must be an ISO-8601 instant", e);
    }
  }

  Instant requiredEpochMillis(String field) {
    long millis = requiredLong(field);
    try {
      return Instant.ofEpochMilli(millis);
    } catch (DateTimeException e) {
      throw new InvalidEventException("field '" + field + "' is out of range", e);
    }
  }

  private static String location(JacksonException e) {
    var loc = e.getLocation();
    return loc == null
        ? "unknown position"
        : "line " + loc.getLineNr() + " column " + loc.getColumnNr();
  }
}
