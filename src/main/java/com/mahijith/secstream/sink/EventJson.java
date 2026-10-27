package com.mahijith.secstream.sink;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mahijith.secstream.model.IpAddress;
import com.mahijith.secstream.model.SecurityEvent;
import com.mahijith.secstream.processing.Json;
import java.io.UncheckedIOException;
import org.jspecify.annotations.Nullable;

/**
 * The output wire format for normalized events. Written by hand rather than by reflecting over the
 * record so that renaming a Java field can never silently change what downstream consumers read.
 */
public final class EventJson {
  private EventJson() {}

  public static ObjectNode toNode(SecurityEvent e) {
    ObjectNode n = Json.MAPPER.createObjectNode();
    n.put("id", e.id().value());
    putIfPresent(n, "sourceEventId", e.sourceEventId());
    n.put("source", e.source());
    n.put("format", e.format().configName());
    n.put("category", e.category().name());
    n.put("outcome", e.outcome().name());
    n.put("severity", e.severity().name());
    n.put("occurredAt", e.occurredAt().toString());
    n.put("receivedAt", e.receivedAt().toString());
    putIfPresent(n, "userId", e.userId());
    putIfPresent(n, "deviceId", e.deviceId());
    putIfPresent(n, "application", e.application());
    putIfPresent(n, "sourceIp", IpAddress.valueOf(e.sourceIp()));
    putIfPresent(n, "destinationIp", IpAddress.valueOf(e.destinationIp()));
    if (e.destinationPort() != null) {
      n.put("destinationPort", e.destinationPort());
    }
    ObjectNode attributes = n.putObject("attributes");
    e.attributes().forEach(attributes::put);
    return n;
  }

  public static String toJsonLine(SecurityEvent e) {
    try {
      return Json.MAPPER.writeValueAsString(toNode(e));
    } catch (JsonProcessingException ex) {
      throw new UncheckedIOException(ex);
    }
  }

  private static void putIfPresent(ObjectNode n, String field, @Nullable String value) {
    if (value != null) {
      n.put(field, value);
    }
  }
}
