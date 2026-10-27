package com.mahijith.secstream.source;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mahijith.secstream.model.EventFormat;
import com.mahijith.secstream.processing.Json;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.SplittableRandom;

/**
 * Deterministic generator of raw sign-in and firewall payloads. Not thread-safe: one instance per
 * source thread. All data is synthetic; addresses come from the RFC 5737 documentation ranges and
 * RFC 1918 private ranges.
 */
public final class SyntheticEventGenerator {
  private static final String[] APPS = {"mail", "portal", "vpn", "hr", "billing"};
  private static final String[] COUNTRIES = {"IN", "US", "GB", "DE", "SG"};
  private static final String[] FAILURE_REASONS = {
    "BAD_PASSWORD", "BAD_PASSWORD", "BAD_PASSWORD", "ACCOUNT_LOCKED", "MFA_DENIED"
  };
  private static final int[] PORTS = {22, 53, 80, 443, 3389, 5432, 8080};
  private static final int DUPLICATE_RING = 1024;

  /** A generated payload and its wire format. */
  public record Generated(EventFormat format, String payload) {}

  private final GeneratorSettings settings;
  private final SplittableRandom random;
  private final Instant baseTime;
  private final Generated[] recent = new Generated[DUPLICATE_RING];
  private long produced;

  public SyntheticEventGenerator(GeneratorSettings settings, Instant now) {
    this.settings = settings;
    this.random = new SplittableRandom(settings.seed());
    // Timestamps run up to "now" rather than past it, which the normalizers would reject.
    this.baseTime = now.minusMillis(settings.count());
  }

  public Generated next() {
    long n = produced++;
    double roll = random.nextDouble();
    if (roll < settings.malformedFraction()) {
      return malformed();
    }
    if (roll < settings.malformedFraction() + settings.duplicateFraction() && n > 0) {
      Generated earlier = recent[random.nextInt((int) Math.min(n, DUPLICATE_RING))];
      if (earlier != null) {
        return earlier;
      }
    }
    Generated g = random.nextDouble() < settings.firewallFraction() ? firewall(n) : signIn(n);
    recent[(int) (n % DUPLICATE_RING)] = g;
    return g;
  }

  private Generated signIn(long n) {
    ObjectNode o = Json.MAPPER.createObjectNode();
    boolean failed = random.nextDouble() < settings.failureFraction();
    o.put("eventId", "si-" + settings.seed() + "-" + n);
    o.put("timestamp", baseTime.plusMillis(n).toString());
    o.put("userId", "user" + random.nextInt(5_000));
    o.put("ip", "198.51.100." + random.nextInt(1, 255));
    o.put("country", COUNTRIES[random.nextInt(COUNTRIES.length)]);
    o.put("result", failed ? "FAILURE" : "SUCCESS");
    o.put("app", APPS[random.nextInt(APPS.length)]);
    o.put("deviceId", "dev-" + random.nextInt(10_000));
    if (failed) {
      o.put("failureReason", FAILURE_REASONS[random.nextInt(FAILURE_REASONS.length)]);
    }
    return new Generated(EventFormat.SIGN_IN, write(o));
  }

  private Generated firewall(long n) {
    ObjectNode o = Json.MAPPER.createObjectNode();
    boolean denied = random.nextDouble() < settings.failureFraction();
    o.put("id", "fw-" + settings.seed() + "-" + n);
    o.put("ts", baseTime.plusMillis(n).toEpochMilli());
    o.put("src_ip", "203.0.113." + random.nextInt(1, 255));
    o.put("dst_ip", "10.0." + random.nextInt(0, 255) + "." + random.nextInt(1, 255));
    o.put("dst_port", PORTS[random.nextInt(PORTS.length)]);
    o.put("proto", random.nextInt(10) < 8 ? "TCP" : "UDP");
    o.put("action", denied ? "DENY" : "ALLOW");
    o.put("rule_id", "r" + random.nextInt(50));
    o.put("bytes", random.nextInt(0, 1_000_000));
    return new Generated(EventFormat.FIREWALL, write(o));
  }

  private Generated malformed() {
    return switch (random.nextInt(3)) {
      case 0 -> new Generated(EventFormat.SIGN_IN, "{\"userId\": \"truncated");
      case 1 ->
          new Generated(
              EventFormat.SIGN_IN,
              "{\"timestamp\":\"not-a-time\",\"userId\":\"u\",\"ip\":\"198.51.100.1\",\"result\":\"SUCCESS\",\"app\":\"mail\"}");
      default ->
          new Generated(
              EventFormat.FIREWALL,
              "{\"ts\":1,\"src_ip\":\"999.1.1.1\",\"dst_ip\":\"10.0.0.1\",\"dst_port\":22,\"proto\":\"TCP\",\"action\":\"DENY\"}");
    };
  }

  private static String write(ObjectNode o) {
    try {
      return Json.MAPPER.writeValueAsString(o);
    } catch (JsonProcessingException e) {
      throw new UncheckedIOException(e);
    }
  }
}
