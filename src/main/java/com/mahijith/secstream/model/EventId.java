package com.mahijith.secstream.model;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * Stable identity of a logical security event, used for deduplication and idempotent sink writes.
 *
 * <p>The id is a SHA-256 over the event's format plus either the upstream event id (when the source
 * provides one) or a canonical rendering of the normalized fields. The same upstream event
 * therefore gets the same id whether it arrives via file, HTTP, or a client retry, but ids from
 * different formats can never collide by accident.
 */
public record EventId(String value) {
  private static final int HEX_LENGTH = 32;

  public EventId {
    if (value == null || value.length() != HEX_LENGTH || !value.chars().allMatch(EventId::isHex)) {
      throw new InvalidEventException("event id must be " + HEX_LENGTH + " lowercase hex chars");
    }
  }

  public static EventId derive(EventFormat format, String identityKey) {
    MessageDigest sha256;
    try {
      sha256 = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is mandatory on every Java platform", e);
    }
    sha256.update(format.configName().getBytes(StandardCharsets.UTF_8));
    sha256.update((byte) 0);
    byte[] digest = sha256.digest(identityKey.getBytes(StandardCharsets.UTF_8));
    // 128 bits is ample for collision resistance at pipeline scale and keeps ids compact.
    return new EventId(HexFormat.of().formatHex(digest, 0, HEX_LENGTH / 2));
  }

  private static boolean isHex(int c) {
    return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
  }

  @Override
  public String toString() {
    return value;
  }
}
