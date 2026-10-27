package com.mahijith.secstream.model;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.regex.Pattern;
import org.jspecify.annotations.Nullable;

/**
 * A validated IPv4 or IPv6 literal in canonical form.
 *
 * <p>{@link InetAddress#getByName} performs a DNS lookup when given a host name, which would let an
 * attacker make the pipeline issue DNS queries for arbitrary names. We therefore only hand it
 * strings that are syntactically IP literals, for which the JDK never consults DNS.
 */
public record IpAddress(String value) {
  private static final Pattern IPV4 =
      Pattern.compile(
          "^((25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)\\.){3}(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)$");
  private static final Pattern IPV6_CHARS = Pattern.compile("^[0-9A-Fa-f:.]+$");
  private static final int MAX_LITERAL_LENGTH = 45;

  public IpAddress {
    if (value == null || value.isEmpty()) {
      throw new InvalidEventException("ip address must not be empty");
    }
  }

  public static IpAddress parse(String literal) {
    if (literal == null || literal.isEmpty() || literal.length() > MAX_LITERAL_LENGTH) {
      throw new InvalidEventException("ip address is empty or too long");
    }
    boolean ipv4 = IPV4.matcher(literal).matches();
    boolean ipv6 = !ipv4 && literal.indexOf(':') >= 0 && IPV6_CHARS.matcher(literal).matches();
    if (!ipv4 && !ipv6) {
      throw new InvalidEventException("ip address is not an IPv4 or IPv6 literal");
    }
    try {
      return new IpAddress(InetAddress.getByName(literal).getHostAddress());
    } catch (UnknownHostException e) {
      throw new InvalidEventException("ip address is not a valid literal", e);
    }
  }

  /** The literal, or null when absent; saves every serializer a null check. */
  public static @Nullable String valueOf(@Nullable IpAddress ip) {
    return ip == null ? null : ip.value;
  }

  public InetAddress toInetAddress() {
    try {
      return InetAddress.getByName(value);
    } catch (UnknownHostException e) {
      throw new IllegalStateException("canonical literal failed to parse: " + value, e);
    }
  }

  @Override
  public String toString() {
    return value;
  }
}
