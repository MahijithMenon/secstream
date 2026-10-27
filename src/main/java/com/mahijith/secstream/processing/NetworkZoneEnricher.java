package com.mahijith.secstream.processing;

import com.mahijith.secstream.model.IpAddress;
import com.mahijith.secstream.model.SecurityEvent;
import java.net.InetAddress;

/**
 * Tags the source address as internal, loopback, link-local or external using only JDK address
 * classification. A real deployment would replace this with a local GeoIP/asset database.
 */
public final class NetworkZoneEnricher implements EventEnricher {
  public static final String ATTRIBUTE = "network.sourceZone";

  @Override
  public SecurityEvent enrich(SecurityEvent event) {
    IpAddress ip = event.sourceIp();
    if (ip == null) {
      return event;
    }
    return event.withAttribute(ATTRIBUTE, zoneOf(ip.toInetAddress()));
  }

  static String zoneOf(InetAddress address) {
    if (address.isLoopbackAddress()) {
      return "loopback";
    }
    if (address.isLinkLocalAddress()) {
      return "link-local";
    }
    if (address.isSiteLocalAddress() || isUniqueLocalV6(address)) {
      return "internal";
    }
    return "external";
  }

  /** fc00::/7; the JDK's isSiteLocalAddress only knows the deprecated fec0::/10 range. */
  private static boolean isUniqueLocalV6(InetAddress address) {
    byte[] bytes = address.getAddress();
    return bytes.length == 16 && (bytes[0] & 0xfe) == 0xfc;
  }
}
