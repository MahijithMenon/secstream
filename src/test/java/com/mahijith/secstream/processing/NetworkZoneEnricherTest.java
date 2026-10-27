package com.mahijith.secstream.processing;

import static org.assertj.core.api.Assertions.assertThat;

import com.mahijith.secstream.model.IpAddress;
import com.mahijith.secstream.testing.TestEvents;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class NetworkZoneEnricherTest {
  @ParameterizedTest
  @CsvSource({
    "10.1.2.3, internal",
    "192.168.0.1, internal",
    "172.16.5.4, internal",
    "127.0.0.1, loopback",
    "169.254.1.1, link-local",
    "198.51.100.7, external",
    "fd00::1, internal",
    "::1, loopback",
    "2001:db8::1, external"
  })
  void classifiesAddresses(String ip, String zone) {
    var event = TestEvents.event("e").sourceIp(IpAddress.parse(ip)).build();
    assertThat(new NetworkZoneEnricher().enrich(event).attributes())
        .containsEntry(NetworkZoneEnricher.ATTRIBUTE, zone);
  }
}
