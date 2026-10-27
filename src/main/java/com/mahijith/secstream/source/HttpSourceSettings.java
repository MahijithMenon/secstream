package com.mahijith.secstream.source;

import java.time.Duration;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * @param bindAddress interface to listen on; defaults to loopback so the endpoint is not exposed to
 *     the network unless explicitly configured
 * @param authToken if non-null, requests must carry {@code Authorization: Bearer <token>}
 * @param admissionTimeout how long a request waits for queue space before answering 503
 */
public record HttpSourceSettings(
    String bindAddress,
    int port,
    int maxBodyBytes,
    int maxEventsPerRequest,
    int threads,
    Duration admissionTimeout,
    @Nullable String authToken) {

  public HttpSourceSettings {
    Objects.requireNonNull(bindAddress, "bindAddress");
    Objects.requireNonNull(admissionTimeout, "admissionTimeout");
    if (port < 0 || port > 65_535) {
      throw new IllegalArgumentException("port must be within 0..65535");
    }
    if (maxBodyBytes < 1 || maxEventsPerRequest < 1 || threads < 1) {
      throw new IllegalArgumentException("maxBodyBytes, maxEventsPerRequest, threads must be >= 1");
    }
  }

  @Override
  public String toString() {
    // Never print the token.
    return "HttpSourceSettings[bind="
        + bindAddress
        + ":"
        + port
        + ", auth="
        + (authToken != null)
        + "]";
  }
}
