package com.mahijith.secstream.source;

import static org.assertj.core.api.Assertions.assertThat;

import com.mahijith.secstream.model.EventFormat;
import com.mahijith.secstream.pipeline.Admission;
import com.mahijith.secstream.pipeline.EventIngress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(30)
class HttpEventSourceTest {
  private final List<String> accepted = new CopyOnWriteArrayList<>();
  private final AtomicInteger capacity = new AtomicInteger(Integer.MAX_VALUE);
  private final HttpClient client = HttpClient.newHttpClient();
  private HttpEventSource source;
  private Thread thread;

  private final EventIngress ingress =
      new EventIngress() {
        @Override
        public Admission submit(String s, EventFormat f, String payload) {
          throw new UnsupportedOperationException("http must use bounded admission");
        }

        @Override
        public Admission trySubmit(String s, EventFormat f, String payload, Duration timeout) {
          if (payload.length() > 200) {
            return Admission.REJECTED_TOO_LARGE;
          }
          if (capacity.getAndDecrement() <= 0) {
            return Admission.REJECTED_QUEUE_FULL;
          }
          accepted.add(f.configName() + ":" + payload);
          return Admission.ACCEPTED;
        }
      };

  private int start(String token) throws Exception {
    source =
        new HttpEventSource(
            "http",
            new HttpSourceSettings("127.0.0.1", 0, 1024, 5, 2, Duration.ofMillis(10), token));
    thread =
        Thread.ofPlatform()
            .start(
                () -> {
                  try {
                    source.run(ingress);
                  } catch (Exception e) {
                    throw new RuntimeException(e);
                  }
                });
    return source.awaitPort();
  }

  @AfterEach
  void stop() throws InterruptedException {
    if (source != null) {
      source.stop();
      thread.join(5_000);
    }
  }

  private HttpResponse<String> post(int port, String path, String body, String token)
      throws Exception {
    HttpRequest.Builder b =
        HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
            .header("Content-Type", "application/x-ndjson")
            .POST(HttpRequest.BodyPublishers.ofString(body));
    if (token != null) {
      b.header("Authorization", "Bearer " + token);
    }
    return client.send(b.build(), HttpResponse.BodyHandlers.ofString());
  }

  @Test
  void acceptsNdjsonBatch() throws Exception {
    int port = start(null);
    HttpResponse<String> r = post(port, "/v1/events/firewall", "{\"a\":1}\n\n{\"b\":2}\n", null);
    assertThat(r.statusCode()).isEqualTo(202);
    assertThat(r.body()).contains("\"accepted\":2");
    assertThat(accepted).containsExactly("firewall:{\"a\":1}", "firewall:{\"b\":2}");
  }

  @Test
  void queueFullReturns503WithAcceptedPrefix() throws Exception {
    int port = start(null);
    capacity.set(2);
    HttpResponse<String> r = post(port, "/v1/events/signin", "1\n2\n3\n4\n", null);
    assertThat(r.statusCode()).isEqualTo(503);
    assertThat(r.headers().firstValue("Retry-After")).hasValue("1");
    assertThat(r.body()).contains("\"accepted\":2").contains("\"notAttempted\":2");
    assertThat(accepted).hasSize(2);
  }

  @Test
  void rejectsInvalidRequestsWithoutAdmittingAnything() throws Exception {
    int port = start(null);
    assertThat(post(port, "/v1/events/syslog", "{}", null).statusCode()).isEqualTo(404);
    assertThat(post(port, "/v1/events/signin", "x".repeat(2_000), null).statusCode())
        .isEqualTo(413);
    assertThat(post(port, "/v1/events/signin", "1\n2\n3\n4\n5\n6\n", null).statusCode())
        .isEqualTo(413);
    assertThat(post(port, "/v1/events/signin", "  \n", null).statusCode()).isEqualTo(400);
    HttpResponse<String> get =
        client.send(
            HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/v1/events/signin"))
                .GET()
                .build(),
            HttpResponse.BodyHandlers.ofString());
    assertThat(get.statusCode()).isEqualTo(405);
    HttpResponse<String> badUtf8 =
        client.send(
            HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/v1/events/signin"))
                .POST(HttpRequest.BodyPublishers.ofByteArray(new byte[] {(byte) 0xC3, (byte) 0x28}))
                .build(),
            HttpResponse.BodyHandlers.ofString());
    assertThat(badUtf8.statusCode()).isEqualTo(400);
    assertThat(accepted).isEmpty();
  }

  @Test
  void enforcesBearerTokenWhenConfigured() throws Exception {
    int port = start("s3cret-test-token");
    assertThat(post(port, "/v1/events/signin", "{}", null).statusCode()).isEqualTo(401);
    assertThat(post(port, "/v1/events/signin", "{}", "wrong").statusCode()).isEqualTo(401);
    assertThat(post(port, "/v1/events/signin", "{}", "s3cret-test-token").statusCode())
        .isEqualTo(202);
  }

  @Test
  void settingsToStringNeverRevealsToken() {
    var s = new HttpSourceSettings("127.0.0.1", 0, 1, 1, 1, Duration.ZERO, "hunter2");
    assertThat(s.toString()).doesNotContain("hunter2");
  }
}
