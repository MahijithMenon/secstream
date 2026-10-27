package com.mahijith.secstream.source;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mahijith.secstream.model.EventFormat;
import com.mahijith.secstream.pipeline.Admission;
import com.mahijith.secstream.pipeline.EventIngress;
import com.mahijith.secstream.processing.Json;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code POST /v1/events/{signin|firewall}} with a body of newline-delimited JSON events.
 *
 * <p>Responses:
 *
 * <ul>
 *   <li>202: every event was accepted (accepted does not mean valid; invalid events are
 *       dead-lettered later, asynchronously).
 *   <li>503 + {@code Retry-After}: the queue stayed full (or the pipeline is closing). Events are
 *       offered in order and admission stops at the first refusal, so the response's {@code
 *       accepted} count tells the client exactly which prefix was taken; it should resend the rest.
 *       Resending an event that was in fact accepted is harmless when it carries an id, because
 *       deduplication drops it.
 *   <li>400 / 401 / 404 / 405 / 413 / 415: client errors; nothing was accepted.
 * </ul>
 *
 * <p>Resource bounds: body size is checked against Content-Length and enforced while reading;
 * events per request are capped; requests are served by a fixed-size thread pool.
 */
public final class HttpEventSource implements EventSource {
  private static final Logger log = LoggerFactory.getLogger(HttpEventSource.class);
  static final String PATH_PREFIX = "/v1/events/";

  private final String name;
  private final HttpSourceSettings settings;
  private final CountDownLatch started = new CountDownLatch(1);
  private final CountDownLatch stopRequested = new CountDownLatch(1);
  private volatile int boundPort = -1;

  public HttpEventSource(String name, HttpSourceSettings settings) {
    this.name = name;
    this.settings = settings;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public boolean finite() {
    return false;
  }

  @Override
  public void run(EventIngress ingress) throws IOException, InterruptedException {
    HttpServer server =
        HttpServer.create(new InetSocketAddress(settings.bindAddress(), settings.port()), 0);
    ExecutorService executor =
        Executors.newFixedThreadPool(
            settings.threads(), Thread.ofPlatform().name("http-" + name + "-", 1).factory());
    server.setExecutor(executor);
    server.createContext(PATH_PREFIX, exchange -> handle(exchange, ingress));
    server.start();
    boundPort = server.getAddress().getPort();
    started.countDown();
    log.atInfo()
        .setMessage("http source listening")
        .addKeyValue("source", name)
        .addKeyValue("address", settings.bindAddress())
        .addKeyValue("port", boundPort)
        .addKeyValue("auth", settings.authToken() != null)
        .log();
    try {
      stopRequested.await();
    } finally {
      // Stops accepting connections, then waits up to 2s for in-progress exchanges.
      server.stop(2);
      executor.shutdownNow();
    }
  }

  @Override
  public void stop() {
    stopRequested.countDown();
  }

  /** For tests binding port 0: waits until listening and returns the actual port. */
  public int awaitPort() throws InterruptedException {
    started.await();
    return boundPort;
  }

  private void handle(HttpExchange exchange, EventIngress ingress) throws IOException {
    try (exchange) {
      if (!"POST".equals(exchange.getRequestMethod())) {
        exchange.getResponseHeaders().add("Allow", "POST");
        respond(exchange, 405, error("method not allowed"));
        return;
      }
      if (!authorized(exchange)) {
        respond(exchange, 401, error("missing or invalid bearer token"));
        return;
      }
      EventFormat format;
      try {
        format =
            EventFormat.fromConfigName(
                exchange.getRequestURI().getPath().substring(PATH_PREFIX.length()));
      } catch (IllegalArgumentException e) {
        respond(exchange, 404, error("unknown event format"));
        return;
      }
      String contentType = exchange.getRequestHeaders().getFirst("Content-Type");
      if (contentType != null
          && !contentType.startsWith("application/x-ndjson")
          && !contentType.startsWith("application/json")) {
        respond(exchange, 415, error("expected application/x-ndjson or application/json"));
        return;
      }
      String declared = exchange.getRequestHeaders().getFirst("Content-Length");
      if (declared != null && parseLongOrMinus(declared) > settings.maxBodyBytes()) {
        respond(exchange, 413, error("body exceeds " + settings.maxBodyBytes() + " bytes"));
        return;
      }
      byte[] body = readBounded(exchange.getRequestBody(), settings.maxBodyBytes());
      if (body == null) {
        respond(exchange, 413, error("body exceeds " + settings.maxBodyBytes() + " bytes"));
        return;
      }
      String text;
      try {
        text =
            StandardCharsets.UTF_8
                .newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .decode(ByteBuffer.wrap(body))
                .toString();
      } catch (CharacterCodingException e) {
        respond(exchange, 400, error("body is not valid UTF-8"));
        return;
      }
      String[] lines = text.lines().filter(l -> !l.isBlank()).toArray(String[]::new);
      if (lines.length == 0) {
        respond(exchange, 400, error("no events in body"));
        return;
      }
      if (lines.length > settings.maxEventsPerRequest()) {
        respond(exchange, 413, error("more than " + settings.maxEventsPerRequest() + " events"));
        return;
      }
      admit(exchange, ingress, format, lines);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (RuntimeException e) {
      log.atError().setMessage("http handler failed").addKeyValue("source", name).setCause(e).log();
      try {
        respond(exchange, 500, error("internal error"));
      } catch (IOException | RuntimeException ignored) {
        // Response may already be committed; the connection will be closed.
      }
    }
  }

  private void admit(
      HttpExchange exchange, EventIngress ingress, EventFormat format, String[] lines)
      throws IOException, InterruptedException {
    int accepted = 0;
    int tooLarge = 0;
    Admission stoppedBy = null;
    for (String line : lines) {
      Admission a = ingress.trySubmit(name, format, line, settings.admissionTimeout());
      if (a == Admission.ACCEPTED) {
        accepted++;
      } else if (a == Admission.REJECTED_TOO_LARGE) {
        tooLarge++;
      } else {
        stoppedBy = a;
        break;
      }
    }
    ObjectNode body = Json.MAPPER.createObjectNode();
    body.put("accepted", accepted);
    body.put("rejectedTooLarge", tooLarge);
    body.put("notAttempted", lines.length - accepted - tooLarge);
    if (stoppedBy == null) {
      respond(exchange, 202, body);
    } else {
      body.put("error", stoppedBy == Admission.REJECTED_CLOSED ? "shutting down" : "queue full");
      exchange.getResponseHeaders().add("Retry-After", "1");
      respond(exchange, 503, body);
    }
  }

  private boolean authorized(HttpExchange exchange) {
    String token = settings.authToken();
    if (token == null) {
      return true;
    }
    String header = exchange.getRequestHeaders().getFirst("Authorization");
    if (header == null || !header.startsWith("Bearer ")) {
      return false;
    }
    // Constant-time comparison: response timing must not reveal how many leading bytes match.
    return MessageDigest.isEqual(
        header.substring(7).getBytes(StandardCharsets.UTF_8),
        token.getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Returns null if the stream holds more than {@code max} bytes; never buffers more than max+1.
   */
  static byte[] readBounded(InputStream in, int max) throws IOException {
    byte[] bytes = in.readNBytes(max + 1);
    return bytes.length > max ? null : bytes;
  }

  private static long parseLongOrMinus(String s) {
    try {
      return Long.parseLong(s.trim());
    } catch (NumberFormatException e) {
      return -1;
    }
  }

  private static ObjectNode error(String message) {
    return Json.MAPPER.createObjectNode().put("error", message);
  }

  private static void respond(HttpExchange exchange, int status, ObjectNode body)
      throws IOException {
    byte[] bytes = Json.MAPPER.writeValueAsBytes(body);
    exchange.getResponseHeaders().set("Content-Type", "application/json");
    exchange.sendResponseHeaders(status, bytes.length);
    try (OutputStream out = exchange.getResponseBody()) {
      out.write(bytes);
    }
  }
}
