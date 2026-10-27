package com.mahijith.secstream.metrics;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BooleanSupplier;

/** {@code GET /metrics} (Prometheus text) and {@code GET /health} on a single small thread. */
public final class MetricsHttpServer implements AutoCloseable {
  private final HttpServer server;
  private final ExecutorService executor;

  public MetricsHttpServer(
      String bindAddress, int port, PipelineMetrics metrics, BooleanSupplier healthy)
      throws IOException {
    server = HttpServer.create(new InetSocketAddress(bindAddress, port), 0);
    executor =
        Executors.newSingleThreadExecutor(Thread.ofPlatform().name("metrics-http").factory());
    server.setExecutor(executor);
    server.createContext(
        "/metrics",
        ex ->
            respond(
                ex,
                200,
                "text/plain; version=0.0.4; charset=utf-8",
                PrometheusFormat.render(metrics.snapshot())));
    server.createContext(
        "/health",
        ex -> {
          boolean up = healthy.getAsBoolean();
          respond(ex, up ? 200 : 503, "text/plain; charset=utf-8", up ? "ok\n" : "stopping\n");
        });
  }

  public void start() {
    server.start();
  }

  public int port() {
    return server.getAddress().getPort();
  }

  private static void respond(HttpExchange ex, int status, String contentType, String body)
      throws IOException {
    try (ex) {
      if (!"GET".equals(ex.getRequestMethod())) {
        ex.sendResponseHeaders(405, -1);
        return;
      }
      byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
      ex.getResponseHeaders().set("Content-Type", contentType);
      ex.sendResponseHeaders(status, bytes.length);
      try (OutputStream out = ex.getResponseBody()) {
        out.write(bytes);
      }
    }
  }

  @Override
  public void close() {
    server.stop(0);
    executor.shutdownNow();
  }
}
