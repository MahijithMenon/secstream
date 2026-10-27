package com.mahijith.secstream;

import com.mahijith.secstream.config.ConfigException;
import com.mahijith.secstream.config.ConfigLoader;
import com.mahijith.secstream.config.Durations;
import com.mahijith.secstream.config.PipelineAssembler;
import com.mahijith.secstream.config.PipelineAssembler.Assembly;
import com.mahijith.secstream.config.SecStreamConfig;
import com.mahijith.secstream.delivery.SinkException;
import com.mahijith.secstream.metrics.MetricsHttpServer;
import com.mahijith.secstream.metrics.MetricsReporter;
import com.mahijith.secstream.metrics.MetricsSnapshot;
import com.mahijith.secstream.pipeline.ShutdownReport;
import com.mahijith.secstream.source.EventSource;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Command-line entry point.
 *
 * <pre>java -jar secstream-all.jar [--config config/pipeline.yaml] [--validate]</pre>
 *
 * <p>Lifecycle: load and validate config; open sinks (fail fast); start workers, metrics and one
 * thread per source. If every source is finite the app shuts down once they finish; otherwise it
 * runs until SIGINT/SIGTERM. Either way shutdown is the same graceful drain.
 *
 * <p>Exit codes: 0 success, 1 runtime failure or forced shutdown, 2 invalid usage/configuration.
 */
public final class App {
  private static final Logger log = LoggerFactory.getLogger(App.class);

  private final Assembly assembly;
  private final List<Thread> sourceThreads = new ArrayList<>();
  private final AtomicBoolean stopping = new AtomicBoolean();
  private final CountDownLatch stopped = new CountDownLatch(1);
  private @Nullable MetricsHttpServer metricsServer;
  private @Nullable MetricsReporter reporter;
  private volatile boolean graceful = true;

  App(Assembly assembly) {
    this.assembly = assembly;
  }

  public static void main(String[] args) throws InterruptedException {
    System.exit(run(args));
  }

  static int run(String[] args) throws InterruptedException {
    Path configPath = Path.of("config/pipeline.yaml");
    boolean validateOnly = false;
    for (int i = 0; i < args.length; i++) {
      switch (args[i]) {
        case "--config" -> {
          if (i + 1 >= args.length) {
            System.err.println("--config requires a path");
            return 2;
          }
          configPath = Path.of(args[++i]);
        }
        case "--validate" -> validateOnly = true;
        default -> {
          System.err.println("usage: secstream [--config <path>] [--validate]");
          return 2;
        }
      }
    }
    // JDK HttpServer limits (seconds) against slow or stalled clients; must be set before use.
    System.setProperty("sun.net.httpserver.maxReqTime", "30");
    System.setProperty("sun.net.httpserver.maxRspTime", "30");

    SecStreamConfig config;
    try {
      config = ConfigLoader.load(configPath, System::getenv);
    } catch (ConfigException e) {
      System.err.println(e.getMessage());
      return 2;
    }
    if (validateOnly) {
      System.err.println("configuration " + configPath + " is valid");
      return 0;
    }
    Assembly assembly;
    try {
      assembly = PipelineAssembler.assemble(config, System::getenv, Clock.systemUTC(), System.out);
    } catch (IOException e) {
      log.atError().setMessage("startup failed").setCause(e).log();
      return 1;
    }
    App app = new App(assembly);
    try {
      app.start();
    } catch (SinkException | IOException e) {
      log.atError().setMessage("startup failed").setCause(e).log();
      app.stop();
      return 1;
    }
    Runtime.getRuntime().addShutdownHook(Thread.ofPlatform().name("shutdown").unstarted(app::stop));
    app.awaitSourcesThenStop();
    app.stopped.await();
    return app.graceful ? 0 : 1;
  }

  void start() throws SinkException, IOException {
    assembly.pipeline().start();
    var m = assembly.metricsConfig();
    if (m.enabled()) {
      MetricsHttpServer server =
          new MetricsHttpServer(
              m.host(), m.port(), assembly.metrics(), assembly.pipeline()::isRunning);
      server.start();
      metricsServer = server;
      MetricsReporter periodic = new MetricsReporter(assembly.metrics());
      periodic.start(Durations.parse(m.logInterval()));
      reporter = periodic;
      log.atInfo()
          .setMessage("metrics endpoint listening")
          .addKeyValue("port", server.port())
          .log();
    }
    for (EventSource source : assembly.sources()) {
      Thread t = Thread.ofPlatform().name("source-" + source.name()).start(() -> runSource(source));
      sourceThreads.add(t);
    }
  }

  private void runSource(EventSource source) {
    try {
      source.run(assembly.pipeline());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (Exception e) {
      log.atError()
          .setMessage("source failed")
          .addKeyValue("source", source.name())
          .setCause(e)
          .log();
    }
  }

  /** Returns once every finite source is done; with an infinite source, only after stop(). */
  void awaitSourcesThenStop() throws InterruptedException {
    boolean allFinite = assembly.sources().stream().allMatch(EventSource::finite);
    if (!allFinite) {
      return; // runs until a signal triggers the shutdown hook
    }
    for (Thread t : sourceThreads) {
      t.join();
    }
    stop();
  }

  /** Idempotent; safe to call from the shutdown hook and the main thread concurrently. */
  void stop() {
    if (!stopping.compareAndSet(false, true)) {
      awaitStoppedQuietly();
      return;
    }
    try {
      assembly.sources().forEach(EventSource::stop);
      for (Thread t : sourceThreads) {
        t.join(assembly.shutdownTimeout().toMillis());
      }
      ShutdownReport report = assembly.pipeline().shutdown(assembly.shutdownTimeout());
      graceful = report.graceful();
      if (reporter != null) {
        reporter.report();
        reporter.close();
      }
      if (metricsServer != null) {
        metricsServer.close();
      }
      logSummary(assembly.metrics().snapshot());
    } catch (InterruptedException e) {
      graceful = false;
      Thread.currentThread().interrupt();
    } finally {
      stopped.countDown();
    }
  }

  private void awaitStoppedQuietly() {
    try {
      stopped.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private static void logSummary(MetricsSnapshot s) {
    log.atInfo()
        .setMessage("final accounting")
        .addKeyValue("accepted", s.accepted())
        .addKeyValue("rejected", s.rejected())
        .addKeyValue("routed", s.routed())
        .addKeyValue("filtered", s.filtered())
        .addKeyValue("duplicates", s.duplicates())
        .addKeyValue("unrouted", s.unrouted())
        .addKeyValue("malformed", s.malformed())
        .addKeyValue("processingErrors", s.processingErrors())
        .addKeyValue("abandoned", s.abandoned())
        .addKeyValue("balanced", s.processingBalanced() && s.deliveriesBalanced())
        .log();
  }
}
