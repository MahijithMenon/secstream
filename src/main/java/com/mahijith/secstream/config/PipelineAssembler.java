package com.mahijith.secstream.config;

import com.mahijith.secstream.config.SecStreamConfig.ConsoleSinkConfig;
import com.mahijith.secstream.config.SecStreamConfig.DiscardSinkConfig;
import com.mahijith.secstream.config.SecStreamConfig.FileSinkConfig;
import com.mahijith.secstream.config.SecStreamConfig.FileSourceConfig;
import com.mahijith.secstream.config.SecStreamConfig.GeneratorSourceConfig;
import com.mahijith.secstream.config.SecStreamConfig.HttpSourceConfig;
import com.mahijith.secstream.config.SecStreamConfig.PostgresSinkConfig;
import com.mahijith.secstream.config.SecStreamConfig.RetryConfig;
import com.mahijith.secstream.config.SecStreamConfig.RouteConfig;
import com.mahijith.secstream.config.SecStreamConfig.SinkConfig;
import com.mahijith.secstream.config.SecStreamConfig.SourceConfig;
import com.mahijith.secstream.delivery.DeadLetterChannel;
import com.mahijith.secstream.delivery.EventSink;
import com.mahijith.secstream.delivery.RetryPolicy;
import com.mahijith.secstream.delivery.SinkDispatcher;
import com.mahijith.secstream.delivery.Sleeper;
import com.mahijith.secstream.metrics.PipelineMetrics;
import com.mahijith.secstream.model.EventCategory;
import com.mahijith.secstream.model.EventFormat;
import com.mahijith.secstream.model.EventOutcome;
import com.mahijith.secstream.model.Severity;
import com.mahijith.secstream.pipeline.Pipeline;
import com.mahijith.secstream.pipeline.PipelineSettings;
import com.mahijith.secstream.processing.Deduplicator;
import com.mahijith.secstream.processing.EventProcessor;
import com.mahijith.secstream.processing.ProcessingStages;
import com.mahijith.secstream.processing.SeverityFilter;
import com.mahijith.secstream.routing.EventMatcher;
import com.mahijith.secstream.routing.Router;
import com.mahijith.secstream.routing.RoutingRule;
import com.mahijith.secstream.sink.ConsoleSink;
import com.mahijith.secstream.sink.DiscardingSink;
import com.mahijith.secstream.sink.FileDeadLetterStore;
import com.mahijith.secstream.sink.JsonLinesFileSink;
import com.mahijith.secstream.sink.PostgresSink;
import com.mahijith.secstream.source.EventSource;
import com.mahijith.secstream.source.GeneratorSettings;
import com.mahijith.secstream.source.HttpEventSource;
import com.mahijith.secstream.source.HttpSourceSettings;
import com.mahijith.secstream.source.JsonLinesFileSource;
import com.mahijith.secstream.source.SyntheticEventSource;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.SplittableRandom;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.jspecify.annotations.Nullable;

/**
 * Turns a validated {@link SecStreamConfig} into wired components. This is the only place that
 * knows about concrete source and sink types (a factory in the plain sense), so onboarding a new
 * type touches the config model, the validator, and this class, never the pipeline.
 */
public final class PipelineAssembler {
  private PipelineAssembler() {}

  /** Fields the validator guarantees are present; a null here means the validator has a bug. */
  private static <T> T required(@Nullable T value) {
    if (value == null) {
      throw new IllegalStateException("validated configuration field is missing");
    }
    return value;
  }

  /** Everything the application needs to run. */
  public record Assembly(
      Pipeline pipeline,
      List<EventSource> sources,
      PipelineMetrics metrics,
      Duration shutdownTimeout,
      SecStreamConfig.MetricsSection metricsConfig) {}

  public static Assembly assemble(
      SecStreamConfig config, Function<String, String> env, Clock clock, PrintStream stdout)
      throws IOException {
    PipelineMetrics metrics = new PipelineMetrics();
    var dl = config.deadLetter();
    DeadLetterChannel deadLetters =
        new DeadLetterChannel(new FileDeadLetterStore(Path.of(dl.path()), dl.fsync()), metrics);

    // Seeded per sink for reproducible tests; the jitter only needs to differ between sinks/runs.
    SplittableRandom seeds = new SplittableRandom(clock.millis());
    List<SinkDispatcher> dispatchers =
        config.sinks().stream()
            .map(
                s ->
                    new SinkDispatcher(
                        sink(s, env, stdout),
                        s.delivery().queueCapacity(),
                        s.delivery().batchSize(),
                        retryPolicy(s.delivery().retry()),
                        deadLetters,
                        metrics.sink(required(s.name())),
                        Sleeper.SYSTEM,
                        seeds.split(),
                        clock))
            .toList();

    var p = config.processing();
    EventProcessor processor =
        new EventProcessor(
            EventProcessor.defaultNormalizers(),
            new SeverityFilter(Severity.parse(p.minSeverity())),
            new Deduplicator(p.dedupCapacity()),
            ProcessingStages.enrichers(p.enrichers()),
            new Router(config.routes().stream().map(PipelineAssembler::rule).toList()));

    var ps = config.pipeline();
    int maxPayload = ps.maxPayloadBytes();
    Pipeline pipeline =
        new Pipeline(
            new PipelineSettings(ps.workers(), ps.queueCapacity(), maxPayload),
            processor,
            dispatchers,
            deadLetters,
            metrics,
            clock);

    List<EventSource> sources =
        config.sources().stream().map(s -> source(s, env, clock, maxPayload)).toList();
    return new Assembly(
        pipeline, sources, metrics, Durations.parse(ps.shutdownTimeout()), config.metrics());
  }

  static RetryPolicy retryPolicy(RetryConfig r) {
    return new RetryPolicy(
        r.maxAttempts(),
        Durations.parse(r.initialBackoff()),
        Durations.parse(r.maxBackoff()),
        r.multiplier());
  }

  static RoutingRule rule(RouteConfig r) {
    return new RoutingRule(
        required(r.name()),
        new EventMatcher(
            Severity.parse(r.minSeverity()),
            r.categories().stream()
                .map(c -> EventCategory.valueOf(c.toUpperCase(Locale.ROOT)))
                .collect(Collectors.toSet()),
            r.outcomes().stream()
                .map(o -> EventOutcome.valueOf(o.toUpperCase(Locale.ROOT)))
                .collect(Collectors.toSet())),
        r.sinks());
  }

  private static EventSink sink(SinkConfig s, Function<String, String> env, PrintStream stdout) {
    return switch (s) {
      case ConsoleSinkConfig c -> new ConsoleSink(required(c.name()), stdout);
      case DiscardSinkConfig d -> new DiscardingSink(required(d.name()));
      case FileSinkConfig f ->
          new JsonLinesFileSink(required(f.name()), Path.of(required(f.path())), f.fsync());
      case PostgresSinkConfig pg ->
          new PostgresSink(
              required(pg.name()),
              required(pg.url()),
              required(pg.user()),
              required(env.apply(required(pg.passwordEnv()))),
              Durations.parse(pg.statementTimeout()));
    };
  }

  private static EventSource source(
      SourceConfig s, Function<String, String> env, Clock clock, int maxPayload) {
    return switch (s) {
      case FileSourceConfig f ->
          new JsonLinesFileSource(
              required(f.name()),
              Path.of(required(f.path())),
              EventFormat.fromConfigName(required(f.format())),
              maxPayload);
      case GeneratorSourceConfig g ->
          new SyntheticEventSource(
              required(g.name()),
              new GeneratorSettings(
                  g.count(),
                  g.ratePerSecond(),
                  g.firewallFraction(),
                  g.failureFraction(),
                  g.duplicateFraction(),
                  g.malformedFraction(),
                  g.seed()),
              clock);
      case HttpSourceConfig h ->
          new HttpEventSource(
              required(h.name()),
              new HttpSourceSettings(
                  h.host(),
                  h.port(),
                  h.maxBodyBytes(),
                  h.maxEventsPerRequest(),
                  h.threads(),
                  Durations.parse(h.admissionTimeout()),
                  h.authTokenEnv() == null ? null : env.apply(h.authTokenEnv())));
    };
  }
}
