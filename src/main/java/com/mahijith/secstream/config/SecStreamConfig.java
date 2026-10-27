package com.mahijith.secstream.config;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * The YAML configuration model. Every record applies its defaults in the compact constructor, so
 * after deserialization the tree is complete and {@link ConfigValidator} only has to check values.
 * Secrets are never configured directly: only the <em>name</em> of the environment variable that
 * holds them ({@code passwordEnv}, {@code authTokenEnv}).
 */
public record SecStreamConfig(
    PipelineSection pipeline,
    ProcessingSection processing,
    List<SourceConfig> sources,
    List<SinkConfig> sinks,
    List<RouteConfig> routes,
    DeadLetterSection deadLetter,
    MetricsSection metrics) {

  public SecStreamConfig {
    pipeline =
        Objects.requireNonNullElseGet(pipeline, () -> new PipelineSection(null, null, null, null));
    processing =
        Objects.requireNonNullElseGet(processing, () -> new ProcessingSection(null, null, null));
    sources = sources == null ? List.of() : List.copyOf(sources);
    sinks = sinks == null ? List.of() : List.copyOf(sinks);
    routes = routes == null ? List.of() : List.copyOf(routes);
    deadLetter = Objects.requireNonNullElseGet(deadLetter, () -> new DeadLetterSection(null, null));
    metrics =
        Objects.requireNonNullElseGet(metrics, () -> new MetricsSection(null, null, null, null));
  }

  public record PipelineSection(
      Integer workers, Integer queueCapacity, Integer maxPayloadBytes, String shutdownTimeout) {
    public PipelineSection {
      workers = Objects.requireNonNullElse(workers, Runtime.getRuntime().availableProcessors());
      queueCapacity = Objects.requireNonNullElse(queueCapacity, 10_000);
      maxPayloadBytes = Objects.requireNonNullElse(maxPayloadBytes, 64 * 1024);
      shutdownTimeout = Objects.requireNonNullElse(shutdownTimeout, "30s");
    }
  }

  public record ProcessingSection(
      String minSeverity, Integer dedupCapacity, List<String> enrichers) {
    public ProcessingSection {
      minSeverity = Objects.requireNonNullElse(minSeverity, "INFO");
      dedupCapacity = Objects.requireNonNullElse(dedupCapacity, 100_000);
      enrichers = enrichers == null ? List.of("network-zone") : List.copyOf(enrichers);
    }
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
  @JsonSubTypes({
    @JsonSubTypes.Type(value = FileSourceConfig.class, name = "file"),
    @JsonSubTypes.Type(value = GeneratorSourceConfig.class, name = "generator"),
    @JsonSubTypes.Type(value = HttpSourceConfig.class, name = "http")
  })
  public sealed interface SourceConfig {
    @Nullable String name();
  }

  public record FileSourceConfig(
      @Nullable String name, @Nullable String path, @Nullable String format)
      implements SourceConfig {}

  public record GeneratorSourceConfig(
      @Nullable String name,
      Long count,
      Integer ratePerSecond,
      Double firewallFraction,
      Double failureFraction,
      Double duplicateFraction,
      Double malformedFraction,
      Long seed)
      implements SourceConfig {
    public GeneratorSourceConfig {
      count = Objects.requireNonNullElse(count, 10_000L);
      ratePerSecond = Objects.requireNonNullElse(ratePerSecond, 0);
      firewallFraction = Objects.requireNonNullElse(firewallFraction, 0.5);
      failureFraction = Objects.requireNonNullElse(failureFraction, 0.2);
      duplicateFraction = Objects.requireNonNullElse(duplicateFraction, 0.0);
      malformedFraction = Objects.requireNonNullElse(malformedFraction, 0.0);
      seed = Objects.requireNonNullElse(seed, 42L);
    }
  }

  public record HttpSourceConfig(
      @Nullable String name,
      String host,
      Integer port,
      Integer maxBodyBytes,
      Integer maxEventsPerRequest,
      Integer threads,
      String admissionTimeout,
      @Nullable String authTokenEnv)
      implements SourceConfig {
    public HttpSourceConfig {
      host = Objects.requireNonNullElse(host, "127.0.0.1");
      port = Objects.requireNonNullElse(port, 8080);
      maxBodyBytes = Objects.requireNonNullElse(maxBodyBytes, 1024 * 1024);
      maxEventsPerRequest = Objects.requireNonNullElse(maxEventsPerRequest, 1_000);
      threads = Objects.requireNonNullElse(threads, 4);
      admissionTimeout = Objects.requireNonNullElse(admissionTimeout, "200ms");
    }
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
  @JsonSubTypes({
    @JsonSubTypes.Type(value = ConsoleSinkConfig.class, name = "console"),
    @JsonSubTypes.Type(value = FileSinkConfig.class, name = "file"),
    @JsonSubTypes.Type(value = PostgresSinkConfig.class, name = "postgres"),
    @JsonSubTypes.Type(value = DiscardSinkConfig.class, name = "discard")
  })
  public sealed interface SinkConfig {
    @Nullable String name();

    DeliveryConfig delivery();
  }

  public record ConsoleSinkConfig(@Nullable String name, DeliveryConfig delivery)
      implements SinkConfig {
    public ConsoleSinkConfig {
      delivery = DeliveryConfig.orDefault(delivery);
    }
  }

  public record DiscardSinkConfig(@Nullable String name, DeliveryConfig delivery)
      implements SinkConfig {
    public DiscardSinkConfig {
      delivery = DeliveryConfig.orDefault(delivery);
    }
  }

  public record FileSinkConfig(
      @Nullable String name, @Nullable String path, Boolean fsync, DeliveryConfig delivery)
      implements SinkConfig {
    public FileSinkConfig {
      fsync = Objects.requireNonNullElse(fsync, false);
      delivery = DeliveryConfig.orDefault(delivery);
    }
  }

  public record PostgresSinkConfig(
      @Nullable String name,
      @Nullable String url,
      @Nullable String user,
      @Nullable String passwordEnv,
      String statementTimeout,
      DeliveryConfig delivery)
      implements SinkConfig {
    public PostgresSinkConfig {
      statementTimeout = Objects.requireNonNullElse(statementTimeout, "5s");
      delivery = DeliveryConfig.orDefault(delivery);
    }
  }

  public record DeliveryConfig(Integer queueCapacity, Integer batchSize, RetryConfig retry) {
    public DeliveryConfig {
      queueCapacity = Objects.requireNonNullElse(queueCapacity, 10_000);
      batchSize = Objects.requireNonNullElse(batchSize, 500);
      retry = Objects.requireNonNullElseGet(retry, () -> new RetryConfig(null, null, null, null));
    }

    static DeliveryConfig orDefault(@Nullable DeliveryConfig value) {
      return value != null ? value : new DeliveryConfig(null, null, null);
    }
  }

  public record RetryConfig(
      Integer maxAttempts, String initialBackoff, String maxBackoff, Double multiplier) {
    public RetryConfig {
      maxAttempts = Objects.requireNonNullElse(maxAttempts, 5);
      initialBackoff = Objects.requireNonNullElse(initialBackoff, "100ms");
      maxBackoff = Objects.requireNonNullElse(maxBackoff, "5s");
      multiplier = Objects.requireNonNullElse(multiplier, 2.0);
    }
  }

  public record RouteConfig(
      @Nullable String name,
      String minSeverity,
      List<String> categories,
      List<String> outcomes,
      List<String> sinks) {
    public RouteConfig {
      minSeverity = Objects.requireNonNullElse(minSeverity, "INFO");
      categories = categories == null ? List.of() : List.copyOf(categories);
      outcomes = outcomes == null ? List.of() : List.copyOf(outcomes);
      sinks = sinks == null ? List.of() : List.copyOf(sinks);
    }
  }

  public record DeadLetterSection(String path, Boolean fsync) {
    public DeadLetterSection {
      path = Objects.requireNonNullElse(path, "out/dead-letter.jsonl");
      fsync = Objects.requireNonNullElse(fsync, false);
    }
  }

  public record MetricsSection(Boolean enabled, String host, Integer port, String logInterval) {
    public MetricsSection {
      enabled = Objects.requireNonNullElse(enabled, true);
      host = Objects.requireNonNullElse(host, "127.0.0.1");
      port = Objects.requireNonNullElse(port, 9090);
      logInterval = Objects.requireNonNullElse(logInterval, "10s");
    }
  }
}
