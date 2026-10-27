package com.mahijith.secstream.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.util.Map;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

class ConfigLoaderTest {
  private static final Function<String, String> ENV =
      Map.of("SECSTREAM_PG_PASSWORD", "test-only", "SECSTREAM_HTTP_TOKEN", "test-only")::get;

  private static final String MINIMAL =
      """
      sources:
        - {name: in, type: file, path: in.jsonl, format: signin}
      sinks:
        - {name: out, type: console}
      routes:
        - {name: all, sinks: [out]}
      """;

  @Test
  void checkedInConfigsAreValid() throws Exception {
    assertThat(ConfigLoader.load(Path.of("config/pipeline.yaml"), ENV).sinks()).hasSize(2);
    assertThat(ConfigLoader.load(Path.of("config/pipeline-full.yaml"), ENV).sources()).hasSize(2);
  }

  @Test
  void appliesDefaults() throws Exception {
    SecStreamConfig c = ConfigLoader.parse(MINIMAL, ENV);
    assertThat(c.pipeline().queueCapacity()).isEqualTo(10_000);
    assertThat(c.sinks().get(0).delivery().retry().maxAttempts()).isEqualTo(5);
    assertThat(c.metrics().host()).isEqualTo("127.0.0.1");
    assertThat(c.processing().enrichers()).containsExactly("network-zone");
  }

  @Test
  void unknownKeysFailWithLocation() {
    assertThatThrownBy(() -> ConfigLoader.parse(MINIMAL + "pipeline: {wrokers: 4}\n", ENV))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("wrokers")
        .hasMessageContaining("workers");
  }

  @Test
  void unknownTypeFails() {
    assertThatThrownBy(
            () -> ConfigLoader.parse(MINIMAL.replace("type: console", "type: kafka"), ENV))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("kafka");
  }

  @Test
  void reportsEveryProblemAtOnce() {
    String bad =
        """
        pipeline: {workers: 0, queueCapacity: -1, shutdownTimeout: soon}
        processing: {minSeverity: URGENT, enrichers: [geoip]}
        sources:
          - {name: In, type: file, format: syslog}
          - {name: gen, type: generator, firewallFraction: 1.5}
        sinks:
          - {name: db, type: postgres, url: "mysql://x", user: u, passwordEnv: NOT_SET}
          - {name: db, type: console, delivery: {retry: {initialBackoff: 10s, maxBackoff: 1s}}}
        routes:
          - {name: r, categories: [EMAIL], sinks: [nowhere]}
        """;
    assertThatThrownBy(() -> ConfigLoader.parse(bad, ENV))
        .isInstanceOfSatisfying(
            ConfigException.class,
            e ->
                assertThat(e.problems())
                    .anyMatch(p -> p.startsWith("pipeline.workers"))
                    .anyMatch(p -> p.startsWith("pipeline.queueCapacity"))
                    .anyMatch(p -> p.startsWith("pipeline.shutdownTimeout"))
                    .anyMatch(p -> p.startsWith("processing.minSeverity"))
                    .anyMatch(p -> p.contains("geoip"))
                    .anyMatch(p -> p.contains("sources[0] (In).name"))
                    .anyMatch(p -> p.contains("sources[0] (In).path: required"))
                    .anyMatch(p -> p.contains("syslog"))
                    .anyMatch(p -> p.contains("firewallFraction"))
                    .anyMatch(p -> p.contains(".url"))
                    .anyMatch(p -> p.contains("NOT_SET is not set"))
                    .anyMatch(p -> p.contains("duplicate name 'db'"))
                    .anyMatch(p -> p.contains("maxBackoff must be >= initialBackoff"))
                    .anyMatch(p -> p.contains("EMAIL"))
                    .anyMatch(p -> p.contains("unknown sink 'nowhere'")));
  }

  @Test
  void emptyAndMissingFilesFailClearly() {
    assertThatThrownBy(() -> ConfigLoader.parse("", ENV)).isInstanceOf(ConfigException.class);
    assertThatThrownBy(() -> ConfigLoader.load(Path.of("no/such.yaml"), ENV))
        .hasMessageContaining("not found");
  }

  @Test
  void durations() {
    assertThat(Durations.parse("250ms").toMillis()).isEqualTo(250);
    assertThat(Durations.parse("2m").toSeconds()).isEqualTo(120);
    assertThatThrownBy(() -> Durations.parse("5 seconds"))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
