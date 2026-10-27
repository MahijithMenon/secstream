package com.mahijith.secstream;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/** Runs the real CLI end to end on the checked-in sample files. */
@Timeout(60)
class AppTest {

  @Test
  void runsSampleFilesToCompletion(@TempDir Path dir) throws Exception {
    Path archive = dir.resolve("archive.jsonl");
    Path dlq = dir.resolve("dlq.jsonl");
    String config =
        Files.readString(Path.of("config/pipeline.yaml"))
            .replace("out/archive.jsonl", archive.toString())
            .replace("out/dead-letter.jsonl", dlq.toString())
            .replace("port: 9090", "port: 0")
            .replace("samples/", Path.of("samples").toAbsolutePath() + "/");
    Path configFile = dir.resolve("pipeline.yaml");
    Files.writeString(configFile, config);

    assertThat(App.run(new String[] {"--config", configFile.toString()})).isZero();

    // samples: 8 sign-in lines (1 duplicate, 2 malformed) + 5 firewall lines (1 malformed)
    assertThat(Files.readAllLines(archive)).hasSize(9);
    assertThat(Files.readAllLines(dlq)).hasSize(3).allMatch(l -> l.contains("\"MALFORMED\""));
  }

  @Test
  void invalidConfigurationExitsWithUsageError(@TempDir Path dir) throws Exception {
    Path bad = dir.resolve("bad.yaml");
    Files.writeString(bad, "sources: []\n");
    assertThat(App.run(new String[] {"--config", bad.toString()})).isEqualTo(2);
    assertThat(App.run(new String[] {"--bogus"})).isEqualTo(2);
  }

  @Test
  void validateOnlyDoesNotStartAnything() throws Exception {
    assertThat(App.run(new String[] {"--config", "config/pipeline.yaml", "--validate"})).isZero();
  }
}
