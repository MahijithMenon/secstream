package com.mahijith.secstream.sink;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.mahijith.secstream.delivery.DeadLetter;
import com.mahijith.secstream.delivery.DeadLetterReason;
import com.mahijith.secstream.processing.Json;
import com.mahijith.secstream.testing.TestEvents;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FileOutputsTest {

  @Test
  void jsonLinesSinkAppendsStableJsonWithOwnerOnlyPermissions(@TempDir Path dir) throws Exception {
    Path out = dir.resolve("nested/archive.jsonl");
    JsonLinesFileSink sink = new JsonLinesFileSink("archive", out, true);
    sink.open();
    sink.write(
        List.of(TestEvents.event("a").attribute("k", "v").build(), TestEvents.event("b").build()));
    sink.close();
    new JsonLinesFileSink("archive", out, false).write(List.of(TestEvents.event("c").build()));

    List<String> lines = Files.readAllLines(out);
    assertThat(lines).hasSize(3);
    JsonNode first = Json.MAPPER.readTree(lines.get(0));
    assertThat(first.get("userId").asText()).isEqualTo("user-a");
    assertThat(first.get("sourceIp").asText()).isEqualTo("198.51.100.7");
    assertThat(first.get("attributes").get("k").asText()).isEqualTo("v");
    assertThat(first.has("destinationIp")).isFalse();
    if (FileSystems.getDefault().supportedFileAttributeViews().contains("posix")) {
      assertThat(PosixFilePermissions.toString(Files.getPosixFilePermissions(out)))
          .isEqualTo("rw-------");
    }
  }

  @Test
  void consoleSinkWritesOneLinePerEvent() throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    ConsoleSink sink = new ConsoleSink("c", new PrintStream(bytes, true, StandardCharsets.UTF_8));
    sink.write(List.of(TestEvents.event("a").build(), TestEvents.event("b").build()));
    assertThat(bytes.toString(StandardCharsets.UTF_8).lines()).hasSize(2);
  }

  @Test
  void deadLetterStoreNeverInterleavesConcurrentWritesAndTruncatesHugePayloads(@TempDir Path dir)
      throws Exception {
    Path file = dir.resolve("dlq.jsonl");
    FileDeadLetterStore store = new FileDeadLetterStore(file, false);
    ExecutorService pool = Executors.newFixedThreadPool(8);
    for (int i = 0; i < 2_000; i++) {
      int n = i;
      pool.execute(
          () -> {
            try {
              store.append(
                  new DeadLetter(
                      TestEvents.NOW,
                      DeadLetterReason.MALFORMED,
                      "t",
                      n,
                      null,
                      0,
                      "bad",
                      "{\"n\":" + n + "}",
                      null));
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          });
    }
    pool.shutdown();
    assertThat(pool.awaitTermination(30, TimeUnit.SECONDS)).isTrue();
    store.append(
        new DeadLetter(
            TestEvents.NOW,
            DeadLetterReason.MALFORMED,
            "t",
            -1,
            null,
            0,
            "huge",
            "x".repeat(100_000),
            null));
    store.append(
        new DeadLetter(
            TestEvents.NOW,
            DeadLetterReason.RETRIES_EXHAUSTED,
            "t",
            -2,
            "db",
            5,
            "down",
            null,
            TestEvents.event("z").build()));
    store.close();

    List<String> lines = Files.readAllLines(file);
    assertThat(lines).hasSize(2_002);
    for (String line : lines) {
      Json.MAPPER.readTree(line); // every line is a complete JSON object
    }
    JsonNode huge = Json.MAPPER.readTree(lines.get(2_000));
    assertThat(huge.get("rawPayload").asText()).hasSize(FileDeadLetterStore.MAX_RAW_PAYLOAD_CHARS);
    assertThat(huge.get("rawPayloadTruncated").asBoolean()).isTrue();
    JsonNode delivery = Json.MAPPER.readTree(lines.get(2_001));
    assertThat(delivery.get("sink").asText()).isEqualTo("db");
    assertThat(delivery.get("event").get("userId").asText()).isEqualTo("user-z");
  }
}
