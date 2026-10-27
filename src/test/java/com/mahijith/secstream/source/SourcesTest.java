package com.mahijith.secstream.source;

import static org.assertj.core.api.Assertions.assertThat;

import com.mahijith.secstream.model.EventFormat;
import com.mahijith.secstream.pipeline.Admission;
import com.mahijith.secstream.pipeline.EventIngress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SourcesTest {
  /** Captures submissions; optionally closes after N. */
  static final class CapturingIngress implements EventIngress {
    final List<String> payloads = new ArrayList<>();
    final int closeAfter;

    CapturingIngress(int closeAfter) {
      this.closeAfter = closeAfter;
    }

    @Override
    public synchronized Admission submit(String source, EventFormat format, String payload) {
      if (payloads.size() >= closeAfter) {
        return Admission.REJECTED_CLOSED;
      }
      payloads.add(payload);
      return Admission.ACCEPTED;
    }

    @Override
    public Admission trySubmit(
        String source, EventFormat format, String payload, Duration timeout) {
      return submit(source, format, payload);
    }
  }

  @Test
  void fileSourceSubmitsNonBlankLinesAndStopsWhenClosed(@TempDir Path dir) throws Exception {
    Path file = dir.resolve("in.jsonl");
    Files.writeString(file, "{\"a\":1}\n\n  \n{\"b\":2}\n{\"c\":3}\n");
    CapturingIngress all = new CapturingIngress(Integer.MAX_VALUE);
    new JsonLinesFileSource("f", file, EventFormat.SIGN_IN, 1024).run(all);
    assertThat(all.payloads).containsExactly("{\"a\":1}", "{\"b\":2}", "{\"c\":3}");

    CapturingIngress closing = new CapturingIngress(1);
    new JsonLinesFileSource("f", file, EventFormat.SIGN_IN, 1024).run(closing);
    assertThat(closing.payloads).hasSize(1);
  }

  @Test
  void generatorIsDeterministicForASeed() {
    GeneratorSettings settings = new GeneratorSettings(500, 0, 0.5, 0.2, 0.1, 0.05, 99);
    Instant now = Instant.parse("2026-10-01T00:00:00Z");
    SyntheticEventGenerator a = new SyntheticEventGenerator(settings, now);
    SyntheticEventGenerator b = new SyntheticEventGenerator(settings, now);
    for (int i = 0; i < 500; i++) {
      assertThat(a.next()).isEqualTo(b.next());
    }
  }

  @Test
  void generatorSourceProducesConfiguredCount() throws Exception {
    CapturingIngress ingress = new CapturingIngress(Integer.MAX_VALUE);
    new SyntheticEventSource(
            "g", new GeneratorSettings(250, 0, 0.5, 0.2, 0, 0, 1), Clock.systemUTC())
        .run(ingress);
    assertThat(ingress.payloads).hasSize(250);
  }
}
