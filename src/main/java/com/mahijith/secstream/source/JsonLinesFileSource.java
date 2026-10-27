package com.mahijith.secstream.source;

import com.mahijith.secstream.model.EventFormat;
import com.mahijith.secstream.pipeline.Admission;
import com.mahijith.secstream.pipeline.EventIngress;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads one event per line from a file and submits it with blocking backpressure: if the pipeline
 * is saturated, reading simply pauses. Blank lines are skipped. Invalid UTF-8 is replaced rather
 * than failing the whole file, and the resulting line then fails validation on its own.
 */
public final class JsonLinesFileSource implements EventSource {
  private static final Logger log = LoggerFactory.getLogger(JsonLinesFileSource.class);

  private final String name;
  private final Path path;
  private final EventFormat format;
  private final int maxLineChars;
  private volatile boolean stopped;

  public JsonLinesFileSource(String name, Path path, EventFormat format, int maxLineChars) {
    this.name = name;
    this.path = path;
    this.format = format;
    this.maxLineChars = maxLineChars;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public boolean finite() {
    return true;
  }

  @Override
  public void run(EventIngress ingress) throws IOException, InterruptedException {
    var decoder =
        StandardCharsets.UTF_8
            .newDecoder()
            .onMalformedInput(CodingErrorAction.REPLACE)
            .onUnmappableCharacter(CodingErrorAction.REPLACE);
    long lines = 0;
    long accepted = 0;
    try (Reader reader = new InputStreamReader(Files.newInputStream(path), decoder)) {
      BoundedLineReader lineReader = new BoundedLineReader(reader, maxLineChars);
      String line;
      while (!stopped && (line = lineReader.readLine()) != null) {
        if (line.isBlank()) {
          continue;
        }
        lines++;
        Admission admission = ingress.submit(name, format, line);
        if (admission == Admission.ACCEPTED) {
          accepted++;
        } else if (admission == Admission.REJECTED_CLOSED) {
          log.atWarn()
              .setMessage("pipeline closed before file was fully read")
              .addKeyValue("source", name)
              .addKeyValue("linesRead", lines)
              .log();
          break;
        }
      }
    }
    log.atInfo()
        .setMessage("file source finished")
        .addKeyValue("source", name)
        .addKeyValue("lines", lines)
        .addKeyValue("accepted", accepted)
        .log();
  }

  @Override
  public void stop() {
    stopped = true;
  }
}
