package com.mahijith.secstream.sink;

import com.mahijith.secstream.delivery.EventSink;
import com.mahijith.secstream.delivery.SinkException;
import com.mahijith.secstream.model.SecurityEvent;
import java.io.PrintStream;
import java.util.List;

/** Prints events as JSON lines to standard output (logs go to stderr, so the two never mix). */
public final class ConsoleSink implements EventSink {
  private final String name;
  private final PrintStream out;

  public ConsoleSink(String name, PrintStream out) {
    this.name = name;
    this.out = out;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public void write(List<SecurityEvent> batch) throws SinkException {
    StringBuilder sb = new StringBuilder(batch.size() * 256);
    for (SecurityEvent event : batch) {
      sb.append(EventJson.toJsonLine(event)).append('\n');
    }
    out.print(sb);
    out.flush();
    if (out.checkError()) {
      throw SinkException.transientFailure(
          "stdout write failed", new java.io.IOException("checkError"));
    }
  }

  @Override
  public void close() {
    out.flush();
  }
}
