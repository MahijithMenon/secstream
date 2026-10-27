package com.mahijith.secstream.sink;

import com.mahijith.secstream.delivery.EventSink;
import com.mahijith.secstream.delivery.SinkException;
import com.mahijith.secstream.model.SecurityEvent;
import java.io.IOException;
import java.io.Writer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import org.jspecify.annotations.Nullable;

/**
 * Appends one JSON object per line. With {@code fsync} enabled each batch is forced to disk before
 * the write is reported successful; otherwise data reaches the OS page cache only and a machine
 * crash (not just a process crash) can lose the most recent batches.
 */
public final class JsonLinesFileSink implements EventSink {
  private final String name;
  private final Path path;
  private final boolean fsync;
  private @Nullable FileChannel channel;
  private @Nullable Writer writer;

  public JsonLinesFileSink(String name, Path path, boolean fsync) {
    this.name = name;
    this.path = path;
    this.fsync = fsync;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public void open() throws SinkException {
    try {
      ensureOpen();
    } catch (IOException e) {
      throw SinkException.permanentFailure("cannot open " + path, e);
    }
  }

  @Override
  public void write(List<SecurityEvent> batch) throws SinkException {
    try {
      Writer w = ensureOpen();
      for (SecurityEvent event : batch) {
        w.write(EventJson.toJsonLine(event));
        w.write('\n');
      }
      w.flush();
      if (fsync && channel != null) {
        channel.force(false);
      }
    } catch (IOException e) {
      // Reopen on the next attempt: the channel may be in an unknown state.
      closeQuietly();
      throw SinkException.transientFailure("write to " + path + " failed", e);
    }
  }

  private Writer ensureOpen() throws IOException {
    if (writer == null) {
      channel = OwnerOnlyFiles.openForAppend(path);
      writer = Channels.newWriter(channel, StandardCharsets.UTF_8);
    }
    return writer;
  }

  @Override
  public void close() {
    closeQuietly();
  }

  private void closeQuietly() {
    try {
      if (writer != null) {
        writer.close();
      }
    } catch (IOException ignored) {
      // Already failing or shutting down; the write path reported the real error.
    } finally {
      writer = null;
      channel = null;
    }
  }
}
