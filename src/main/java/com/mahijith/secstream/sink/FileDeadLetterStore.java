package com.mahijith.secstream.sink;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.mahijith.secstream.delivery.DeadLetter;
import com.mahijith.secstream.delivery.DeadLetterStore;
import com.mahijith.secstream.model.SecurityEvent;
import com.mahijith.secstream.processing.Json;
import java.io.IOException;
import java.io.Writer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Dead letters as JSON lines in an owner-only file. Each entry is flushed (and optionally fsynced)
 * before {@link #append} returns.
 *
 * <p>Many threads write here (every worker, every dispatcher), and a line must never interleave
 * with another, so appends are serialized by a lock. Contention is acceptable because dead letters
 * are the exception path. A {@link ReentrantLock} rather than {@code synchronized} keeps the lock
 * private (nobody can synchronize on this object from outside) and avoids pinning a virtual-thread
 * carrier during blocking I/O on Java 21.
 *
 * <p>Raw payloads are truncated to bound file growth from oversized garbage.
 */
public final class FileDeadLetterStore implements DeadLetterStore {
  static final int MAX_RAW_PAYLOAD_CHARS = 16 * 1024;

  private final ReentrantLock lock = new ReentrantLock();
  private final FileChannel channel;
  private final Writer writer;
  private final boolean fsync;

  public FileDeadLetterStore(Path path, boolean fsync) throws IOException {
    this.channel = OwnerOnlyFiles.openForAppend(path);
    this.writer = Channels.newWriter(channel, StandardCharsets.UTF_8);
    this.fsync = fsync;
  }

  @Override
  public void append(DeadLetter d) throws IOException {
    String line = Json.MAPPER.writeValueAsString(toNode(d));
    lock.lock();
    try {
      writer.write(line);
      writer.write('\n');
      writer.flush();
      if (fsync) {
        channel.force(false);
      }
    } finally {
      lock.unlock();
    }
  }

  static ObjectNode toNode(DeadLetter d) {
    ObjectNode n = Json.MAPPER.createObjectNode();
    n.put("timestamp", d.timestamp().toString());
    n.put("reason", d.reason().name());
    n.put("source", d.source());
    n.put("sequence", d.sequence());
    String sink = d.sink();
    if (sink != null) {
      n.put("sink", sink);
    }
    n.put("attempts", d.attempts());
    n.put("detail", d.detail());
    String raw = d.rawPayload();
    if (raw != null) {
      boolean truncated = raw.length() > MAX_RAW_PAYLOAD_CHARS;
      n.put("rawPayload", truncated ? raw.substring(0, MAX_RAW_PAYLOAD_CHARS) : raw);
      n.put("rawPayloadTruncated", truncated);
    }
    SecurityEvent event = d.event();
    if (event != null) {
      n.set("event", EventJson.toNode(event));
    }
    return n;
  }

  @Override
  public void close() throws IOException {
    lock.lock();
    try {
      writer.close();
    } finally {
      lock.unlock();
    }
  }
}
