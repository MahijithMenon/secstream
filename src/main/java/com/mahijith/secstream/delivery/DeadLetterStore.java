package com.mahijith.secstream.delivery;

import java.io.IOException;

/** Durable-ish storage for dead letters. Implementations must be thread-safe. */
public interface DeadLetterStore extends AutoCloseable {
  void append(DeadLetter deadLetter) throws IOException;

  @Override
  void close() throws IOException;
}
