package com.mahijith.secstream.source;

import java.io.IOException;
import java.io.Reader;
import org.jspecify.annotations.Nullable;

/**
 * Reads lines without ever buffering more than {@code maxChars + 1} characters.
 *
 * <p>{@link java.io.BufferedReader#readLine()} has no limit: a single 2 GB line without a newline
 * would exhaust the heap. Here an over-long line is returned truncated to {@code maxChars + 1}
 * characters (so the caller, or the pipeline's size check, can recognise and reject it) and the
 * rest of it is skipped.
 */
final class BoundedLineReader {
  private final Reader in;
  private final int maxChars;
  private final char[] buffer = new char[8192];
  private int position;
  private int limit;

  BoundedLineReader(Reader in, int maxChars) {
    this.in = in;
    this.maxChars = maxChars;
  }

  /** Returns the next line without its terminator, or null at end of input. */
  @Nullable String readLine() throws IOException {
    StringBuilder line = new StringBuilder();
    boolean sawAnything = false;
    while (true) {
      if (position == limit) {
        limit = in.read(buffer, 0, buffer.length);
        position = 0;
        if (limit <= 0) {
          limit = 0;
          return sawAnything ? stripCr(line) : null;
        }
      }
      sawAnything = true;
      char c = buffer[position++];
      if (c == '\n') {
        return stripCr(line);
      }
      if (line.length() <= maxChars) {
        line.append(c);
      }
    }
  }

  private static String stripCr(StringBuilder line) {
    int n = line.length();
    return n > 0 && line.charAt(n - 1) == '\r' ? line.substring(0, n - 1) : line.toString();
  }
}
