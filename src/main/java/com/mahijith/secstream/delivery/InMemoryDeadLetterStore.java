package com.mahijith.secstream.delivery;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/** For tests and embedding. Unbounded, so never used by the CLI application. */
public final class InMemoryDeadLetterStore implements DeadLetterStore {
  private final List<DeadLetter> entries = new CopyOnWriteArrayList<>();

  @Override
  public void append(DeadLetter deadLetter) {
    entries.add(deadLetter);
  }

  public List<DeadLetter> entries() {
    return List.copyOf(entries);
  }

  @Override
  public void close() {}
}
