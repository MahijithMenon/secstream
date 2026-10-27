package com.mahijith.secstream.sink;

import com.mahijith.secstream.delivery.EventSink;
import com.mahijith.secstream.model.SecurityEvent;
import java.util.List;

/** Accepts and drops everything; used to benchmark the pipeline without sink I/O. */
public final class DiscardingSink implements EventSink {
  private final String name;

  public DiscardingSink(String name) {
    this.name = name;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public void write(List<SecurityEvent> batch) {}

  @Override
  public void close() {}
}
