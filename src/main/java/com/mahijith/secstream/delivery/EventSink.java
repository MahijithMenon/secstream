package com.mahijith.secstream.delivery;

import com.mahijith.secstream.model.SecurityEvent;
import java.util.List;

/**
 * A delivery destination.
 *
 * <p><b>Threading contract:</b> each sink is driven by exactly one {@link SinkDispatcher} thread,
 * so implementations need not be thread-safe. {@link #open}, every {@link #write}, and {@link
 * #close} happen on that thread or are ordered before/after it by thread start/join.
 *
 * <p><b>Atomicity:</b> {@link #write} should be all-or-nothing where the destination allows it
 * (PostgreSQL: one transaction). Where it cannot (files), a failed write may have persisted a
 * prefix of the batch, which the retry will write again: delivery is at-least-once.
 */
public interface EventSink extends AutoCloseable {
  String name();

  default void open() throws SinkException {}

  void write(List<SecurityEvent> batch) throws SinkException;

  @Override
  void close();
}
