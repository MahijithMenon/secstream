package com.mahijith.secstream.pipeline;

import com.mahijith.secstream.model.EventFormat;
import java.time.Duration;

/**
 * How sources hand events to the pipeline. Thread-safe: any number of sources may call
 * concurrently.
 *
 * <p>Once a call returns {@link Admission#ACCEPTED}, the pipeline owns the event: it will be
 * counted in exactly one terminal outcome (see {@code MetricsSnapshot}) unless the process dies.
 */
public interface EventIngress {
  /** Blocks while the ingest queue is full: backpressure for pull-based sources (files). */
  Admission submit(String source, EventFormat format, String payload) throws InterruptedException;

  /** Waits at most {@code timeout} for queue space: for push sources (HTTP) that must answer. */
  Admission trySubmit(String source, EventFormat format, String payload, Duration timeout)
      throws InterruptedException;
}
