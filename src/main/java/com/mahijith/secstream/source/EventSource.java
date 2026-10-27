package com.mahijith.secstream.source;

import com.mahijith.secstream.pipeline.EventIngress;

/**
 * Produces raw events into the pipeline. Each source runs on its own thread, started by the
 * application. Adding a new source type means implementing this interface and registering a config
 * type for it; no pipeline code changes.
 */
public interface EventSource {
  String name();

  /**
   * Runs until the source is exhausted, {@link #stop} is called, or the pipeline stops accepting.
   * Blocks the calling thread.
   */
  void run(EventIngress ingress) throws Exception;

  /** Requests the source to stop; {@link #run} returns soon after. Idempotent, thread-safe. */
  void stop();

  /** False for sources such as HTTP that run until stopped. */
  boolean finite();
}
