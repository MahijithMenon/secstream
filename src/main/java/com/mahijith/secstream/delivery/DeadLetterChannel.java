package com.mahijith.secstream.delivery;

import com.mahijith.secstream.metrics.PipelineMetrics;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The single entry point for dead-lettering. It counts every dead letter by reason and, if the
 * store itself fails, counts and logs that too: a dead letter that cannot be written is the one
 * place this pipeline loses an event while running, so it must at least be visible.
 */
public final class DeadLetterChannel {
  private static final Logger log = LoggerFactory.getLogger(DeadLetterChannel.class);

  private final DeadLetterStore store;
  private final PipelineMetrics metrics;

  public DeadLetterChannel(DeadLetterStore store, PipelineMetrics metrics) {
    this.store = store;
    this.metrics = metrics;
  }

  public void publish(DeadLetter deadLetter) {
    try {
      store.append(deadLetter);
      metrics.deadLetterWritten(deadLetter.reason());
    } catch (IOException | RuntimeException e) {
      metrics.deadLetterWriteFailed();
      log.atError()
          .setMessage("dead letter could not be stored; event lost")
          .addKeyValue("reason", deadLetter.reason())
          .addKeyValue("source", deadLetter.source())
          .addKeyValue("sequence", deadLetter.sequence())
          .addKeyValue("sink", deadLetter.sink())
          .setCause(e)
          .log();
    }
  }

  public void close() throws IOException {
    store.close();
  }
}
