package com.mahijith.secstream.testing;

import com.mahijith.secstream.delivery.SinkException;
import com.mahijith.secstream.model.SecurityEvent;
import java.io.IOException;
import java.util.List;
import java.util.function.IntPredicate;

/**
 * Fails write attempts according to a deterministic schedule over the attempt number (0-based),
 * e.g. {@code attempt -> attempt % 10 < 3} fails exactly 30% of attempts regardless of thread
 * timing. Correctness assertions never depend on random chance.
 */
public final class FlakySink extends RecordingSink {
  private final IntPredicate failsOnAttempt;
  private final boolean retryable;
  private int attempts; // single dispatcher thread, see EventSink contract

  public FlakySink(String name, IntPredicate failsOnAttempt, boolean retryable) {
    super(name);
    this.failsOnAttempt = failsOnAttempt;
    this.retryable = retryable;
  }

  public static FlakySink failingPercent(String name, int percent) {
    return new FlakySink(name, attempt -> attempt % 100 < percent, true);
  }

  @Override
  protected void beforeWrite(List<SecurityEvent> batch) throws SinkException {
    int attempt = attempts++;
    if (failsOnAttempt.test(attempt)) {
      IOException cause = new IOException("injected failure #" + attempt);
      throw retryable
          ? SinkException.transientFailure("injected", cause)
          : SinkException.permanentFailure("injected", cause);
    }
  }

  public int attempts() {
    return attempts;
  }
}
