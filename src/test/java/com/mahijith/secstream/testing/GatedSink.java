package com.mahijith.secstream.testing;

import com.mahijith.secstream.model.SecurityEvent;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/** Blocks every write until {@link #open()} is called: simulates a stalled destination. */
public final class GatedSink extends RecordingSink {
  private final CountDownLatch gate = new CountDownLatch(1);
  private final CountDownLatch firstWriteStarted = new CountDownLatch(1);

  public GatedSink(String name) {
    super(name);
  }

  @Override
  protected void beforeWrite(List<SecurityEvent> batch) {
    firstWriteStarted.countDown();
    try {
      gate.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("interrupted while gated", e);
    }
  }

  public void release() {
    gate.countDown();
  }

  public void awaitFirstWrite() throws InterruptedException {
    firstWriteStarted.await();
  }
}
