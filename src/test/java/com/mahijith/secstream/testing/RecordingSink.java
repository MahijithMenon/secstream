package com.mahijith.secstream.testing;

import com.mahijith.secstream.delivery.EventSink;
import com.mahijith.secstream.delivery.SinkException;
import com.mahijith.secstream.model.EventId;
import com.mahijith.secstream.model.SecurityEvent;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/** Records every successfully written event and counts writes per id (to detect duplicates). */
public class RecordingSink implements EventSink {
  private final String name;
  // Written by the single dispatcher thread, read by the test thread after join; synchronized
  // lists rather than CopyOnWriteArrayList, whose O(n) copy per add is quadratic at 1M events.
  private final List<SecurityEvent> written = Collections.synchronizedList(new ArrayList<>());
  private final ConcurrentMap<EventId, AtomicInteger> writesPerId = new ConcurrentHashMap<>();
  private final List<Integer> batchSizes = Collections.synchronizedList(new ArrayList<>());
  private volatile boolean closed;
  private volatile String writerThread;

  public RecordingSink(String name) {
    this.name = name;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public void write(List<SecurityEvent> batch) throws SinkException {
    String current = Thread.currentThread().getName();
    if (writerThread == null) {
      writerThread = current;
    } else if (!writerThread.equals(current)) {
      throw new AssertionError("sink written from two threads: " + writerThread + ", " + current);
    }
    beforeWrite(batch);
    batchSizes.add(batch.size());
    for (SecurityEvent e : batch) {
      written.add(e);
      writesPerId.computeIfAbsent(e.id(), k -> new AtomicInteger()).incrementAndGet();
    }
  }

  /** Hook for subclasses that inject failures or delays. */
  protected void beforeWrite(List<SecurityEvent> batch) throws SinkException {}

  public List<SecurityEvent> written() {
    synchronized (written) {
      return new ArrayList<>(written);
    }
  }

  public int distinctIds() {
    return writesPerId.size();
  }

  public List<Integer> batchSizes() {
    synchronized (batchSizes) {
      return List.copyOf(batchSizes);
    }
  }

  public boolean closed() {
    return closed;
  }

  @Override
  public void close() {
    closed = true;
  }
}
