package com.mahijith.secstream.delivery;

/** What a {@link SinkDispatcher} queue holds: a real delivery or the end-of-stream marker. */
sealed interface DispatchItem permits Delivery, DispatchItem.EndOfStream {
  enum EndOfStream implements DispatchItem {
    INSTANCE
  }
}
