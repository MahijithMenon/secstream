package com.mahijith.secstream.processing;

import com.mahijith.secstream.model.EventFormat;
import com.mahijith.secstream.model.InvalidEventException;
import com.mahijith.secstream.model.RawEvent;
import com.mahijith.secstream.model.SecurityEvent;

/**
 * Parses one raw wire format and maps it onto {@link SecurityEvent}. Implementations must be
 * stateless (or otherwise thread-safe): every worker thread shares one instance per format.
 */
public interface EventNormalizer {
  EventFormat format();

  /**
   * @throws InvalidEventException if the payload is malformed or violates a domain constraint; such
   *     events are dead-lettered and never retried
   */
  SecurityEvent normalize(RawEvent raw);
}
