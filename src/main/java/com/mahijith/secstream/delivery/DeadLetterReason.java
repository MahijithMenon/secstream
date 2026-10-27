package com.mahijith.secstream.delivery;

public enum DeadLetterReason {
  /** Payload could not be parsed or violated a domain constraint. Never retried. */
  MALFORMED,
  /** A processing stage threw unexpectedly (a bug); the raw payload is preserved for replay. */
  PROCESSING_ERROR,
  /** A sink classified the failure as permanent (e.g. a constraint violation). */
  DELIVERY_REJECTED,
  /** Every retry attempt for a sink failed with a transient error. */
  RETRIES_EXHAUSTED,
  /** A forced shutdown interrupted the event before it was processed or delivered. */
  SHUTDOWN_ABANDONED
}
